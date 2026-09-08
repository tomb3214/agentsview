package vector

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/parser"
	agentsync "go.kenn.io/agentsview/internal/sync"
	"go.kenn.io/agentsview/internal/testjsonl"
	kitvec "go.kenn.io/kit/vector"
)

// A detached encoder must not own native sync or its publication callback.
// Restarting the vector owner retains committed documents and retries only
// unfinished work, including a realistically sized multi-chunk document.
func TestBackgroundBuildAllowsNativePublicationAndResumesCommittedDocuments(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	archive := dbtest.OpenTestDBAt(t, filepath.Join(dir, "sessions.db"))
	dbtest.SeedSessionWithMessages(t, archive, "retained", "project", []db.Message{
		dbtest.UserMsg("retained", 0, "already durable"),
	})
	vectorPath := filepath.Join(dir, "vectors.db")
	ix, err := Open(ctx, vectorPath, false, 8192)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ix.Close()) })
	gen := fakeGeneration("fake-model")
	_, err = ix.Build(ctx, archive, fakeBuildEncoder(), gen, BuildOptions{})
	require.NoError(t, err)
	key := DocKey("user", "retained", "", 0, 1)
	var beforeID int64
	var beforeStamp string
	require.NoError(t, ix.db.QueryRow(`SELECT c.vec_rowid,st.revision FROM message_vectors_chunks c JOIN message_vectors_stamps st ON st.doc_key=c.doc_key AND st.ordinal=c.ordinal WHERE c.doc_key=?`, key).Scan(&beforeID, &beforeStamp))
	// Calculate the exact observed 63-chunk shape from the existing splitter.
	content := strings.Repeat("x", 8192+62*(8192-ix.split.Overlap))
	require.Len(t, kitvec.Split(content, ix.split), 63)
	dbtest.SeedSessionWithMessages(t, archive, "unfinished", "project", []db.Message{
		dbtest.UserMsg("unfinished", 0, content),
	})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	enc := func(_ context.Context, texts []string) ([][]float32, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil, errors.New("isolated encoder interrupted")
	}
	m := NewManager(ix, archive, EncoderSet{Default: "test", ByName: map[string]ManagedEncoder{
		"test": {Encode: enc, Settings: EncodeSettings{BatchSize: 4, Concurrency: 1}},
	}}, gen)
	require.NoError(t, m.StartBuild(BuildRequest{Backstop: true}))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("encoder did not start")
	}
	require.True(t, m.Status().Running)
	assert.ErrorIs(t, m.StartBuild(BuildRequest{}), ErrBuildRunning)
	assert.Nil(t, m.Status().LastSuccessfulBackstop)

	root := filepath.Join(dir, "claude", "project")
	require.NoError(t, os.MkdirAll(root, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "new-session.jsonl"), []byte(testjsonl.NewSessionBuilder().
		AddClaudeUserWithSessionID("2026-07-12T00:00:00Z", "arrived during encoding", "new-session").String()), 0600))
	engine := agentsync.NewEngine(archive, agentsync.EngineConfig{AgentDirs: map[parser.AgentType][]string{
		parser.AgentClaude: {filepath.Dir(root)},
	}, Machine: "fixture-host"})
	defer engine.Close()
	publication := make(chan error, 1)
	go func() {
		_, e := engine.SyncThenRun(ctx, false, nil, func(bool) error {
			// Same callback boundary used by PostgreSQL publication; observe native
			// results while the model request is still deliberately held.
			session, e := archive.GetSession(ctx, "new-session")
			if e != nil {
				return e
			}
			if session == nil {
				return errors.New("new native session absent at publication")
			}
			return nil
		})
		publication <- e
	}()
	select {
	case e := <-publication:
		require.NoError(t, e)
	case <-time.After(5 * time.Second):
		t.Fatal("native publication waited for encoding")
	}
	assert.True(t, m.Status().Running)
	once.Do(func() { close(release) })
	m.Wait()
	assert.Contains(t, m.Status().LastError, "isolated encoder interrupted")
	assert.Nil(t, m.Status().LastSuccessfulBackstop)
	require.NoError(t, ix.Close())
	ix, err = Open(ctx, vectorPath, false, 8192)
	require.NoError(t, err)
	encodedDurable := false
	resumed := func(ctx context.Context, texts []string) ([][]float32, error) {
		for _, text := range texts {
			if text == "already durable" {
				encodedDurable = true
			}
		}
		return fakeBuildEncoder()(ctx, texts)
	}
	next := NewManager(ix, archive, EncoderSet{Default: "test", ByName: map[string]ManagedEncoder{
		"test": {Encode: resumed, Settings: EncodeSettings{BatchSize: 4, Concurrency: 1}},
	}}, gen)
	started, err := next.TryBuild(ctx, BuildRequest{Backstop: true})
	require.NoError(t, err)
	require.True(t, started)
	require.NotNil(t, next.Status().LastSuccessfulBackstop)
	assert.False(t, encodedDurable)
	var afterID int64
	var afterStamp string
	require.NoError(t, ix.db.QueryRow(`SELECT c.vec_rowid,st.revision FROM message_vectors_chunks c JOIN message_vectors_stamps st ON st.doc_key=c.doc_key AND st.ordinal=c.ordinal WHERE c.doc_key=?`, key).Scan(&afterID, &afterStamp))
	assert.Equal(t, beforeID, afterID)
	assert.Equal(t, beforeStamp, afterStamp)
	var count int
	require.NoError(t, ix.db.QueryRow(`SELECT count(*) FROM message_vectors_chunks WHERE doc_key=?`, DocKey("user", "unfinished", "", 0, 1)).Scan(&count))
	assert.Equal(t, 63, count)
	exp, ok, err := ix.BeginExport(ctx, nil)
	require.NoError(t, err)
	require.True(t, ok)
	defer exp.Close()
	docs, _, err := exp.SessionDocs(ctx, "unfinished")
	require.NoError(t, err)
	require.Len(t, docs, 1)
	assert.Len(t, docs[0].Chunks, 63)
}
