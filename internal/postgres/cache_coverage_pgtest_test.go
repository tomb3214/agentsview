//go:build pgtest

package postgres

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
)

func TestCacheCoverageProtectsFullAndIncrementalPublication(t *testing.T) {
	ctx := context.Background()
	url := testPGURL(t)
	const schema = "agentsview_cache_test"
	cleanNamedPGSchema(t, url, schema)
	t.Cleanup(func() { cleanNamedPGSchema(t, url, schema) })
	local, err := db.Open(filepath.Join(t.TempDir(), "local.db"))
	require.NoError(t, err)
	defer local.Close()
	hash := "source-v1"
	require.NoError(t, local.UpsertSession(db.Session{ID: "old", Machine: "machine", Project: "project", Agent: "codex", FileHash: &hash, MessageCount: 1}))
	payload := strings.Repeat("real tool result ", 4000)
	msgs := []db.Message{{SessionID: "old", Ordinal: 0, Role: "assistant", Content: "inspect", ContentLength: 7, ToolCalls: []db.ToolCall{{ToolName: "Read", Category: "file", InputJSON: `{"path":"fixture"}`, ResultContent: payload, ResultContentLength: len(payload)}}}}
	require.NoError(t, local.ReplaceSessionMessages("old", msgs))
	push, err := New(url, schema, local, "machine", true, SyncOptions{})
	require.NoError(t, err)
	defer push.Close()
	require.NoError(t, push.EnsureSchema(ctx))
	result, err := push.Push(ctx, false, nil)
	require.NoError(t, err)
	require.Zero(t, result.Errors)
	// A backup snapshot excludes a later concurrent commit.
	holder, err := push.pg.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer holder.Rollback()
	var snapshot string
	require.NoError(t, holder.QueryRowContext(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot))
	backup, err := ReadCacheCoverage(ctx, push.pg, snapshot, nil)
	require.NoError(t, err)
	require.Len(t, backup.Sessions, 1)
	current, err := ReadCacheCoverage(ctx, push.pg, "", []string{"old"})
	require.NoError(t, err)
	sess, err := local.GetSessionFull(ctx, "old")
	require.NoError(t, err)
	eviction, err := VerifyCachedSession(ctx, local, *sess, current.Sessions["old"], backup.Sessions["old"])
	require.NoError(t, err)
	// Equal-length tool-output drift must fail, despite equal row counts.
	_, err = push.pg.ExecContext(ctx, "UPDATE tool_calls SET result_content=replace(result_content,'real','fake') WHERE session_id='old'")
	require.NoError(t, err)
	changed, err := ReadCacheCoverage(ctx, push.pg, "", []string{"old"})
	require.NoError(t, err)
	_, err = VerifyCachedSession(ctx, local, *sess, changed.Sessions["old"], backup.Sessions["old"])
	require.Error(t, err)
	snapAgain, err := ReadCacheCoverage(ctx, push.pg, snapshot, nil)
	require.NoError(t, err)
	assert.Equal(t, backup.Sessions, snapAgain.Sessions)
	_, err = push.pg.ExecContext(ctx, "UPDATE tool_calls SET result_content=replace(result_content,'fake','real') WHERE session_id='old'")
	require.NoError(t, err)
	eviction.BackupID = "verified-backup"
	require.NoError(t, local.EvictCachedSession(ctx, *eviction))
	for _, full := range []bool{false, true} {
		result, err = push.Push(ctx, full, nil)
		require.NoError(t, err)
		require.Zero(t, result.Errors)
		after, err := ReadCacheCoverage(ctx, push.pg, "", nil)
		require.NoError(t, err)
		assert.Equal(t, backup.Sessions, after.Sessions)
	}
}

func TestCacheEvictionPreservesCentralVectors(t *testing.T) {
	ctx := context.Background()
	push, local, pg := newVectorPushTestSync(t, testPGURL(t), "agentsview_cache_vector_test")
	seedVectorSession(t, local, "old")
	sess, err := local.GetSessionFull(ctx, "old")
	require.NoError(t, err)
	hash := "source"
	sess.FileHash = &hash
	require.NoError(t, local.UpsertSession(*sess))
	source := &fakeVectorSource{gen: VectorGenerationInfo{Fingerprint: "cache-gen", Model: "test", Dimension: 4}, hasGen: true,
		hashes: map[string]string{"old": "hash"}, docs: map[string][]VectorPushDoc{"old": {vdoc("old", "old#0", 0, "old", "hash", []float32{1, 0, 0, 0})}}}
	push.vectorSource = source
	result, err := push.Push(ctx, false, nil)
	require.NoError(t, err)
	require.Equal(t, 1, result.Vectors.SessionsPushed)
	coverage, err := ReadCacheCoverage(ctx, pg, "", nil)
	require.NoError(t, err)
	sess, err = local.GetSessionFull(ctx, "old")
	require.NoError(t, err)
	eviction, err := VerifyCachedSession(ctx, local, *sess, coverage.Sessions["old"], coverage.Sessions["old"])
	require.NoError(t, err)
	eviction.BackupID = "verified"
	require.NoError(t, local.EvictCachedSession(ctx, *eviction))
	delete(source.hashes, "old")
	delete(source.docs, "old")
	for _, full := range []bool{false, true} {
		result, err = push.Push(ctx, full, nil)
		require.NoError(t, err)
		require.Zero(t, result.Vectors.SessionsEvicted)
		assert.Equal(t, 1, countRows(t, pg, "SELECT count(*) FROM vector_documents WHERE session_id='old'"))
		assert.Equal(t, 1, countRows(t, pg, "SELECT count(*) FROM vector_push_state WHERE session_id='old'"))
		assert.Equal(t, 1, countRows(t, pg, "SELECT count(*) FROM messages WHERE session_id='old'"))
	}
}
