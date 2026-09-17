package sync

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/testjsonl"
)

func TestLocalCacheUnchangedResyncAndResume(t *testing.T) {
	ctx := context.Background()
	database := openTestDB(t)
	root := t.TempDir()
	const uuid = "019eb791-cf7d-75c1-8439-9ed74c1229ed"
	id := "codex:" + uuid
	path := filepath.Join(root, "rollout-2024-01-01T10-00-00-"+uuid+".jsonl")
	content := testjsonl.JoinJSONL(
		testjsonl.CodexSessionMetaJSON(uuid, "/home/user/code/project", "codex_cli_rs", "2024-01-01T10:00:00Z"),
		testjsonl.CodexMsgJSON("user", "original full prefix", "2024-01-01T10:00:01Z"))
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	cfg := EngineConfig{AgentDirs: map[parser.AgentType][]string{parser.AgentCodex: {root}}, Machine: "machine"}
	engine := NewEngine(database, cfg)
	engine.SyncAll(ctx, nil)
	sess, err := database.GetSessionFull(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, sess)
	e := db.CacheEviction{SessionID: id, FileHash: *sess.FileHash, Revision: *sess.TranscriptRevision, MessageCount: sess.MessageCount, ContentDigest: strings.Repeat("a", 64), BackupID: "verified-backup"}
	if sess.LocalModifiedAt != nil {
		e.Modified = *sess.LocalModifiedAt
	}
	require.NoError(t, database.EvictCachedSession(ctx, e))
	// A newly started process must not refill the cache from unchanged files.
	engine = NewEngine(database, cfg)
	engine.SyncAll(ctx, nil)
	msgs, err := database.GetAllMessages(ctx, id)
	require.NoError(t, err)
	assert.Empty(t, msgs)
	stats := engine.ResyncAll(ctx, nil)
	require.False(t, stats.Aborted, "%+v", stats)
	receipt, err := database.CacheEviction(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	msgs, err = database.GetAllMessages(ctx, id)
	require.NoError(t, err)
	assert.Empty(t, msgs)
	// A genuine append must parse the entire source, not its saved suffix.
	content += "\n" + testjsonl.CodexMsgJSON("assistant", "resumed answer", "2024-01-02T10:00:01Z") + "\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	engine.SyncAll(ctx, nil)
	msgs, err = database.GetAllMessages(ctx, id)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	assert.Equal(t, "original full prefix", msgs[0].Content)
	assert.Equal(t, "resumed answer", msgs[1].Content)
	receipt, err = database.CacheEviction(ctx, id)
	require.NoError(t, err)
	assert.Nil(t, receipt)
}
