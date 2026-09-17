package db

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalCacheEvictionPreservesIdentityAndResync(t *testing.T) {
	ctx := context.Background()
	d := testDB(t)
	hash, path := "source-content-hash", "/fixture/session.jsonl"
	require.NoError(t, d.UpsertSession(Session{ID: "old", Project: "project", Machine: "machine", Agent: "codex", FileHash: &hash, FilePath: &path, MessageCount: 1}))
	payload := strings.Repeat("cached payload ", 40000)
	require.NoError(t, d.ReplaceSessionMessages("old", []Message{{SessionID: "old", Ordinal: 0, Role: "user", Content: payload, ContentLength: len(payload)}}))
	s, err := d.GetSessionFull(ctx, "old")
	require.NoError(t, err)
	e := CacheEviction{SessionID: "old", FileHash: hash, Revision: *s.TranscriptRevision, MessageCount: 1, ContentDigest: strings.Repeat("a", 64), BackupID: "verified-backup"}
	if s.LocalModifiedAt != nil {
		e.Modified = *s.LocalModifiedAt
	}
	before, err := d.CacheUsedBytes(ctx)
	require.NoError(t, err)
	changed := e
	changed.Revision = "wrong"
	require.Error(t, d.EvictCachedSession(ctx, changed))
	require.NoError(t, d.EvictCachedSession(ctx, e))
	afterSession, err := d.GetSessionFull(ctx, "old")
	require.NoError(t, err)
	assert.Equal(t, s.TranscriptRevision, afterSession.TranscriptRevision)
	assert.Equal(t, s.MessageCount, afterSession.MessageCount)
	assert.Equal(t, s.LocalModifiedAt, afterSession.LocalModifiedAt)
	messages, err := d.GetAllMessages(ctx, "old")
	require.NoError(t, err)
	assert.Empty(t, messages)
	_, ok := d.GetSessionForIncremental(path, "codex")
	assert.False(t, ok)
	same, err := d.CacheSourceUnchanged(ctx, "codex", path, hash)
	require.NoError(t, err)
	assert.True(t, same)
	same, err = d.CacheSourceUnchanged(ctx, "codex", path, "changed")
	require.NoError(t, err)
	assert.False(t, same)
	candidates, err := d.ListSessionsForMirrorWindow(ctx, "", nil, nil)
	require.NoError(t, err)
	assert.Empty(t, candidates)
	require.NoError(t, d.CompleteCacheRestore(ctx, "old"))
	receipt, err := d.CacheEviction(ctx, "old")
	require.NoError(t, err)
	require.NotNil(t, receipt)
	require.NoError(t, d.CompactCache(ctx))
	after, err := d.CacheUsedBytes(ctx)
	require.NoError(t, err)
	assert.Less(t, after, before)
	var deletions int
	require.NoError(t, d.Reader().QueryRow("SELECT count(*) FROM session_deletion_changes").Scan(&deletions))
	assert.Zero(t, deletions)
	rebuilt := testDB(t)
	require.NoError(t, rebuilt.CopyCacheEvictionsFrom(d.path))
	copied, err := rebuilt.CacheEviction(ctx, "old")
	require.NoError(t, err)
	assert.Equal(t, receipt, copied)
	row, err := rebuilt.GetSessionFull(ctx, "old")
	require.NoError(t, err)
	require.NotNil(t, row)
	require.NoError(t, d.ReplaceSessionMessages("old", []Message{{SessionID: "old", Ordinal: 0, Role: "user", Content: "original"}, {SessionID: "old", Ordinal: 1, Role: "assistant", Content: "resumed"}}))
	require.NoError(t, d.CompleteCacheRestore(ctx, "old"))
	receipt, err = d.CacheEviction(ctx, "old")
	require.NoError(t, err)
	assert.Nil(t, receipt)
}
