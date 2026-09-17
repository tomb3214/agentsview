package service_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/service"
)

func TestLocalCacheHTTPUnavailable(t *testing.T) {
	env := newHTTPBackendEnv(t)
	hash := "source"
	require.NoError(t, env.DB.UpsertSession(db.Session{ID: "old", Machine: "machine", Project: "project", Agent: "codex", FileHash: &hash, MessageCount: 1}))
	require.NoError(t, env.DB.ReplaceSessionMessages("old", []db.Message{{SessionID: "old", Role: "user", Content: "cached"}}))
	ctx := context.Background()
	s, err := env.DB.GetSessionFull(ctx, "old")
	require.NoError(t, err)
	e := db.CacheEviction{SessionID: "old", FileHash: hash, Revision: *s.TranscriptRevision, MessageCount: 1, ContentDigest: strings.Repeat("a", 64), BackupID: "verified"}
	if s.LocalModifiedAt != nil {
		e.Modified = *s.LocalModifiedAt
	}
	require.NoError(t, env.DB.EvictCachedSession(ctx, e))
	backend := env.Backend("", false)
	_, err = backend.Get(ctx, "old")
	require.ErrorIs(t, err, service.ErrLocalContentEvicted)
	_, err = backend.Messages(ctx, "old", service.MessageFilter{})
	require.ErrorIs(t, err, service.ErrLocalContentEvicted)
	_, err = backend.ToolCalls(ctx, "old")
	require.ErrorIs(t, err, service.ErrLocalContentEvicted)
}
