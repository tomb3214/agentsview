package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/postgres"
)

func TestCacheRecencyAndFreshBackup(t *testing.T) {
	older, newer := "2026-09-17T08:00:00+10:00", "2026-09-16T23:00:00Z"
	candidates := []db.Session{{ID: "new", EndedAt: &newer}, {ID: "unknown"}, {ID: "old", EndedAt: &older}}
	slices.SortFunc(candidates, compareCacheActivity)
	assert.Equal(t, "old", candidates[0].ID)
	assert.Equal(t, "new", candidates[1].ID)
	assert.Equal(t, "unknown", candidates[2].ID)
	now := time.Now().UTC()
	proof := postgres.CacheCoverage{Format: postgres.CacheCoverageFormat, BackupID: "verified", Sessions: map[string]postgres.CacheCopy{"old": {}}}
	for _, age := range []time.Duration{time.Hour, 73 * time.Hour, -time.Hour} {
		proof.CapturedAt = now.Add(-age).Format(time.RFC3339)
		err := validateCacheProof(proof, now)
		if age == time.Hour {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
	// Already-under-budget runs do no database/network work, even offline.
	cfg := config.Config{DataDir: t.TempDir()}
	result, err := trimLocalCache(context.Background(), cfg, 5_000_000_000, postgres.CacheCoverage{})
	require.NoError(t, err)
	assert.True(t, result.WithinBudget)
	files, err := os.ReadDir(cfg.DataDir)
	require.NoError(t, err)
	assert.Empty(t, files)
}

func TestCacheAuxiliaryOverflowRetainsTranscripts(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{DataDir: t.TempDir()}
	cfg.DBPath = filepath.Join(cfg.DataDir, "sessions.db")
	local, err := db.Open(cfg.DBPath)
	require.NoError(t, err)
	hash := "source"
	require.NoError(t, local.UpsertSession(db.Session{ID: "old", Machine: "machine", Agent: "codex", Project: "project", FileHash: &hash, MessageCount: 1}))
	require.NoError(t, local.ReplaceSessionMessages("old", []db.Message{{SessionID: "old", Role: "user", Content: "retained"}}))
	require.NoError(t, local.Close())
	require.NoError(t, os.WriteFile(filepath.Join(cfg.DataDir, "auxiliary.bin"), make([]byte, 1024*1024), 0600))
	proof := postgres.CacheCoverage{Format: postgres.CacheCoverageFormat, BackupID: "verified", CapturedAt: time.Now().UTC().Format(time.RFC3339), Sessions: map[string]postgres.CacheCopy{"old": {}}}
	result, err := trimLocalCache(ctx, cfg, 1024*1024, proof)
	require.ErrorContains(t, err, "auxiliary files")
	assert.Zero(t, result.Evicted)
	local, err = db.Open(cfg.DBPath)
	require.NoError(t, err)
	defer local.Close()
	msgs, err := local.GetAllMessages(ctx, "old")
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "retained", msgs[0].Content)
}
