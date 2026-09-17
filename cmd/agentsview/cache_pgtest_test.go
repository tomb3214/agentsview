//go:build pgtest

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/postgres"
)

func TestCacheTrimCompleteBudgetLoop(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprint("external-", external), func(t *testing.T) { testCacheTrimCompleteBudgetLoop(t, external) })
	}
}

func testCacheTrimCompleteBudgetLoop(t *testing.T, external bool) {
	url := os.Getenv("TEST_PG_URL")
	if url == "" {
		t.Skip("requires dedicated TEST_PG_URL")
	}
	ctx := context.Background()
	const schema = "agentsview_cache_command_test"
	pg, err := postgres.Open(url, schema, true)
	require.NoError(t, err)
	defer pg.Close()
	_, err = pg.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	require.NoError(t, err)
	defer pg.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	cfg := config.Config{DataDir: t.TempDir(), PG: config.PGConfig{Schema: schema, AllowInsecure: true}}
	cfg.DBPath = filepath.Join(cfg.DataDir, "sessions.db")
	backupDir := filepath.Join(cfg.DataDir, "backups")
	require.NoError(t, os.Mkdir(backupDir, 0700))
	backupFile := filepath.Join(backupDir, "recovery.dump")
	require.NoError(t, os.WriteFile(backupFile, []byte("recovery copy outside the cache budget"), 0600))
	if external {
		require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "sessions.db"), cfg.DBPath))
	}
	t.Setenv("AGENTSVIEW_CACHE_PG_URL", url)
	local, err := db.Open(cfg.DBPath)
	require.NoError(t, err)
	// Oldest protected backlog stays; timezone-aware recency keeps the newer
	// eligible session even though the lexical timestamp order says otherwise.
	for _, seed := range []struct{ id, ended string }{{"protected", "2026-01-01T00:00:00Z"}, {"old", "2026-09-17T08:00:00+10:00"}, {"new", "2026-09-16T23:00:00Z"}} {
		hash := seed.id + "-source"
		require.NoError(t, local.UpsertSession(db.Session{ID: seed.id, Machine: "machine", Project: "project", Agent: "codex", FileHash: &hash, MessageCount: 1, EndedAt: &seed.ended}))
		payload := strings.Repeat(seed.id+" tool payload ", 50000)
		require.NoError(t, local.ReplaceSessionMessages(seed.id, []db.Message{{SessionID: seed.id, Role: "assistant", Content: "inspect", ContentLength: 7, ToolCalls: []db.ToolCall{{ToolName: "Read", ResultContent: payload, ResultContentLength: len(payload)}}}}))
	}
	push, err := postgres.New(url, schema, local, "machine", true, postgres.SyncOptions{})
	require.NoError(t, err)
	require.NoError(t, push.EnsureSchema(ctx))
	pushed, err := push.Push(ctx, false, nil)
	require.NoError(t, err)
	require.Zero(t, pushed.Errors)
	_, err = postgres.RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	proof, err := postgres.ReadCacheCoverage(ctx, pg, "", nil)
	require.NoError(t, err)
	centralBefore := *proof
	proof.BackupID = "verified-test-backup"
	// Copy the map before removing unavailable backup coverage.
	proof.Sessions = map[string]postgres.CacheCopy{"old": centralBefore.Sessions["old"], "new": centralBefore.Sessions["new"]}
	used, err := local.CacheUsedBytes(ctx)
	require.NoError(t, err)
	require.NoError(t, push.Close())
	require.NoError(t, local.Close())
	result, err := trimLocalCache(ctx, cfg, used-400_000, *proof)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Evicted)
	assert.Equal(t, 1, result.Protected)
	assert.True(t, result.WithinBudget)
	local, err = db.Open(cfg.DBPath)
	require.NoError(t, err)
	for _, id := range []string{"old", "new", "protected"} {
		receipt, err := local.CacheEviction(ctx, id)
		require.NoError(t, err)
		if id == "old" {
			require.NotNil(t, receipt)
		} else {
			require.Nil(t, receipt)
		}
	}
	require.NoError(t, local.Close())
	// A recurrence does no work and does not require available backup/server.
	t.Setenv("AGENTSVIEW_CACHE_PG_URL", "invalid")
	again, err := trimLocalCache(ctx, cfg, result.BudgetBytes, postgres.CacheCoverage{})
	require.NoError(t, err)
	assert.Zero(t, again.Evicted)
	assert.True(t, again.WithinBudget)
	recovery, err := os.ReadFile(backupFile)
	require.NoError(t, err)
	assert.Equal(t, "recovery copy outside the cache budget", string(recovery))
	after, err := postgres.ReadCacheCoverage(ctx, pg, "", nil)
	require.NoError(t, err)
	assert.Equal(t, centralBefore.Sessions, after.Sessions)
}
