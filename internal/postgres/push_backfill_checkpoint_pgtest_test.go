//go:build pgtest

package postgres

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/db"
)

func TestMetadataBackfillCheckpointRetainsReplacementContract(t *testing.T) {
	pgURL := testPGURL(t)
	const schema = "agentsview_backfill_checkpoint_test"
	cleanNamedPGSchema(t, pgURL, schema)
	t.Cleanup(func() { cleanNamedPGSchema(t, pgURL, schema) })
	ctx := context.Background()
	local, err := db.Open(filepath.Join(t.TempDir(), "local.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	for i := range 51 {
		id := fmt.Sprintf("session-%03d", i)
		require.NoError(t, local.UpsertSession(db.Session{
			ID: id, Machine: "workstation", Project: "project", Agent: "codex",
		}))
		require.NoError(t, local.InsertMessages([]db.Message{{
			SessionID: id, Ordinal: 0, Role: "user", Content: "retained", ContentLength: 8,
		}}))
	}
	push, err := New(pgURL, schema, local, "workstation", true, SyncOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, push.Close()) })
	require.NoError(t, push.EnsureSchema(ctx))
	initial, err := push.Push(ctx, false, nil)
	require.NoError(t, err)
	require.Zero(t, initial.Errors)
	require.Equal(t, 51, initial.MessagesPushed)
	rowIdentity := func() string {
		t.Helper()
		var value string
		require.NoError(t, push.pg.QueryRowContext(ctx,
			"SELECT xmin::text FROM messages WHERE session_id='session-000'").Scan(&value))
		return value
	}
	before := rowIdentity()
	interruptSweep := func() {
		t.Helper()
		require.NoError(t, local.SetSyncState(sessionProvenanceBackfillStateKey, ""))
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		result, err := push.PushWithOptions(runCtx, PushOptions{}, func(progress PushProgress) {
			if progress.Phase == "" && progress.SessionsDone >= 50 {
				cancel()
			}
		})
		require.Error(t, err)
		require.Equal(t, 50, result.SessionsPushed)
		require.Zero(t, result.MessagesPushed)
		raw, err := local.GetSyncState(fullPushProgressStateKey)
		require.NoError(t, err)
		var progress fullPushProgressState
		require.NoError(t, json.Unmarshal([]byte(raw), &progress))
		require.True(t, progress.CompareMessages)
		require.Len(t, progress.Fingerprints, 50)
		assert.Equal(t, before, rowIdentity())
	}
	interruptSweep()
	resumed, err := push.Push(ctx, false, nil)
	require.NoError(t, err)
	require.Zero(t, resumed.Errors)
	assert.Equal(t, 1, resumed.SessionsPushed)
	assert.Zero(t, resumed.MessagesPushed)
	assert.Equal(t, before, rowIdentity())

	interruptSweep()
	forced, err := push.PushWithOptions(ctx, PushOptions{Full: true}, nil)
	require.NoError(t, err)
	require.Zero(t, forced.Errors)
	assert.Equal(t, 51, forced.SessionsPushed,
		"explicit full must not reuse a metadata-only comparison checkpoint")
	assert.Equal(t, 51, forced.MessagesPushed)
	assert.NotEqual(t, before, rowIdentity())
}
