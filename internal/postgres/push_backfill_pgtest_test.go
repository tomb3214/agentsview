//go:build pgtest

package postgres

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/db"
)

func TestRebuiltArchiveBackfillPreservesMatchingTranscripts(t *testing.T) {
	pgURL := testPGURL(t)
	const schema = "agentsview_rebuilt_backfill_test"
	cleanNamedPGSchema(t, pgURL, schema)
	t.Cleanup(func() { cleanNamedPGSchema(t, pgURL, schema) })
	ctx := context.Background()
	originalPath := filepath.Join(t.TempDir(), "original.db")
	original, err := db.Open(originalPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, original.Close()) })
	seed := func(local *db.DB) {
		t.Helper()
		for _, id := range []string{"unchanged", "changed"} {
			require.NoError(t, local.UpsertSession(db.Session{
				ID: id, Machine: "workstation", Project: "project", Agent: "codex",
				SourceSessionID: "native-" + id,
			}))
			var msgs []db.Message
			for ordinal := range 6 {
				msg := db.Message{SessionID: id, Ordinal: ordinal, Role: "assistant",
					Content: "retained message", ContentLength: 16,
					Timestamp: "2026-05-02T02:06:39.123Z"}
				if ordinal >= 2 && ordinal <= 4 {
					msg.HasToolUse = true
					msg.ToolCalls = []db.ToolCall{{ToolName: "Read", Category: "file",
						ResultEvents: []db.ToolResultEvent{{Source: "cli", Status: "ok",
							Content: strings.Repeat("native result ", 64), ContentLength: 14 * 64,
							Timestamp: "2026-05-02T02:06:39.23Z"}}}}
				}
				msgs = append(msgs, msg)
			}
			require.NoError(t, local.InsertMessages(msgs))
		}
	}
	seed(original)
	first, err := New(pgURL, schema, original, "workstation", true, SyncOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	require.NoError(t, first.EnsureSchema(ctx))
	result, err := first.Push(ctx, false, nil)
	require.NoError(t, err)
	require.Zero(t, result.Errors)
	markers := []string{sessionAliasBackfillStateKey, sessionProvenanceBackfillStateKey,
		transcriptRevisionBackfillStateKey}
	for _, key := range markers {
		value, err := original.GetSyncState(key)
		require.NoError(t, err)
		require.Equal(t, "1", value, "normal initial push must complete %s", key)
	}

	// Follow the normal rebuild identity, parse and durable-state copy paths.
	// Completed metadata backfills are deliberately absent in the fresh DB.
	rebuilt, err := db.Open(filepath.Join(t.TempDir(), "rebuilt.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rebuilt.Close()) })
	require.NoError(t, rebuilt.CopyArchiveIdentityFrom(originalPath))
	seed(rebuilt)
	require.NoError(t, rebuilt.CopySyncStateFrom(originalPath))
	_, err = rebuilt.CopyOrphanedDataFromExcluding(originalPath, nil)
	require.NoError(t, err)
	require.NoError(t, rebuilt.CopySessionMetadataFrom(originalPath))
	for _, key := range markers {
		value, err := rebuilt.GetSyncState(key)
		require.NoError(t, err)
		require.Empty(t, value, "fixture must reproduce the missing %s marker", key)
	}
	changed, err := rebuilt.GetMessages(ctx, "changed", 0, 100, true)
	require.NoError(t, err)
	require.Len(t, changed, 6)
	changed[0].Content = "changed message"
	changed[0].ContentLength = len(changed[0].Content)
	changed[2].ToolCalls[0].ResultEvents = nil
	require.NoError(t, rebuilt.ReplaceSessionMessages("changed", changed))
	_, err = first.pg.ExecContext(ctx, `
		UPDATE sessions SET source_archive_id='', source_session_id='stale',
			transcript_revision='stale' WHERE id='unchanged';
		INSERT INTO session_aliases (session_id, alias_id) VALUES ('unchanged', 'stale-alias')`)
	require.NoError(t, err)
	identity := func() [3]string {
		t.Helper()
		var got [3]string
		for i, query := range []string{
			`SELECT string_agg(ordinal::text || ':' || xmin::text, ',' ORDER BY ordinal)
			 FROM messages WHERE session_id='unchanged'`,
			`SELECT string_agg(id::text, ',' ORDER BY message_ordinal, call_index)
			 FROM tool_calls WHERE session_id='unchanged'`,
			`SELECT string_agg(id::text, ',' ORDER BY tool_call_message_ordinal, call_index, event_index)
			 FROM tool_result_events WHERE session_id='unchanged'`,
		} {
			require.NoError(t, first.pg.QueryRowContext(ctx, query).Scan(&got[i]))
		}
		return got
	}
	before := identity()
	push, err := New(pgURL, schema, rebuilt, "workstation", true, SyncOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, push.Close()) })
	require.NoError(t, push.EnsureSchema(ctx))
	result, err = push.PushWithOptions(ctx, PushOptions{}, nil)
	require.NoError(t, err)
	require.Zero(t, result.Errors)
	assert.Equal(t, 2, result.SessionsPushed, "metadata sweep must visit both sessions")
	assert.Equal(t, 6, result.MessagesPushed, "only the changed transcript needs replacement")
	assert.Equal(t, before, identity(), "matching messages, calls and events must retain row identities")
	var archiveID, sourceID, revision string
	require.NoError(t, first.pg.QueryRowContext(ctx, `
		SELECT source_archive_id, source_session_id, transcript_revision
		FROM sessions WHERE id='unchanged'`).Scan(&archiveID, &sourceID, &revision))
	wantArchiveID, err := rebuilt.GetArchiveID(ctx)
	require.NoError(t, err)
	assert.Equal(t, wantArchiveID, archiveID)
	assert.Equal(t, "native-unchanged", sourceID)
	assert.NotEqual(t, "stale", revision)
	var aliases int
	require.NoError(t, first.pg.QueryRowContext(ctx,
		`SELECT count(*) FROM session_aliases WHERE session_id='unchanged'`).Scan(&aliases))
	assert.Zero(t, aliases, "stale aliases must be reconciled for the current provider")
	for _, key := range markers {
		value, err := rebuilt.GetSyncState(key)
		require.NoError(t, err)
		assert.Equal(t, "1", value, "successful sweep must complete %s", key)
	}
	store, err := NewStore(pgURL, schema, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	read, err := store.GetMessages(ctx, "changed", 0, 100, true)
	require.NoError(t, err)
	require.Len(t, read, 6)
	assert.Equal(t, changed[0].Content, read[0].Content)
	require.Len(t, read[2].ToolCalls, 1)
	assert.Empty(t, read[2].ToolCalls[0].ResultEvents, "removed events must not survive the sweep")
	assert.Equal(t, changed[3].ToolCalls[0].ResultEvents, read[3].ToolCalls[0].ResultEvents)

	result, err = push.PushWithOptions(ctx, PushOptions{Full: true}, nil)
	require.NoError(t, err)
	require.Zero(t, result.Errors)
	assert.Equal(t, 12, result.MessagesPushed, "explicit full must still replace matching transcripts")
	afterFull := identity()
	for i := range before {
		assert.NotEqual(t, before[i], afterFull[i])
	}
	_, err = first.pg.ExecContext(ctx, `DELETE FROM sync_metadata WHERE key LIKE 'push_marker:%'`)
	require.NoError(t, err)
	result, err = push.PushWithOptions(ctx, PushOptions{}, nil)
	require.NoError(t, err)
	require.Zero(t, result.Errors)
	assert.Equal(t, 12, result.MessagesPushed, "PG reset recovery must retain forced replacement")
	afterReset := identity()
	for i := range afterFull {
		assert.NotEqual(t, afterFull[i], afterReset[i])
	}
}
