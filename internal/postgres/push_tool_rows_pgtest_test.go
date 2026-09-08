//go:build pgtest

package postgres

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
)

// Growing transcripts often append a few calls to hundreds of unchanged,
// indexed result payloads. Only changed keys should produce new row versions.
func TestPushReconcilesToolRowsByKey(t *testing.T) {
	pgURL := testPGURL(t)
	const schema = "agentsview_tool_rows_test"
	const id = "growing-transcript"
	cleanNamedPGSchema(t, pgURL, schema)
	t.Cleanup(func() { cleanNamedPGSchema(t, pgURL, schema) })
	ctx := context.Background()
	local, err := db.Open(filepath.Join(t.TempDir(), "local.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	syncer, err := New(pgURL, schema, local, "machine", true, SyncOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, syncer.Close()) })
	require.NoError(t, syncer.EnsureSchema(ctx))
	store, err := NewStore(pgURL, schema, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, local.UpsertSession(db.Session{ID: id, Machine: "machine", Project: "project", Agent: "codex"}))
	message := func(n int, result bool) db.Message {
		payload := strings.Repeat(fmt.Sprintf("result %03d ", n), 1024)
		tc := db.ToolCall{ToolName: "Read", Category: "file", ToolUseID: fmt.Sprintf("call-%d", n), InputJSON: `{"path":"file.txt"}`, FilePath: "file.txt", SkillName: "inspect", ResultContent: payload, ResultContentLength: len(payload), SubagentSessionID: "child"}
		if result {
			tc.ResultEvents = []db.ToolResultEvent{{ToolUseID: tc.ToolUseID, AgentID: "worker", SubagentSessionID: "child", Source: "cli", Status: "ok", Content: payload, ContentLength: len(payload), Timestamp: "2026-07-01T10:00:01.123456Z"}}
		}
		return db.Message{SessionID: id, Ordinal: n, Role: "assistant", Content: "inspect", ContentLength: 7, HasToolUse: true, ToolCalls: []db.ToolCall{tc}}
	}
	var msgs []db.Message
	for n := 0; n < 231; n++ {
		msgs = append(msgs, message(n, n < 159))
	}
	msgs[0].ToolCalls[0].ResultEvents = append(msgs[0].ToolCalls[0].ResultEvents, db.ToolResultEvent{Source: "cli", Status: "ok", Content: "second result", ContentLength: 13})
	push := func(full bool) {
		t.Helper()
		require.NoError(t, local.ReplaceSessionMessages(id, msgs))
		require.NoError(t, local.SetSyncState("last_push_at", ""))
		require.NoError(t, local.SetSyncState(lastPushBoundaryStateKey, ""))
		result, err := syncer.Push(ctx, full, nil)
		require.NoError(t, err)
		require.Zero(t, result.Errors)
	}
	type rowKey struct{ ordinal, callIndex, eventIndex int }
	type rowIdentity struct {
		ID   int64
		Xmin string
	}
	identities := func(events bool) map[rowKey]rowIdentity {
		t.Helper()
		query := `SELECT message_ordinal,call_index,-1,id,xmin::text FROM tool_calls WHERE session_id=$1`
		if events {
			query = `SELECT tool_call_message_ordinal,call_index,event_index,id,xmin::text FROM tool_result_events WHERE session_id=$1`
		}
		rows, err := syncer.pg.QueryContext(ctx, query, id)
		require.NoError(t, err)
		defer rows.Close()
		got := map[rowKey]rowIdentity{}
		for rows.Next() {
			var key rowKey
			var row rowIdentity
			require.NoError(t, rows.Scan(&key.ordinal, &key.callIndex, &key.eventIndex, &row.ID, &row.Xmin))
			got[key] = row
		}
		require.NoError(t, rows.Err())
		return got
	}
	parity := func() {
		t.Helper()
		want, err := local.GetMessages(ctx, id, 0, 1000, true)
		require.NoError(t, err)
		got, err := store.GetMessages(ctx, id, 0, 1000, true)
		require.NoError(t, err)
		require.Len(t, got, len(want))
		for i := range want {
			assert.Equal(t, want[i].Ordinal, got[i].Ordinal)
			assert.Equal(t, want[i].Content, got[i].Content)
			for j := range want[i].ToolCalls {
				want[i].ToolCalls[j].MessageID = 0
			}
			assert.Equal(t, want[i].ToolCalls, got[i].ToolCalls, "attached calls/events at ordinal %d", want[i].Ordinal)
		}
		wantFP, err := localToolResultEventPGFingerprint(local, id)
		require.NoError(t, err)
		tx, err := syncer.pg.BeginTx(ctx, nil)
		require.NoError(t, err)
		gotFP, err := pgToolResultEventFingerprint(ctx, tx, id)
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		assert.Equal(t, wantFP, gotFP)
	}
	push(false)
	beforeCalls, beforeEvents := identities(false), identities(true)
	require.Len(t, beforeCalls, 231)
	require.Len(t, beforeEvents, 160)
	for n := 231; n < 234; n++ {
		msgs = append(msgs, message(n, true))
	}
	push(false)
	afterCalls, afterEvents := identities(false), identities(true)
	require.Len(t, afterCalls, 234)
	require.Len(t, afterEvents, 163)
	for key, row := range beforeCalls {
		require.Equal(t, row, afterCalls[key], "unchanged call key %+v", key)
	}
	for key, row := range beforeEvents {
		require.Equal(t, row, afterEvents[key], "unchanged event key %+v", key)
	}
	parity()

	// Change payload and context at one stable key; remove another whole message
	// and one call, and move an event to another call index in the same message.
	tc := &msgs[0].ToolCalls[0]
	tc.ToolName = "Execute"
	tc.Category = "shell"
	tc.ToolUseID = "edited-call"
	tc.InputJSON = ""
	tc.SkillName = ""
	tc.ResultContent = "edited"
	tc.ResultContentLength = 6
	tc.SubagentSessionID = "other-child"
	tc.FilePath = ""
	tc.ResultEvents = []db.ToolResultEvent{{ToolUseID: "edited-call", AgentID: "other-worker", SubagentSessionID: "other-child", Source: "api", Status: "error", Content: "edited", ContentLength: 6, Timestamp: "2026-07-02T11:00:02Z"}}
	moved := msgs[1].ToolCalls[0]
	msgs[1].ToolCalls = []db.ToolCall{{ToolName: "Read", Category: "file", ToolUseID: "new-first-call"}, moved}
	msgs[2].ToolCalls = nil
	msgs = append(msgs[:3], msgs[4:]...)
	push(false)
	changedCalls, changedEvents := identities(false), identities(true)
	for _, events := range []bool{false, true} {
		old, now := afterCalls, changedCalls
		eventIndex := -1
		if events {
			old, now = afterEvents, changedEvents
			eventIndex = 0
		}
		changedKey := rowKey{0, 0, eventIndex}
		assert.Equal(t, old[changedKey].ID, now[changedKey].ID, "edit keeps logical row identity")
		assert.NotEqual(t, old[changedKey].Xmin, now[changedKey].Xmin, "edit updates actual row")
		assert.Equal(t, old[rowKey{4, 0, eventIndex}], now[rowKey{4, 0, eventIndex}], "unaffected neighbor")
		assert.NotContains(t, now, rowKey{2, 0, eventIndex}, "removed call")
		assert.NotContains(t, now, rowKey{3, 0, eventIndex}, "removed message")
	}
	assert.NotContains(t, changedEvents, rowKey{0, 0, 1}, "removed second event at retained call")
	assert.NotContains(t, changedEvents, rowKey{1, 0, 0})
	assert.Contains(t, changedEvents, rowKey{1, 1, 0})
	parity()
	push(true)
	for key, row := range identities(false) {
		assert.NotEqual(t, changedCalls[key].ID, row.ID, "explicit full replaces calls")
	}
	for key, row := range identities(true) {
		assert.NotEqual(t, changedEvents[key].ID, row.ID, "explicit full replaces events")
	}
	parity()
	for i := range msgs {
		msgs[i].ToolCalls = nil
	}
	push(false)
	assert.Empty(t, identities(false), "authoritative empty call set")
	assert.Empty(t, identities(true), "authoritative empty event set")
	parity()
}
