//go:build pgtest

package postgres

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
)

func TestPushMessageReconciliationParityRetentionAndRollback(t *testing.T) {
	ctx := context.Background()
	pgURL := testPGURL(t)
	const schema = "agentsview_message_reconciliation_test"
	const id = "active-conversation"
	cleanNamedPGSchema(t, pgURL, schema)
	t.Cleanup(func() { cleanNamedPGSchema(t, pgURL, schema) })
	local := testDB(t)
	pusher, err := New(pgURL, schema, local, "device", true, SyncOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pusher.Close()) })
	require.NoError(t, pusher.EnsureSchema(ctx))
	session := db.Session{ID: id, Project: "project", Machine: "local", Agent: "codex", MessageCount: 201}
	require.NoError(t, local.UpsertSession(session))
	var messages []db.Message
	for i := range 201 {
		content := fmt.Sprintf("Step %d: quoted ' result with unicode é.\n%s", i, strings.Repeat("stable context ", 80))
		messages = append(messages, db.Message{
			SessionID: id, Ordinal: i, Role: "assistant", Content: content, ContentLength: len(content),
			Timestamp: "2026-01-02T03:04:05.123456Z", SourceUUID: fmt.Sprintf("message-%d", i),
		})
	}
	require.NoError(t, local.InsertMessages(messages))
	result, err := pusher.Push(ctx, false, nil)
	require.NoError(t, err)
	require.Zero(t, result.Errors)
	store, err := NewStore(pgURL, schema, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	note := "keep this explanation"
	_, err = store.PinMessage(id, 100, &note)
	require.NoError(t, err)
	_, err = pusher.pg.ExecContext(ctx, `
		INSERT INTO sessions(id,machine,project,agent) VALUES('other-conversation','other-device','project','codex');
		INSERT INTO messages(session_id,ordinal,role,content) VALUES('other-conversation',0,'user','other owner');
		CREATE TABLE message_writes(operation text NOT NULL);
		CREATE FUNCTION record_message_write() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN INSERT INTO message_writes VALUES(TG_OP); RETURN NULL; END $$;
		CREATE TRIGGER record_message_write AFTER INSERT OR UPDATE OR DELETE ON messages
		FOR EACH ROW EXECUTE FUNCTION record_message_write()`)
	require.NoError(t, err)
	read := func() []string {
		rows, err := pusher.pg.QueryContext(ctx, `SELECT to_jsonb(m)::text FROM messages m WHERE session_id=$1 ORDER BY ordinal`, id)
		require.NoError(t, err)
		defer rows.Close()
		var result []string
		for rows.Next() {
			var row string
			require.NoError(t, rows.Scan(&row))
			result = append(result, row)
		}
		require.NoError(t, rows.Err())
		return result
	}
	identities := func() map[int]string {
		rows, err := pusher.pg.QueryContext(ctx, `SELECT ordinal,ctid::text||'/'||xmin::text FROM messages WHERE session_id=$1`, id)
		require.NoError(t, err)
		defer rows.Close()
		result := make(map[int]string)
		for rows.Next() {
			var ordinal int
			var identity string
			require.NoError(t, rows.Scan(&ordinal, &identity))
			result[ordinal] = identity
		}
		require.NoError(t, rows.Err())
		return result
	}
	writes := func() map[string]int {
		rows, err := pusher.pg.QueryContext(ctx, `SELECT operation,count(*) FROM message_writes GROUP BY operation`)
		require.NoError(t, err)
		defer rows.Close()
		result := make(map[string]int)
		for rows.Next() {
			var op string
			var count int
			require.NoError(t, rows.Scan(&op, &count))
			result[op] = count
		}
		require.NoError(t, rows.Err())
		return result
	}
	assertPin := func() {
		var ordinal int
		var gotNote string
		require.NoError(t, pusher.pg.QueryRowContext(ctx, `SELECT ordinal,note FROM pinned_messages WHERE session_id=$1`, id).Scan(&ordinal, &gotNote))
		assert.Equal(t, 100, ordinal)
		assert.Equal(t, note, gotNote)
	}
	before, beforeIDs := read(), identities()
	watermark, err := local.GetSyncState("last_push_at")
	require.NoError(t, err)
	// A corrected transcript removes an old tail, appends three new messages,
	// edits all non-key fields of one row, and clears another row's timestamp.
	changed := slices.Clone(messages[:190])
	changed[40] = db.Message{
		SessionID: id, Ordinal: 40, Role: "user", Content: "corrected answer", ThinkingText: "corrected reasoning",
		Timestamp: "2026-01-03T04:05:06.654321Z", HasThinking: true, HasToolUse: true,
		ContentLength: len("corrected answer"), IsSystem: true, Model: "updated-model",
		TokenUsage: []byte(`{"input_tokens":17,"output_tokens":9}`), ContextTokens: 17, OutputTokens: 9,
		HasContextTokens: true, HasOutputTokens: true, ClaudeMessageID: "provider-message", ClaudeRequestID: "provider-request",
		SourceType: "message", SourceSubtype: "correction", PromptSource: "human", SourceUUID: "corrected-uuid",
		SourceParentUUID: "parent-uuid", IsSidechain: true, IsCompactBoundary: true,
	}
	changed[101].Timestamp = ""
	for ordinal := 201; ordinal < 204; ordinal++ {
		changed = append(changed, db.Message{SessionID: id, Ordinal: ordinal, Role: "assistant", Content: "new result", ContentLength: 10, SourceUUID: fmt.Sprintf("message-%d", ordinal)})
	}
	session.MessageCount = len(changed)
	require.NoError(t, local.UpsertSession(session))
	require.NoError(t, local.ReplaceSessionMessages(id, changed))
	_, err = pusher.pg.ExecContext(ctx, `
		CREATE FUNCTION reject_late_message() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.ordinal=202 THEN RAISE EXCEPTION 'later message batch rejected'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER reject_late_message BEFORE INSERT OR UPDATE ON messages
		FOR EACH ROW EXECUTE FUNCTION reject_late_message()`)
	require.NoError(t, err)
	result, err = pusher.Push(ctx, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Errors)
	assert.Equal(t, before, read(), "later batch failure rolls back earlier message updates")
	assert.Equal(t, beforeIDs, identities())
	assert.Empty(t, writes(), "failed transaction leaves no row changes")
	assertPin()
	gotWatermark, err := local.GetSyncState("last_push_at")
	require.NoError(t, err)
	assert.Equal(t, watermark, gotWatermark)
	_, err = pusher.pg.ExecContext(ctx, `DROP TRIGGER reject_late_message ON messages`)
	require.NoError(t, err)
	result, err = pusher.Push(ctx, false, nil)
	require.NoError(t, err)
	require.Zero(t, result.Errors)
	assert.Equal(t, map[string]int{"INSERT": 3, "UPDATE": 2, "DELETE": 11}, writes())
	afterIDs := identities()
	for ordinal := range 190 {
		if ordinal == 40 || ordinal == 101 {
			assert.NotEqual(t, beforeIDs[ordinal], afterIDs[ordinal])
		} else {
			assert.Equal(t, beforeIDs[ordinal], afterIDs[ordinal], "unchanged ordinal %d retains its heap tuple", ordinal)
		}
	}
	assert.Len(t, afterIDs, 193)
	assertPin()
	reconciled := read()
	// Compare the same original-to-corrected transition with the preserved
	// full-replacement path, counting actual row DML rather than estimating it.
	tx, err := pusher.pg.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `DELETE FROM messages WHERE session_id=$1`, id)
	require.NoError(t, err)
	require.NoError(t, bulkInsertMessages(ctx, tx, id, messages, nil))
	require.NoError(t, tx.Commit())
	_, err = pusher.pg.ExecContext(ctx, `DELETE FROM message_writes`)
	require.NoError(t, err)
	result, err = pusher.Push(ctx, true, nil)
	require.NoError(t, err)
	require.Zero(t, result.Errors)
	assert.Equal(t, reconciled, read(), "all 25 fields match the unchanged full-replacement path")
	assert.Equal(t, map[string]int{"INSERT": 193, "DELETE": 201}, writes(), "explicit full retains replacement behavior")
	assertPin()
	var otherContent string
	require.NoError(t, pusher.pg.QueryRowContext(ctx, `SELECT content FROM messages WHERE session_id='other-conversation'`).Scan(&otherContent))
	assert.Equal(t, "other owner", otherContent)
}
