//go:build pgtest

package postgres

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/db"
)

func TestPushReusesPreparedLocalMessageSummary(t *testing.T) {
	ctx := context.Background()
	pgURL := testPGURL(t)
	const schema = "agentsview_local_summary_test"
	cleanNamedPGSchema(t, pgURL, schema)
	t.Cleanup(func() { cleanNamedPGSchema(t, pgURL, schema) })
	local, err := db.Open(filepath.Join(t.TempDir(), "local.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	const id = "summary-session"
	require.NoError(t, local.UpsertSession(db.Session{ID: id, Machine: "machine", Project: "project", Agent: "codex", MessageCount: 1}))
	payload := strings.Repeat("retained tool result ", 1024)
	msgs := []db.Message{{SessionID: id, Ordinal: 0, Role: "assistant", Content: "inspect", ContentLength: 7,
		ToolCalls: []db.ToolCall{{ToolName: "Read", Category: "file", InputJSON: `{"path":"sample"}`,
			ResultContent: payload, ResultContentLength: len(payload),
			ResultEvents: []db.ToolResultEvent{{Source: "cli", Status: "ok", Content: payload, ContentLength: len(payload)}}}}}}
	require.NoError(t, local.ReplaceSessionMessages(id, msgs))
	pusher, err := New(pgURL, schema, local, "machine", true, SyncOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pusher.Close()) })
	require.NoError(t, pusher.EnsureSchema(ctx))
	first, err := pusher.Push(ctx, false, nil)
	require.NoError(t, err)
	require.Zero(t, first.Errors)
	var before, after string
	identityQuery := `SELECT id::text || ':' || xmin::text FROM tool_calls WHERE session_id=$1`
	require.NoError(t, pusher.pg.QueryRowContext(ctx, identityQuery, id).Scan(&before))

	// Observe actual SQLite payload reads on its sole, sequential reader.
	// The authorizer reports prepared column reads without logging contents.
	connector := local.Reader().(interface {
		Conn(context.Context) (*sql.Conn, error)
	})
	conn, err := connector.Conn(ctx)
	require.NoError(t, err)
	var prepared atomic.Bool
	var payloadReads atomic.Int64
	require.NoError(t, conn.Raw(func(driverConn any) error {
		driverConn.(*sqlite3.SQLiteConn).RegisterAuthorizer(func(action int, table, column, database string) int {
			if prepared.Load() && action == sqlite3.SQLITE_READ && table == "tool_calls" && column == "result_content" {
				payloadReads.Add(1)
			}
			return sqlite3.SQLITE_OK
		})
		return nil
	}))
	require.NoError(t, conn.Close())

	// Metadata changes must visit the session while leaving its transcript alone.
	sess, err := local.GetArtifactExportSession(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, sess)
	name := "renamed session"
	sess.DisplayName = &name
	require.NoError(t, local.UpsertSession(*sess))
	require.NoError(t, local.SetSyncState("last_push_at", ""))
	require.NoError(t, local.SetSyncState(lastPushBoundaryStateKey, ""))
	result, err := pusher.Push(ctx, false, func(p PushProgress) {
		if p.Phase == "preparing" && p.SessionsTotal > 0 && p.SessionsDone == p.SessionsTotal {
			prepared.Store(true)
		}
	})
	require.NoError(t, err)
	require.Zero(t, result.Errors)
	assert.Equal(t, 1, result.SessionsPushed)
	assert.Zero(t, result.MessagesPushed)
	require.NoError(t, pusher.pg.QueryRowContext(ctx, identityQuery, id).Scan(&after))
	assert.Equal(t, before, after)
	t.Logf("SQLite tool-call payload column reads after preparation: %d", payloadReads.Load())
	assert.Zero(t, payloadReads.Load(), "prepared local fingerprint must avoid rereading unchanged payloads")

	// A supported same-count transcript edit after preparation invalidates reuse.
	prepared.Store(false)
	payloadReads.Store(0)
	require.NoError(t, local.SetSyncState("last_push_at", ""))
	require.NoError(t, local.SetSyncState(lastPushBoundaryStateKey, ""))
	result, err = pusher.Push(ctx, false, func(p PushProgress) {
		if p.Phase == "preparing" && p.SessionsTotal > 0 && p.SessionsDone == p.SessionsTotal {
			msgs[0].Content = "changed"
			require.NoError(t, local.ReplaceSessionMessages(id, msgs))
			prepared.Store(true)
		}
	})
	require.NoError(t, err)
	require.Zero(t, result.Errors)
	assert.Equal(t, 1, result.MessagesPushed)
	assert.Positive(t, payloadReads.Load(), "changed revision must use the original local read path")
	var content string
	require.NoError(t, pusher.pg.QueryRowContext(ctx, `SELECT content FROM messages WHERE session_id=$1`, id).Scan(&content))
	assert.Equal(t, "changed", content)

	// A summary cannot cross an archive/generation switch, even for the same ID.
	state, err := readLocalPushDependencyState(ctx, local, []string{id})
	require.NoError(t, err)
	fp, err := state.messageFingerprint(local, id, "", true)
	require.NoError(t, err)
	digest, err := hashLocalDependencyPayload(fp, nil, nil)
	require.NoError(t, err)
	sess, err = local.GetArtifactExportSession(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, sess)
	summaries := &localPushMessageSummaries{
		archiveID: pusher.archiveID, databaseID: pusher.databaseGeneration,
		sessions: map[string]localPushMessageSummary{id: {
			digest: digest, revision: stringValue(sess.TranscriptRevision),
			modified: stringValue(sess.LocalModifiedAt), count: 1,
		}},
	}
	for _, key := range []string{"archive_id", "database_id"} {
		t.Run(key, func(t *testing.T) {
			var original string
			require.NoError(t, local.Reader().QueryRow(`SELECT value FROM archive_metadata WHERE key=?`, key).Scan(&original))
			require.NoError(t, local.Update(func(tx *sql.Tx) error {
				_, err := tx.Exec(`UPDATE archive_metadata SET value=? WHERE key=?`, "other-identity", key)
				return err
			}))
			_, used, err := summaries.compare(ctx, local, id, 1, &pushMessageComparison{})
			require.NoError(t, err)
			assert.False(t, used)
			require.NoError(t, local.Update(func(tx *sql.Tx) error {
				_, err := tx.Exec(`UPDATE archive_metadata SET value=? WHERE key=?`, original, key)
				return err
			}))
		})
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = summaries.compare(canceled, local, id, 1, &pushMessageComparison{})
	assert.ErrorIs(t, err, context.Canceled)
}
