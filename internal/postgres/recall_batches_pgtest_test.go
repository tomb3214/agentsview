//go:build pgtest

package postgres

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
)

func TestRecallPublicationBatchParityAndRollback(t *testing.T) {
	ctx := context.Background()
	pgURL := testPGURL(t)
	const schema = "agentsview_recall_batches_test"
	cleanNamedPGSchema(t, pgURL, schema)
	t.Cleanup(func() { cleanNamedPGSchema(t, pgURL, schema) })
	local := testDB(t)
	require.NoError(t, local.UpsertSession(db.Session{ID: "source", Project: "project", Machine: "local", Agent: "codex"}))
	confidence := 0.75
	for i := 0; i < 201; i++ {
		entry := db.RecallEntry{
			ID: fmt.Sprintf("entry-%03d", i), Type: "procedure", Scope: "project", Status: "accepted", ReviewState: "unreviewed_auto",
			Title: fmt.Sprintf("Procedure %d", i), Body: "Quoted ' body\nwith unicode é", Trigger: "when needed", Uncertainty: "bounded", Project: "project",
			CWD: "workspace", GitBranch: "topic", Agent: "codex", SourceSessionID: "source", SourceEpisodeID: "episode", SourceRunID: "run",
			ExtractorMethod: "episode", Model: "test-model", Transferable: i%2 == 0, ProvenanceOK: true, SupersedesEntryID: "prior", SupersededByEntryID: "later",
			CreatedAt: "2026-01-02T03:04:05.123Z", UpdatedAt: "2026-01-03T04:05:06.456Z",
		}
		if i%2 == 0 {
			entry.Confidence = &confidence
		}
		for j := 0; j < 2; j++ {
			entry.Evidence = append(entry.Evidence, db.RecallEvidence{SessionID: "source", MessageStartOrdinal: i*2 + j, MessageEndOrdinal: i*2 + j + 1, MessageStartSourceUUID: "start", MessageEndSourceUUID: "end", ContentDigest: "digest", ToolUseID: "tool", Snippet: fmt.Sprintf("evidence %03d/%d", i, j)})
		}
		_, err := local.InsertRecallEntry(entry)
		require.NoError(t, err)
	}
	syncer, err := New(pgURL, schema, local, "device", true, SyncOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, syncer.Close()) })
	require.NoError(t, syncer.EnsureSchema(ctx))
	_, err = syncer.pg.ExecContext(ctx, `INSERT INTO sessions(id,machine,project,agent) VALUES('source','device','project','codex')`)
	require.NoError(t, err)
	_, err = syncer.pg.ExecContext(ctx, `INSERT INTO recall_entries(id,machine,type,scope,title,body,source_session_id,created_at,updated_at) VALUES('other-entry','other-device','fact','project','title','body','source',now(),now())`)
	require.NoError(t, err)
	syncer.databaseGeneration, err = local.GetDatabaseID(ctx)
	require.NoError(t, err)
	read := func() []db.RecallEntry {
		rows, err := syncer.pg.QueryContext(ctx, `SELECT to_jsonb(e)::text FROM recall_entries e WHERE machine='device' ORDER BY id`)
		require.NoError(t, err)
		var result []db.RecallEntry
		for rows.Next() {
			var raw string
			require.NoError(t, rows.Scan(&raw))
			var entry db.RecallEntry
			require.NoError(t, json.Unmarshal([]byte(raw), &entry))
			result = append(result, entry)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		for i := range result {
			rows, err := syncer.pg.QueryContext(ctx, `SELECT to_jsonb(e)::text FROM recall_evidence e WHERE entry_id=$1 ORDER BY id`, result[i].ID)
			require.NoError(t, err)
			for rows.Next() {
				var raw string
				require.NoError(t, rows.Scan(&raw))
				var evidence db.RecallEvidence
				require.NoError(t, json.Unmarshal([]byte(raw), &evidence))
				result[i].Evidence = append(result[i].Evidence, evidence)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
		}
		return result
	}
	parity := func() {
		snapshot, err := local.RecallPublicationSnapshot(ctx, nil, nil)
		require.NoError(t, err)
		actual := read()
		require.Len(t, actual, len(snapshot.Entries))
		for i := range actual {
			expected := snapshot.Entries[i]
			expected.Machine = "device"
			for _, pair := range [][2]string{{expected.CreatedAt, actual[i].CreatedAt}, {expected.UpdatedAt, actual[i].UpdatedAt}} {
				a, err := time.Parse(time.RFC3339Nano, pair[0])
				require.NoError(t, err)
				b, err := time.Parse(time.RFC3339Nano, pair[1])
				require.NoError(t, err)
				assert.True(t, a.Equal(b))
			}
			actual[i].CreatedAt = expected.CreatedAt
			actual[i].UpdatedAt = expected.UpdatedAt
			for j := range expected.Evidence {
				expected.Evidence[j].ID = 0
				actual[i].Evidence[j].ID = 0
			}
			assert.Equal(t, expected, actual[i])
		}
	}
	require.NoError(t, syncer.PushRecall(ctx, false))
	parity()
	before := read()
	marker, err := local.GetSyncState(recallPublicationRevisionStateKey)
	require.NoError(t, err)
	require.NoError(t, syncer.PushRecall(ctx, false))
	assert.Equal(t, before, read(), "unchanged snapshot preserves evidence identities")
	require.NoError(t, syncer.PushRecall(ctx, true))
	parity()
	assert.NotEqual(t, before[0].Evidence[0].ID, read()[0].Evidence[0].ID, "explicit full still republishes")
	before = read()
	_, err = local.InsertRecallEntry(db.RecallEntry{ID: "entry-extra", Type: "fact", Scope: "project", Title: "new", Body: "new revision", Project: "project", SourceSessionID: "source", ProvenanceOK: true})
	require.NoError(t, err)
	_, err = syncer.pg.ExecContext(ctx, `CREATE FUNCTION reject_late_evidence() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.snippet = 'evidence 060/1' THEN RAISE EXCEPTION 'later evidence batch rejected'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_late_evidence BEFORE INSERT ON recall_evidence FOR EACH ROW EXECUTE FUNCTION reject_late_evidence()`)
	require.NoError(t, err)
	require.ErrorContains(t, syncer.PushRecall(ctx, false), "later evidence batch rejected")
	assert.Equal(t, before, read(), "failure after earlier successful batches rolls back the whole publication")
	got, err := local.GetSyncState(recallPublicationRevisionStateKey)
	require.NoError(t, err)
	assert.Equal(t, marker, got)
	_, err = syncer.pg.ExecContext(ctx, `DROP TRIGGER reject_late_evidence ON recall_evidence`)
	require.NoError(t, err)
	require.NoError(t, syncer.PushRecall(ctx, false))
	parity()
	got, err = local.GetSyncState(recallPublicationRevisionStateKey)
	require.NoError(t, err)
	assert.NotEqual(t, marker, got)
	var otherCount int
	require.NoError(t, syncer.pg.QueryRowContext(ctx, `SELECT count(*) FROM recall_entries WHERE id='other-entry' AND machine='other-device'`).Scan(&otherCount))
	assert.Equal(t, 1, otherCount, "other device publication remains intact")
}
