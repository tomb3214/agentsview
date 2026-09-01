//go:build pgtest

package postgres

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/db"
)

func TestRecallPublicationIsIncrementalAndMachineScoped(t *testing.T) {
	pgURL := testPGURL(t)
	const schema = "agentsview_recall_publication_test"
	cleanNamedPGSchema(t, pgURL, schema)
	t.Cleanup(func() { cleanNamedPGSchema(t, pgURL, schema) })
	ctx := context.Background()

	local, err := db.Open(filepath.Join(t.TempDir(), "local.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	require.NoError(t, local.UpsertSession(db.Session{
		ID: "recall-source", Project: "tag-ops", Machine: "local", Agent: "codex",
	}))
	_, err = local.InsertRecallEntry(db.RecallEntry{
		ID: "recall-entry", Type: "procedure", Scope: "project",
		Status: "accepted", ReviewState: "unreviewed_auto",
		Title: "Use the protected path", Body: "Deploy through the normal workflow.",
		Project: "tag-ops", SourceSessionID: "recall-source",
		ExtractorMethod: "episode", Model: "test-model", ProvenanceOK: true,
		Evidence: []db.RecallEvidence{{
			SessionID: "recall-source", MessageStartOrdinal: 1,
			MessageEndOrdinal: 2, Snippet: "normal workflow",
		}},
	})
	require.NoError(t, err)

	syncer, err := New(pgURL, schema, local, "tom-macbook", true, SyncOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, syncer.Close()) })
	require.NoError(t, syncer.EnsureSchema(ctx))
	_, err = syncer.pg.ExecContext(ctx, `
		INSERT INTO sessions (id, machine, project, agent)
		VALUES ('recall-source', 'tom-macbook', 'tag-ops', 'codex')`)
	require.NoError(t, err)
	syncer.databaseGeneration, err = local.GetDatabaseID(ctx)
	require.NoError(t, err)
	require.NoError(t, syncer.syncRecallPublication(
		ctx, syncer.effectiveSyncState(), false,
	))

	store := &Store{pg: syncer.pg}
	page, err := store.QueryRecallEntries(ctx, db.RecallQuery{
		Text: "protected workflow", Machine: "tom-macbook", Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, page.RecallEntries, 1)
	assert.Equal(t, "tom-macbook", page.RecallEntries[0].Machine)
	require.Len(t, page.RecallEntries[0].Evidence, 1)

	_, err = syncer.pg.ExecContext(ctx, `
		UPDATE recall_entries SET title = 'remote sentinel'
		WHERE id = 'recall-entry'`)
	require.NoError(t, err)
	require.NoError(t, syncer.syncRecallPublication(
		ctx, syncer.effectiveSyncState(), false,
	))
	var title string
	require.NoError(t, syncer.pg.QueryRowContext(ctx,
		"SELECT title FROM recall_entries WHERE id = 'recall-entry'",
	).Scan(&title))
	assert.Equal(t, "remote sentinel", title, "unchanged local revision must be a no-op")

	_, err = local.InsertRecallEntry(db.RecallEntry{
		ID: "recall-entry-two", Type: "fact", Scope: "project",
		Status: "accepted", Title: "Second fact", Body: "Revision changed.",
		Project: "tag-ops", SourceSessionID: "recall-source", ProvenanceOK: true,
	})
	require.NoError(t, err)
	require.NoError(t, syncer.syncRecallPublication(
		ctx, syncer.effectiveSyncState(), false,
	))
	require.NoError(t, syncer.pg.QueryRowContext(ctx,
		"SELECT title FROM recall_entries WHERE id = 'recall-entry'",
	).Scan(&title))
	assert.Equal(t, "Use the protected path", title)

	other, err := store.QueryRecallEntries(ctx, db.RecallQuery{
		Text: "protected workflow", Machine: "gus-macbook", Limit: 10,
	})
	require.NoError(t, err)
	assert.Empty(t, other.RecallEntries)
}
