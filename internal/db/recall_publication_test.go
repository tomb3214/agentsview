package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecallPublicationSnapshotIsScopedAndRevisioned(t *testing.T) {
	database := testDB(t)
	ctx := context.Background()
	insertSession(t, database, "recall-publish-a", "project-a")
	insertSession(t, database, "recall-publish-b", "project-b")
	for _, fixture := range []struct {
		id      string
		session string
		project string
	}{
		{"entry-a", "recall-publish-a", "project-a"},
		{"entry-b", "recall-publish-b", "project-b"},
	} {
		_, err := database.InsertRecallEntry(RecallEntry{
			ID: fixture.id, Type: "fact", Scope: "project", Status: "accepted",
			Title: "Published fact", Body: "Bounded corpus",
			Project: fixture.project, SourceSessionID: fixture.session,
			ProvenanceOK: true,
			Evidence: []RecallEvidence{{
				SessionID: fixture.session, MessageStartOrdinal: 1,
				MessageEndOrdinal: 2, Snippet: "bounded evidence",
			}},
		})
		require.NoError(t, err)
	}

	snapshot, err := database.RecallPublicationSnapshot(
		ctx, []string{"project-a"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, snapshot.Entries, 1)
	assert.Equal(t, "entry-a", snapshot.Entries[0].ID)
	require.Len(t, snapshot.Entries[0].Evidence, 1)
	assert.NotEmpty(t, snapshot.Revision)

	_, err = database.getWriter().Exec(`
		UPDATE recall_evidence SET snippet = 'changed evidence'
		WHERE entry_id = 'entry-a'`)
	require.NoError(t, err)
	changed, err := database.RecallPublicationSnapshot(
		ctx, []string{"project-a"}, nil,
	)
	require.NoError(t, err)
	assert.NotEqual(t, snapshot.Revision, changed.Revision)
	assert.Equal(t, "changed evidence", changed.Entries[0].Evidence[0].Snippet)

	excluded, err := database.RecallPublicationSnapshot(
		ctx, nil, []string{"project-a"},
	)
	require.NoError(t, err)
	require.Len(t, excluded.Entries, 1)
	assert.Equal(t, "entry-b", excluded.Entries[0].ID)
}
