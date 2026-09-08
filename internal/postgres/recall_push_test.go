package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
)

type recallInsertProbe struct {
	queries []string
	args    [][]any
	failAt  int
}

func (p *recallInsertProbe) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	p.queries = append(p.queries, query)
	p.args = append(p.args, append([]any(nil), args...))
	if len(p.queries) == p.failAt {
		return nil, errors.New("insert rejected")
	}
	return nil, nil
}

func TestRecallPublicationBatchesRequestsInEvidenceOrder(t *testing.T) {
	entries := make([]db.RecallEntry, 201)
	for i := range entries {
		entries[i] = db.RecallEntry{ID: fmt.Sprintf("entry-%03d", i), Evidence: []db.RecallEvidence{
			{SessionID: "source", Snippet: fmt.Sprintf("%d first", i)},
			{SessionID: "source", Snippet: fmt.Sprintf("%d second", i)},
		}}
	}
	p := &recallInsertProbe{}
	require.NoError(t, insertPGRecallPublication(context.Background(), p, "device", entries))
	require.Len(t, p.queries, 8, "201 entries plus402 evidence need3+5 requests, not603")
	for i, n := range []int{2600, 2600, 26, 900, 900, 900, 900, 18} {
		assert.Len(t, p.args[i], n)
	}
	for i := range 3 {
		assert.Contains(t, p.queries[i], "INSERT INTO recall_entries")
		assert.Contains(t, p.queries[i], "$25::timestamptz,$26::timestamptz")
	}
	var evidence []any
	for i := 3; i < len(p.queries); i++ {
		assert.Contains(t, p.queries[i], "INSERT INTO recall_evidence")
		evidence = append(evidence, p.args[i]...)
	}
	for i, entry := range entries {
		for j, e := range entry.Evidence {
			offset := (i*2 + j) * 9
			assert.Equal(t, entry.ID, evidence[offset])
			assert.Equal(t, e.Snippet, evidence[offset+8])
		}
	}
	p = &recallInsertProbe{failAt: 4}
	require.ErrorContains(t, insertPGRecallPublication(context.Background(), p, "device", entries), "insert rejected")
	assert.Len(t, p.queries, 4, "a failed batch stops publication")
	p = &recallInsertProbe{}
	require.NoError(t, insertPGRecallPublication(context.Background(), p, "device", nil))
	assert.Empty(t, p.queries)
}
