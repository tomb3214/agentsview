//go:build pgtest

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
)

func TestNativeCandidateBranchesScopeAndFullPassage(t *testing.T) {
	s := setupContentSearch(t)
	ctx := context.Background()
	var installed bool
	require.NoError(t, s.pg.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_search')").Scan(&installed))
	if !installed {
		t.Skip("requires commissioned pg_search native fixture")
	}
	_, err := ensureVectorBaseSchemaPG(ctx, s.pg)
	require.NoError(t, err)
	require.NoError(t, ensureVectorChunkTable(ctx, s.pg, 1, 3))
	for _, id := range []string{"allowed", "other", "deleted"} {
		insertCSSession(t, s, id, "project", "agent", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z")
	}
	_, err = s.pg.Exec("UPDATE sessions SET machine='other-machine' WHERE id='other'")
	require.NoError(t, err)
	text := "éé database migration " + strings.Repeat("complete source passage ", 20)
	for _, id := range []string{"allowed", "other", "deleted"} {
		ordinal := 0
		if id == "deleted" {
			ordinal = -1
		}
		_, err = s.pg.Exec("INSERT INTO vector_documents(doc_key,session_id,ordinal,ordinal_end,content,content_hash) VALUES($1,$1,$2,0,$3,'hash')", id, ordinal, text)
		require.NoError(t, err)
		_, err = s.pg.Exec("INSERT INTO vector_chunks_g1(doc_key,chunk_index,embedding) VALUES($1,0,'[1,0,0]')", id)
		require.NoError(t, err)
	}
	_, err = s.pg.Exec("CREATE INDEX candidate_bm25 ON vector_documents USING bm25(doc_key,content) WITH(key_field='doc_key')")
	require.NoError(t, err)
	s.SetVectorSearcher(NewVectorSearcher(s.pg, 1, 3, 1000, func(context.Context, string) ([]float32, error) { return []float32{1, 0, 0}, nil }))
	got, err := s.SearchContent(ctx, db.ContentSearchFilter{Candidates: true, Mode: "hybrid", Pattern: "database migration", Machine: "test-machine", Scope: "all", IncludeOneShot: true})
	require.NoError(t, err)
	require.Len(t, got.Rankings, 2)
	for _, branch := range got.Rankings {
		require.Len(t, branch, 1)
		assert.Equal(t, "allowed", branch[0].SessionID)
		assert.Equal(t, text, branch[0].Text)
		assert.Equal(t, 0, branch[0].ChunkIndex)
	}
	assert.Equal(t, "bm25", got.LexicalMethod)
	assert.Equal(t, int64(1), got.Generation)
	// More than one page of matching documents exercises late highlighting,
	// exact tie ordering, and a match in a later chunk of a Unicode passage.
	longText := strings.Repeat("ordinary source text ", 55) + "éé database migration"
	for i := range 105 {
		key := fmt.Sprintf("bulk-%03d", i)
		_, err = s.pg.Exec("INSERT INTO vector_documents(doc_key,session_id,ordinal,ordinal_end,content,content_hash) VALUES($1,'allowed',$2,$2,$3,'bulk-hash')", key, i+1, longText)
		require.NoError(t, err)
		_, err = s.pg.Exec("INSERT INTO vector_chunks_g1(doc_key,chunk_index,embedding) VALUES($1,1,'[1,0,0]')", key)
		require.NoError(t, err)
	}
	got, err = s.SearchContent(ctx, db.ContentSearchFilter{Candidates: true, Mode: "hybrid", Pattern: "database migration", Machine: "test-machine", Scope: "all", IncludeOneShot: true})
	require.NoError(t, err)
	require.Len(t, got.Rankings[0], 100)
	tx, err := s.pg.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	// Keep the previous production query as the complete ordered reference.
	baseline := `SELECT d.doc_key,d.session_id,d.content_hash,d.ordinal,d.ordinal_end,d.content,s.machine,s.project,s.agent,to_json(pdb.snippet_positions(d.content,1))::text
		FROM vector_documents d JOIN sessions s ON s.id=d.session_id
		WHERE d.ordinal>=0 AND s.machine='test-machine'
		AND EXISTS(SELECT 1 FROM vector_chunks_g1 c WHERE c.doc_key=d.doc_key)
		AND d.content ||| $1 ORDER BY pdb.score(d.doc_key) DESC,d.doc_key COLLATE "C" LIMIT 100`
	want, err := scanCandidateRows(ctx, tx, baseline, []any{"database migration"}, 1000, true)
	require.NoError(t, err)
	assert.Equal(t, want, got.Rankings[0])
	for _, candidate := range got.Rankings[0] {
		if strings.HasPrefix(candidate.DocKey, "bulk-") {
			assert.Equal(t, 1, candidate.ChunkIndex)
			assert.Contains(t, candidate.Text, "database migration")
		}
	}

}
