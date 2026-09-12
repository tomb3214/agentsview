//go:build pgtest

package postgres

import (
	"context"
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
}
