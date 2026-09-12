package service_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/service"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCandidateRequestRejectsIncompatibleOptions(t *testing.T) {
	d := dbtest.OpenTestDB(t)
	be := service.NewDirectBackend(d, nil)
	for _, req := range []service.ContentSearchRequest{
		{Candidates: true, Mode: "fts"}, {Candidates: true, Mode: "hybrid", Context: 1},
		{Candidates: true, Mode: "hybrid", Cursor: 1}, {Candidates: true, Mode: "hybrid", Reveal: true},
	} {
		_, err := be.SearchContent(context.Background(), req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "candidates requires hybrid")
	}
	_, err := be.SearchContent(context.Background(), service.ContentSearchRequest{Candidates: true, Mode: "hybrid", Pattern: "query"})
	require.ErrorIs(t, err, db.ErrSemanticUnavailable)
}

func TestHTTPCandidateSearchPreservesBranchesAndIntent(t *testing.T) {
	var candidates, intent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		candidates = r.URL.Query().Get("candidates")
		intent = r.Header.Get("X-AgentsView-Search-Intent")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"matches":[],"rankings":[[{"session_id":"one","text":"whole passage"}],[]],"generation":1,"lexical_method":"bm25"}`))
	}))
	defer srv.Close()
	be := service.NewHTTPBackend(srv.URL, "", true)
	got, err := be.SearchContent(context.Background(), service.ContentSearchRequest{Candidates: true, Mode: "hybrid", Pattern: "query"})
	require.NoError(t, err)
	require.Len(t, got.Rankings, 2)
	assert.Equal(t, "whole passage", got.Rankings[0][0].Text)
	assert.Equal(t, "true", candidates)
	assert.Equal(t, "semantic", intent)
}
