// ABOUTME: Verifies the exact source-retirement CLI contract used by managed
// ABOUTME: native archive offload without deleting transcript data itself.
package main

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/service"
)

func TestSessionSourceRetireCommandSendsExactProof(t *testing.T) {
	t.Parallel()
	const hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	var got service.SessionSourceRetirementInput
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v1/sessions/source-retire", r.URL.Path)
		require.NoError(t, json.UnmarshalRead(r.Body, &got))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"session_id":"codex:task","machine":"machine","agent":"codex","file_path":"/archive/task.jsonl","file_hash":"` + hash + `","message_count":2,"retired_at":"2026-09-03T00:00:00Z"}`))
	}))
	defer server.Close()

	output, err := executeCommand(
		newRootCommand(),
		"session", "source-retire", "codex:task",
		"--server", server.URL,
		"--machine", "machine",
		"--agent", "codex",
		"--path", "/archive/task.jsonl",
		"--sha256", hash,
		"--format", "json",
	)

	require.NoError(t, err)
	assert.Equal(t, "codex:task", got.SessionID)
	assert.Equal(t, "/archive/task.jsonl", got.FilePath)
	var receipt map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &receipt))
	assert.Equal(t, "codex:task", receipt["session_id"])
}
