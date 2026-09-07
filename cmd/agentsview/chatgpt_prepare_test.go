package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareChatGPTCommandRejectsWithoutDatabase(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	command := newRootCommand()
	command.SetOut(&bytes.Buffer{})
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{"import", "prepare-chatgpt", home, "--manifest", filepath.Join(home, "missing.json"), "--output", filepath.Join(home, "prepared")})
	require.ErrorContains(t, command.Execute(), "missing.json")
	entries, err := os.ReadDir(home)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestPrepareChatGPTCommandDoesNotOpenDatabase(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	source := t.TempDir()
	out := filepath.Join(t.TempDir(), "prepared")
	data := []byte(`[{"id":"approved","mapping":{}}]`)
	require.NoError(t, os.WriteFile(filepath.Join(source, "conversations.json"), data, 0600))
	manifest := filepath.Join(source, "manifest.json")
	raw := fmt.Sprintf(`{"version":1,"membership_evidence":"fixture-membership","conversation_ids":["approved"],"shards":[{"name":"conversations.json","sha256":"%x"}]}`, sha256.Sum256(data))
	require.NoError(t, os.WriteFile(manifest, []byte(raw), 0600))
	command := newRootCommand()
	command.SetOut(&bytes.Buffer{})
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{"import", "prepare-chatgpt", source, "--manifest", manifest, "--output", out})
	require.NoError(t, command.Execute())
	entries, err := os.ReadDir(home)
	require.NoError(t, err)
	require.Empty(t, entries)
	_, err = os.Stat(filepath.Join(out, "preparation-receipt.json"))
	require.NoError(t, err)
}
