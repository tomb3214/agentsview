package importer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func prepareFixture(t *testing.T) (string, string, string) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	require.NoError(t, os.Mkdir(source, 0700))
	manifest := ChatGPTPreparationManifest{Version: 1, MembershipEvidence: "native-project-membership.json#verified", ConversationIDs: []string{"cg-1"}}
	for i, data := range []string{`[{"id":"personal","mapping":{},"secret":"unapproved"}]`, testChatGPTConv, `[]`} {
		name := fmt.Sprintf("conversations-%03d.json", i)
		require.NoError(t, os.WriteFile(filepath.Join(source, name), []byte(data), 0600))
		manifest.Shards = append(manifest.Shards, PreparationFile{Name: name, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(data)))})
	}
	b, err := json.Marshal(manifest)
	require.NoError(t, err)
	path := filepath.Join(root, "manifest.json")
	require.NoError(t, os.WriteFile(path, b, 0600))
	return source, path, filepath.Join(root, "prepared")
}

func TestPrepareChatGPTAdmissionAndReplay(t *testing.T) {
	source, manifest, out := prepareFixture(t)
	receipt, err := PrepareChatGPTExport(context.Background(), source, manifest, out)
	require.NoError(t, err)
	require.Equal(t, 1, receipt.Selected)
	require.Equal(t, 1, receipt.Excluded)
	data, err := os.ReadFile(filepath.Join(out, "conversations.json"))
	require.NoError(t, err)
	require.NotContains(t, string(data), "unapproved")
	var original, prepared []json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(testChatGPTConv), &original))
	require.NoError(t, json.Unmarshal(data, &prepared))
	require.Equal(t, string(original[0]), string(prepared[0]))
	info, err := os.Stat(out)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	// Windows mode bits do not represent POSIX directory permissions.
	if runtime.GOOS != "windows" {
		require.Equal(t, os.FileMode(0700), info.Mode().Perm())
	}
	d := testDB(t)
	stats, err := ImportChatGPT(context.Background(), d, out, t.TempDir(), nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Imported)
	stats, err = ImportChatGPT(context.Background(), d, out, t.TempDir(), nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Skipped)
	_, err = PrepareChatGPTExport(context.Background(), source, manifest, out)
	require.Error(t, err)
}

func TestPrepareChatGPTRejectsBeforePublication(t *testing.T) {
	for _, kind := range []string{"hash", "missing", "duplicate", "malformed", "extra-shard", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			source, manifest, out := prepareFixture(t)
			b, err := os.ReadFile(manifest)
			require.NoError(t, err)
			var m ChatGPTPreparationManifest
			require.NoError(t, json.Unmarshal(b, &m))
			switch kind {
			case "hash":
				m.Shards[0].SHA256 = fmt.Sprintf("%064d", 0)
			case "missing":
				m.ConversationIDs = append(m.ConversationIDs, "missing")
			case "duplicate", "malformed":
				data := testChatGPTConv
				if kind == "malformed" {
					data = `[broken`
				}
				require.NoError(t, os.WriteFile(filepath.Join(source, m.Shards[0].Name), []byte(data), 0600))
				m.Shards[0].SHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(data)))
			case "extra-shard":
				require.NoError(t, os.WriteFile(filepath.Join(source, "conversations-999.json"), []byte(`[]`), 0600))
			case "symlink":
				path := filepath.Join(source, m.Shards[0].Name)
				require.NoError(t, os.Rename(path, path+".original"))
				require.NoError(t, os.Symlink(path+".original", path))
			}
			b, err = json.Marshal(m)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(manifest, b, 0600))
			_, err = PrepareChatGPTExport(context.Background(), source, manifest, out)
			require.Error(t, err)
			_, err = os.Lstat(out)
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestPrepareChatGPTPreservesBranchesAndSelectsAssets(t *testing.T) {
	source, manifest, out := prepareFixture(t)
	b, err := os.ReadFile(manifest)
	require.NoError(t, err)
	var m ChatGPTPreparationManifest
	require.NoError(t, json.Unmarshal(b, &m))
	data := `[{"id":"cg-1","current_node":"a","mapping":{"a":{"id":"a","parent":null,"message":{"id":"message-a","content":{"parts":[{"asset_pointer":"file-service://file-admitted"}]}}},"b":{"id":"b","parent":null,"message":{"id":"message-b","content":{"parts":["sibling branch"]}}}}}]`
	require.NoError(t, os.WriteFile(filepath.Join(source, m.Shards[1].Name), []byte(data), 0600))
	m.Shards[1].SHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(data)))
	b, err = json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifest, b, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "file-admitted.png"), []byte("selected"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "file-personal.png"), []byte("unselected"), 0600))
	_, err = PrepareChatGPTExport(context.Background(), source, manifest, out)
	require.NoError(t, err)
	b, err = os.ReadFile(filepath.Join(out, "conversations.json"))
	require.NoError(t, err)
	require.Equal(t, data+"\n", string(b))
	b, err = os.ReadFile(filepath.Join(out, "file-admitted.png"))
	require.NoError(t, err)
	require.Equal(t, "selected", string(b))
	_, err = os.Stat(filepath.Join(out, "file-personal.png"))
	require.True(t, os.IsNotExist(err))
}

func TestPreparedChatGPTImportResumesAfterCommittedSession(t *testing.T) {
	source, manifest, out := prepareFixture(t)
	b, err := os.ReadFile(manifest)
	require.NoError(t, err)
	var m ChatGPTPreparationManifest
	require.NoError(t, json.Unmarshal(b, &m))
	second := strings.ReplaceAll(testChatGPTConv, "cg-1", "cg-2")
	require.NoError(t, os.WriteFile(filepath.Join(source, m.Shards[0].Name), []byte(second), 0600))
	m.Shards[0].SHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(second)))
	m.ConversationIDs = append(m.ConversationIDs, "cg-2")
	b, err = json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifest, b, 0600))
	_, err = PrepareChatGPTExport(context.Background(), source, manifest, out)
	require.NoError(t, err)
	d := testDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stats, err := ImportChatGPT(ctx, d, out, t.TempDir(), &ImportCallbacks{OnProgress: func(ImportStats) { cancel() }})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, stats.Imported)
	stats, err = ImportChatGPT(context.Background(), d, out, t.TempDir(), nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Imported)
	require.Equal(t, 1, stats.Skipped)
	stats, err = ImportChatGPT(context.Background(), d, out, t.TempDir(), nil)
	require.NoError(t, err)
	require.Equal(t, 2, stats.Skipped)
}
