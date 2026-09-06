package importer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportChatGPTAdditiveRefresh(t *testing.T) {
	d := testDB(t)
	ctx := context.Background()
	dir, assets := t.TempDir(), t.TempDir()
	path := filepath.Join(dir, "conversations.json")
	export := func(leaf, text string) string {
		return fmt.Sprintf(`[{"id":"cg-1","title":"Test","current_node":%q,"mapping":{
   "root":{"id":"root","message":null},
   "user":{"id":"user","parent":"root","message":{"id":"user","create_time":1706745600,"author":{"role":"user"},"content":{"content_type":"text","parts":["Hello"]}}},
   %q:{"id":%q,"parent":"user","message":{"id":%q,"create_time":1706745660,"author":{"role":"assistant"},"content":{"content_type":"text","parts":[%q]}}}
  }}]`, leaf, leaf, leaf, leaf, text)
	}
	first := export("a", "First answer")
	require.NoError(t, os.WriteFile(path, []byte(first), 0600))
	stats, err := ImportChatGPT(ctx, d, dir, assets, nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Imported)
	original, err := d.GetAllMessages(ctx, "chatgpt:cg-1")
	require.NoError(t, err)
	// Reproduce rows written by the earlier importer, which omitted source IDs.
	for i := range original {
		original[i].SourceUUID = ""
	}
	require.NoError(t, d.ReplaceSessionMessages("chatgpt:cg-1", original))
	original, err = d.GetAllMessages(ctx, "chatgpt:cg-1")
	require.NoError(t, err)
	_, err = d.PinMessage("chatgpt:cg-1", original[1].ID, nil)
	require.NoError(t, err)
	name := "My archived conversation"
	require.NoError(t, d.RenameSession("chatgpt:cg-1", &name))
	continued := strings.Replace(first, `"current_node":"a"`, `"current_node":"later"`, 1)
	continued = strings.Replace(continued, `"mapping":{`, `"mapping":{"later":{"id":"later","parent":"a","message":{"id":"later","create_time":1706745720,"author":{"role":"user"},"content":{"content_type":"text","parts":["Continue"]}}},`, 1)
	for _, step := range []struct {
		text                    string
		updated, skipped, count int
	}{
		{export("b", "Alternative answer"), 1, 0, 3},
		{export("b", "Alternative answer"), 0, 1, 3},
		{first, 0, 1, 3},
		{continued, 1, 0, 4},
		{continued, 0, 1, 4},
		{first, 0, 1, 4},
	} {
		require.NoError(t, os.WriteFile(path, []byte(step.text), 0600))
		stats, err = ImportChatGPT(ctx, d, dir, assets, nil)
		require.NoError(t, err)
		assert.Equal(t, step.updated, stats.Updated)
		assert.Equal(t, step.skipped, stats.Skipped)
		msgs, err := d.GetAllMessages(ctx, "chatgpt:cg-1")
		require.NoError(t, err)
		require.Len(t, msgs, step.count)
		assert.Equal(t, "First answer", msgs[1].Content)
		assert.Equal(t, "Alternative answer", msgs[2].Content)
	}
	pins, err := d.ListPinnedMessages(ctx, "chatgpt:cg-1", "")
	require.NoError(t, err)
	assert.Len(t, pins, 1)
	session, err := d.GetSession(ctx, "chatgpt:cg-1")
	require.NoError(t, err)
	assert.Equal(t, &name, session.DisplayName)
	assert.Equal(t, 4, session.MessageCount)
	assert.Equal(t, 2, session.UserMessageCount)
	hits, err := d.SearchSession(ctx, "chatgpt:cg-1", "Continue")
	require.NoError(t, err)
	assert.Len(t, hits, 1)
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, first, string(contents), "import must not change source export")
}

func TestImportChatGPTPreservesToolResults(t *testing.T) {
	d := testDB(t)
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "conversations.json")
	source := `[{"id":"tools","current_node":"tool","mapping":{
 "a":{"id":"a","message":{"id":"answer","author":{"role":"assistant"},"content":{"content_type":"code","text":"print(2)"}}},
 "code":{"id":"code","parent":"a","message":{"id":"code","author":{"role":"tool","name":"python"},"content":{"content_type":"code","text":"print(2)"}}},
 "tool":{"id":"tool","parent":"code","message":{"id":"result","author":{"role":"tool","name":"python"},"content":{"content_type":"execution_output","text":"2"}}}
 }}]`
	require.NoError(t, os.WriteFile(path, []byte(source), 0600))
	_, err := ImportChatGPT(ctx, d, dir, t.TempDir(), nil)
	require.NoError(t, err)
	stats, err := ImportChatGPT(ctx, d, dir, t.TempDir(), nil)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Skipped)
	msgs, err := d.GetAllMessages(ctx, "chatgpt:tools")
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Len(t, msgs[0].ToolCalls, 1)
	assert.Contains(t, msgs[0].ToolCalls[0].ResultContent, "2")
	require.NoError(t, d.SoftDeleteSession("chatgpt:tools"))
	newer := strings.Replace(source, `"text":"print(2)"`, `"text":"print(3)"`, 1)
	require.NoError(t, os.WriteFile(path, []byte(newer), 0600))
	stats, err = ImportChatGPT(ctx, d, dir, t.TempDir(), nil)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Skipped)
	msgs, err = d.GetAllMessages(ctx, "chatgpt:tools")
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Contains(t, msgs[0].ToolCalls[0].ResultContent, "2")
}
