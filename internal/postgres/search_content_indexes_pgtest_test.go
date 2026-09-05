//go:build pgtest

package postgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/db"
)

func TestPGSearchContentToolIndexUpgrade(t *testing.T) {
	store := setupContentSearch(t)
	ctx := t.Context()
	indexes := []struct{ name, table, column string }{
		{"idx_tool_calls_input_trgm", "tool_calls", "input_json"},
		{"idx_tool_calls_result_trgm", "tool_calls", "result_content"},
		{"idx_tool_result_events_content_trgm", "tool_result_events", "content"},
	}
	// Start with the previous schema: messages indexed, tool payloads unindexed.
	for _, index := range indexes {
		_, err := store.DB().ExecContext(ctx, "DROP INDEX "+index.name)
		require.NoError(t, err)
	}
	insertCSSession(t, store, "index-upgrade", "project", "claude",
		"2026-05-01T10:00:00Z", "2026-05-01T10:30:00Z")
	insertCSMessage(t, store, "index-upgrade", 1, "assistant",
		"Inspecting the tool evidence", "2026-05-01T10:01:00Z", false)
	const needle = "quartz-index-needle"
	insertCSToolCall(t, store, "index-upgrade", 1, 0, "read", "input-hit", needle, "")
	insertCSToolCall(t, store, "index-upgrade", 1, 1, "read", "result-hit", "{}", needle)
	insertCSToolCall(t, store, "index-upgrade", 1, 2, "read", "event-hit", "{}", needle)
	insertCSToolResultEvent(t, store, "index-upgrade", 1, 2, 0, "event-hit", needle)
	_, err := store.DB().ExecContext(ctx, `
		INSERT INTO tool_calls
			(session_id, message_ordinal, call_index, tool_name, category,
			 tool_use_id, input_json, result_content)
		SELECT 'index-upgrade', 1, g + 10, 'read', 'read', 'noise-' || g,
			repeat(md5(g::text), 16), repeat(md5((g + 1)::text), 16)
		FROM generate_series(1, 3000) g;
		INSERT INTO tool_result_events
			(session_id, tool_call_message_ordinal, call_index, tool_use_id,
			 source, status, content, event_index)
		SELECT 'index-upgrade', 1, g + 10, 'noise-' || g, 'stdout', 'ok',
			repeat(md5((g + 2)::text), 16), 0
		FROM generate_series(1, 3000) g;`)
	require.NoError(t, err)
	filter := db.ContentSearchFilter{Pattern: needle, Mode: "substring", Limit: 20}
	before, err := store.SearchContent(ctx, filter)
	require.NoError(t, err)
	require.Len(t, before.Matches, 3, "result-event dedup must preserve all three sources")

	createContentSearchIndexesPG(ctx, store.DB())
	createContentSearchIndexesPG(ctx, store.DB()) // Existing stores upgrade idempotently.
	_, err = store.DB().ExecContext(ctx, "ANALYZE tool_calls; ANALYZE tool_result_events")
	require.NoError(t, err)
	after, err := store.SearchContent(ctx, filter)
	require.NoError(t, err)
	assert.Equal(t, before, after, "index upgrade must preserve search results and ordering")

	for _, index := range indexes {
		t.Run(index.name, func(t *testing.T) {
			rows, err := store.DB().QueryContext(ctx, fmt.Sprintf(
				"EXPLAIN SELECT id FROM %s WHERE %s ILIKE $1", index.table, index.column,
			), "%"+needle+"%")
			require.NoError(t, err)
			defer rows.Close()
			var plan []string
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				plan = append(plan, line)
			}
			require.NoError(t, rows.Err())
			assert.Contains(t, strings.Join(plan, "\n"), index.name,
				"selective tool-content lookup should use the index without planner overrides")
		})
	}
}
