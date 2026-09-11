package db

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBatchReplacementRetainsIdenticalMessageRows(t *testing.T) {
	for _, size := range []int{4, 400} {
		d := testDB(t)
		write := messageCountWrite("session", size)
		_, err := d.WriteSessionBatchContext(context.Background(), []SessionBatchWrite{write})
		require.NoError(t, err)
		ids := messageIDsByOrdinal(t, d, "session")
		_, err = d.PinMessage("session", ids[0], Ptr("retain note"))
		require.NoError(t, err)
		_, err = d.getWriter().Exec(`CREATE TABLE message_changes(kind TEXT);
			CREATE TRIGGER observe_message_insert AFTER INSERT ON messages BEGIN
			INSERT INTO message_changes VALUES ('insert'); END;
			CREATE TRIGGER observe_message_delete AFTER DELETE ON messages BEGIN
			INSERT INTO message_changes VALUES ('delete'); END;
			CREATE TRIGGER observe_message_update AFTER UPDATE ON messages BEGIN
			INSERT INTO message_changes VALUES ('update'); END;`)
		require.NoError(t, err)
		write.Session.SourceVersion = "new-source-version"
		_, err = d.WriteSessionBatchContext(context.Background(), []SessionBatchWrite{write})
		require.NoError(t, err)
		var changes int
		require.NoError(t, d.getReader().QueryRow("SELECT count(*) FROM message_changes").Scan(&changes))
		require.Zero(t, changes, "unchanged message work must stay zero as the session grows")
		require.Equal(t, ids, messageIDsByOrdinal(t, d, "session"))
		pins, err := d.GetPinnedMessageIDs(context.Background(), "session")
		require.NoError(t, err)
		require.True(t, pins[ids[0]])
		var version string
		require.NoError(t, d.getReader().QueryRow("SELECT source_version FROM sessions WHERE id='session'").Scan(&version))
		require.Equal(t, "new-source-version", version)
		// This metadata is deliberately excluded from transcript equality but
		// remains part of the persisted message row and must still be updated.
		write.Messages[0].SourceType = "updated-source-type"
		_, err = d.WriteSessionBatchContext(context.Background(), []SessionBatchWrite{write})
		require.NoError(t, err)
		messages, err := d.GetAllMessages(context.Background(), "session")
		require.NoError(t, err)
		require.Equal(t, "updated-source-type", messages[0].SourceType)
	}
}

func BenchmarkBatchReplacementIdenticalMessages(b *testing.B) {
	d := testDB(b)
	write := messageCountWrite("session", 2000)
	for i := range write.Messages {
		write.Messages[i].Content += strings.Repeat(" representative archived transcript", 64)
		write.Messages[i].ContentLength = len(write.Messages[i].Content)
	}
	_, err := d.WriteSessionBatchContext(context.Background(), []SessionBatchWrite{write})
	require.NoError(b, err)
	b.ResetTimer()
	for range b.N {
		_, err := d.WriteSessionBatchContext(context.Background(), []SessionBatchWrite{write})
		require.NoError(b, err)
	}
}
