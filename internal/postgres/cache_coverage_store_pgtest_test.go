//go:build pgtest

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cacheStoreTestDB(t *testing.T, schema string, sessions int) *sql.DB {
	t.Helper()
	url := testPGURL(t)
	cleanNamedPGSchema(t, url, schema)
	t.Cleanup(func() { cleanNamedPGSchema(t, url, schema) })
	pg, err := Open(url, schema, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Close() })
	require.NoError(t, EnsureSchema(context.Background(), pg, schema))
	_, err = pg.Exec(`INSERT INTO sessions(id,machine,project,agent,message_count,source_archive_id,transcript_revision,started_at)
        SELECT 'session-'||lpad(n::text,5,'0'),'machine','project','codex',1,'archive','revision','2026-01-01'::timestamptz
        FROM generate_series(1,$1) n;
        `, sessions)
	require.NoError(t, err)
	_, err = pg.Exec(`INSERT INTO messages(session_id,ordinal,role,content,content_length)
        SELECT id,0,'user','before',6 FROM sessions`)
	require.NoError(t, err)
	return pg
}

func TestStoredCoverageSnapshotAndIncrementalInvalidation(t *testing.T) {
	ctx := context.Background()
	pg := cacheStoreTestDB(t, "agentsview_stored_coverage", 2)
	first, err := RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	require.Equal(t, 2, first.Verified)
	raw, err := ReadCacheCoverage(ctx, pg, "", nil)
	require.NoError(t, err)
	stored, err := ReadStoredCacheCoverage(ctx, pg, "", nil)
	require.NoError(t, err)
	require.Equal(t, raw.Sessions, stored.Sessions)
	holder, err := pg.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer holder.Rollback()
	var snapshot string
	require.NoError(t, holder.QueryRow("SELECT pg_export_snapshot()").Scan(&snapshot))
	// A direct equal-length edit without a session revision change must clear
	// coverage. Rolling it back must preserve the old check.
	mutation, err := pg.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = mutation.Exec("UPDATE messages SET content='edited' WHERE session_id='session-00001'")
	require.NoError(t, err)
	require.NoError(t, mutation.Rollback())
	unchanged, err := RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	assert.Zero(t, unchanged.Checked)
	_, err = pg.Exec("UPDATE messages SET content='edited' WHERE session_id='session-00001'")
	require.NoError(t, err)
	current, err := ReadStoredCacheCoverage(ctx, pg, "", nil)
	require.NoError(t, err)
	assert.NotContains(t, current.Sessions, "session-00001")
	oldSnapshot, err := ReadStoredCacheCoverage(ctx, pg, snapshot, nil)
	require.NoError(t, err)
	assert.Equal(t, stored.Sessions, oldSnapshot.Sessions)
	delta, err := RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 1, delta.Checked)
	current, err = ReadStoredCacheCoverage(ctx, pg, "", nil)
	require.NoError(t, err)
	assert.NotEqual(t, stored.Sessions["session-00001"].Digest, current.Sessions["session-00001"].Digest)
	assert.Equal(t, stored.Sessions["session-00002"], current.Sessions["session-00002"])
	for _, table := range cacheCoverageSourceTables {
		column := "session_id"
		if table == "sessions" {
			column = "id"
		}
		// Empty statements must not invalidate unrelated sessions.
		_, err = pg.Exec("DELETE FROM " + table + " WHERE " + column + "='absent'")
		require.NoError(t, err)
	}
	unchanged, err = RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	assert.Zero(t, unchanged.Checked)
}

func TestStoredCoverageResumeAfterInterruptedBatch(t *testing.T) {
	ctx := context.Background()
	pg := cacheStoreTestDB(t, "agentsview_coverage_resume", 130)
	require.NoError(t, EnsureCacheCoverageStore(ctx, pg))
	// Stop the second installation at the database, after the first batch
	// committed. Cancel the real running refresh, not a fabricated receipt.
	blocker, err := pg.Conn(ctx)
	require.NoError(t, err)
	defer blocker.Close()
	_, err = blocker.ExecContext(ctx, "SELECT pg_advisory_lock(734923)")
	require.NoError(t, err)
	defer blocker.ExecContext(ctx, "SELECT pg_advisory_unlock(734923)")
	_, err = pg.Exec(`CREATE FUNCTION pause_coverage() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.session_id='session-00065' AND NEW.copy IS NOT NULL THEN
          PERFORM pg_advisory_xact_lock(734923); END IF; RETURN NEW; END; $$;
        CREATE TRIGGER pause_coverage BEFORE UPDATE ON cache_coverage_state_v1
        FOR EACH ROW EXECUTE FUNCTION pause_coverage()`)
	require.NoError(t, err)
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := RefreshCacheCoverage(work, pg, time.Minute); done <- err }()
	require.Eventually(t, func() bool {
		var checked int
		err := pg.QueryRow("SELECT count(*) FROM cache_coverage_state_v1 WHERE copy IS NOT NULL").Scan(&checked)
		return err == nil && checked == 64
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		var waiting bool
		err := pg.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=734923 AND NOT granted)").Scan(&waiting)
		return err == nil && waiting
	}, 10*time.Second, 10*time.Millisecond)
	cancel()
	require.Error(t, <-done)
	require.Eventually(t, func() bool {
		var waiting bool
		err := pg.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=734923 AND NOT granted)").Scan(&waiting)
		return err == nil && !waiting
	}, 10*time.Second, 10*time.Millisecond)
	_, err = blocker.ExecContext(ctx, "SELECT pg_advisory_unlock(734923)")
	require.NoError(t, err)
	_, err = pg.Exec("DROP TRIGGER pause_coverage ON cache_coverage_state_v1")
	require.NoError(t, err)
	resumed, err := RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 66, resumed.Checked)
	assert.Zero(t, resumed.Remaining)
	unchanged, err := RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	assert.Zero(t, unchanged.Checked)
	assert.Zero(t, unchanged.Remaining)
}

func TestStoredCoverageRejectsMutationDuringFingerprint(t *testing.T) {
	ctx := context.Background()
	pg := cacheStoreTestDB(t, "agentsview_coverage_race", 1)
	require.NoError(t, EnsureCacheCoverageStore(ctx, pg))
	writer, err := pg.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer writer.Rollback()
	_, err = writer.Exec("LOCK TABLE messages IN ACCESS EXCLUSIVE MODE; UPDATE messages SET content='edited'")
	require.NoError(t, err)
	type outcome struct {
		result *CacheCoverageRefresh
		err    error
	}
	done := make(chan outcome, 1)
	go func() { r, err := RefreshCacheCoverage(ctx, pg, time.Minute); done <- outcome{r, err} }()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pg.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='messages'::regclass AND NOT granted AND mode='AccessShareLock')").Scan(&waiting)
		return err == nil && waiting
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, writer.Commit())
	got := <-done
	require.NoError(t, got.err)
	assert.Zero(t, got.result.Checked)
	assert.Equal(t, 1, got.result.Changed)
	coverage, err := ReadStoredCacheCoverage(ctx, pg, "", nil)
	require.NoError(t, err)
	assert.Empty(t, coverage.Sessions)
	resumed, err := RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 1, resumed.Verified)
}

func TestStoredCoverageLargeUnchangedArchiveAndTriggerRepair(t *testing.T) {
	ctx := context.Background()
	pg := cacheStoreTestDB(t, "agentsview_coverage_scale", 2048)
	first, err := RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	require.Equal(t, 2048, first.Verified)
	unchanged, err := RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	assert.Zero(t, unchanged.Checked)
	_, err = pg.Exec("UPDATE messages SET content='edited' WHERE session_id='session-00001'")
	require.NoError(t, err)
	delta, err := RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 1, delta.Checked)
	_, err = pg.Exec("ALTER TABLE messages DISABLE TRIGGER cache_coverage_update_v1; UPDATE messages SET content='missed' WHERE session_id='session-00002'")
	require.NoError(t, err)
	_, err = ReadStoredCacheCoverage(ctx, pg, "", nil)
	require.Error(t, err)
	require.NoError(t, EnsureCacheCoverageStore(ctx, pg))
	coverage, err := ReadStoredCacheCoverage(ctx, pg, "", nil)
	require.NoError(t, err)
	assert.Empty(t, coverage.Sessions)
	_, err = RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	_, err = pg.Exec("TRUNCATE messages CASCADE")
	require.NoError(t, err)
	coverage, err = ReadStoredCacheCoverage(ctx, pg, "", nil)
	require.NoError(t, err)
	assert.Empty(t, coverage.Sessions)
	checked, err := RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 2048, checked.Checked)
	assert.Zero(t, checked.Verified)
	unchanged, err = RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	assert.Zero(t, unchanged.Checked, "inconsistent source counts are protected without repeated full work")
}

func TestStoredCoverageChildWritesAndRestrictedReader(t *testing.T) {
	ctx := context.Background()
	const schema = "agentsview_coverage_children"
	pg := cacheStoreTestDB(t, schema, 2)
	_, err := RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	// Every retained dependency participates, even with unchanged timestamps,
	// lengths, message counts and transcript revision.
	for _, mutation := range []string{
		`INSERT INTO tool_calls(session_id,message_ordinal,tool_name,category,result_content,result_content_length) VALUES ('session-00001',0,'Read','file','before',6)`,
		`UPDATE tool_calls SET result_content='edited'`,
		`UPDATE tool_calls SET session_id='session-00002'`,
		`DELETE FROM tool_calls`,
		`INSERT INTO tool_result_events(session_id,tool_call_message_ordinal,source,status,content) VALUES ('session-00001',0,'tool','ok','before')`,
		`UPDATE tool_result_events SET content='edited'`,
		`DELETE FROM tool_result_events`,
		`INSERT INTO usage_events(session_id,source,model,input_tokens) VALUES ('session-00001','message','model',1)`,
		`UPDATE usage_events SET input_tokens=2`,
		`DELETE FROM usage_events`,
	} {
		_, err = pg.Exec(mutation)
		require.NoError(t, err)
		coverage, err := ReadStoredCacheCoverage(ctx, pg, "", nil)
		require.NoError(t, err)
		assert.Less(t, len(coverage.Sessions), 2, mutation)
		_, err = RefreshCacheCoverage(ctx, pg, time.Minute)
		require.NoError(t, err)
		raw, err := ReadCacheCoverage(ctx, pg, "", nil)
		require.NoError(t, err)
		stored, err := ReadStoredCacheCoverage(ctx, pg, "", nil)
		require.NoError(t, err)
		assert.Equal(t, raw.Sessions, stored.Sessions, mutation)
	}
	const role = "cache_coverage_fixture_reader"
	_, err = pg.Exec("CREATE ROLE " + role + " NOLOGIN; GRANT USAGE ON SCHEMA " + schema + " TO " + role + "; GRANT SELECT ON sessions,cache_coverage_state_v1 TO " + role + "; GRANT UPDATE ON messages TO " + role)
	require.NoError(t, err)
	defer func() {
		_, err := pg.Exec("DROP OWNED BY " + role + "; DROP ROLE " + role)
		require.NoError(t, err)
	}()
	url, err := appendConnParams(testPGURL(t), map[string]string{"role": role})
	require.NoError(t, err)
	reader, err := Open(url, schema, true)
	require.NoError(t, err)
	defer reader.Close()
	_, err = reader.Exec("SELECT content FROM messages")
	require.Error(t, err)
	// Stored coverage succeeds without permission to read any transcript body.
	coverage, err := ReadStoredCacheCoverage(ctx, reader, "", nil)
	require.NoError(t, err)
	assert.Len(t, coverage.Sessions, 2)
	_, err = reader.Exec("UPDATE cache_coverage_state_v1 SET copy='{}'")
	require.Error(t, err)
	// UPDATE without a predicate needs no SELECT privilege on messages; its
	// protected invalidation trigger must still run successfully.
	_, err = reader.Exec("UPDATE messages SET content='writer'")
	require.NoError(t, err)
	coverage, err = ReadStoredCacheCoverage(ctx, reader, "", nil)
	require.NoError(t, err)
	assert.Empty(t, coverage.Sessions)
	_, err = RefreshCacheCoverage(ctx, pg, time.Minute)
	require.NoError(t, err)
	_, err = pg.Exec("DELETE FROM sessions WHERE id='session-00001'")
	require.NoError(t, err)
	coverage, err = ReadStoredCacheCoverage(ctx, pg, "", nil)
	require.NoError(t, err)
	assert.NotContains(t, coverage.Sessions, "session-00001")
}
