package postgres

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"time"
)

// This opt-in, PostgreSQL-only store belongs to the backup administrator.
// Device writers need SELECT, never write access. Statement triggers invalidate
// checks in the same transaction as every source mutation, including direct SQL.
// Random change tokens remain distinct across dump/restore and concurrent writes;
// they are not timestamps or content heuristics.
const cacheCoverageStoreDDL = `
CREATE TABLE IF NOT EXISTS __SCHEMA__.cache_coverage_state_v1 (
    session_id TEXT PRIMARY KEY,
    source_version UUID NOT NULL,
    format TEXT NOT NULL DEFAULT '',
    copy JSONB
);
REVOKE ALL ON __SCHEMA__.cache_coverage_state_v1 FROM PUBLIC;
CREATE OR REPLACE FUNCTION __SCHEMA__.invalidate_cache_coverage_v1()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE
    source_column TEXT;
    changed_rows TEXT;
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        EXECUTE format('UPDATE %I.cache_coverage_state_v1 SET source_version=gen_random_uuid(), copy=NULL', TG_TABLE_SCHEMA);
        RETURN NULL;
    END IF;
    source_column := CASE WHEN TG_TABLE_NAME='sessions' THEN 'id' ELSE 'session_id' END;
    IF TG_OP = 'INSERT' THEN
        changed_rows := format('SELECT %I AS id FROM cache_new_rows', source_column);
    ELSIF TG_OP = 'DELETE' THEN
        changed_rows := format('SELECT %I AS id FROM cache_old_rows', source_column);
    ELSE
        changed_rows := format('SELECT %I AS id FROM cache_old_rows UNION SELECT %I AS id FROM cache_new_rows', source_column, source_column);
    END IF;
    EXECUTE format('INSERT INTO %I.cache_coverage_state_v1 AS state(session_id,source_version,copy)
        SELECT id,gen_random_uuid(),NULL FROM (%s) changed WHERE id IS NOT NULL GROUP BY id ORDER BY id
        ON CONFLICT(session_id) DO UPDATE SET source_version=EXCLUDED.source_version,copy=NULL
        ', TG_TABLE_SCHEMA, changed_rows);
    RETURN NULL;
END;
$function$;
REVOKE ALL ON FUNCTION __SCHEMA__.invalidate_cache_coverage_v1() FROM PUBLIC;
`

var cacheCoverageSourceTables = []string{"sessions", "messages", "tool_calls", "tool_result_events", "usage_events"}

func cacheCoverageStoreReady(ctx context.Context, tx *sql.Tx) (bool, error) {
	var ready bool
	err := tx.QueryRowContext(ctx, `SELECT to_regclass(current_schema() || '.cache_coverage_state_v1') IS NOT NULL
        AND (SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
             JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE n.nspname=current_schema() AND c.relname=ANY($1)
             AND t.tgname=ANY($2) AND t.tgenabled IN ('O','A'))=20`,
		cacheCoverageSourceTables, []string{"cache_coverage_insert_v1", "cache_coverage_update_v1", "cache_coverage_delete_v1", "cache_coverage_truncate_v1"}).Scan(&ready)
	return ready, err
}

// EnsureCacheCoverageStore installs invalidation atomically with its initial
// state. It must run as a schema/table owner, not a device ingest role. Repairing
// a missing/disabled trigger discards old checks because changes may have been
// missed. Ordinary recurrence does no DDL and reads only catalog metadata.
func EnsureCacheCoverageStore(ctx context.Context, pg *sql.DB) error {
	tx, err := pg.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	ready, err := cacheCoverageStoreReady(ctx, tx)
	if err != nil || ready {
		return err
	}
	var schema string
	if err = tx.QueryRowContext(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		return err
	}
	quoted, err := quoteIdentifier(schema)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "SET LOCAL lock_timeout='5s'"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "LOCK TABLE sessions,messages,tool_calls,tool_result_events,usage_events IN SHARE ROW EXCLUSIVE MODE"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, strings.ReplaceAll(cacheCoverageStoreDDL, "__SCHEMA__", quoted)); err != nil {
		return err
	}
	for _, table := range cacheCoverageSourceTables {
		for _, event := range []struct{ name, transition string }{
			{"INSERT", "REFERENCING NEW TABLE AS cache_new_rows"},
			{"UPDATE", "REFERENCING OLD TABLE AS cache_old_rows NEW TABLE AS cache_new_rows"},
			{"DELETE", "REFERENCING OLD TABLE AS cache_old_rows"},
			{"TRUNCATE", ""},
		} {
			name := "cache_coverage_" + strings.ToLower(event.name) + "_v1"
			ddl := fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON %s.%s;
                CREATE TRIGGER %s AFTER %s ON %s.%s %s FOR EACH STATEMENT
                EXECUTE FUNCTION %s.invalidate_cache_coverage_v1()`, name, quoted, table,
				name, event.name, quoted, table, event.transition, quoted)
			if _, err = tx.ExecContext(ctx, ddl); err != nil {
				return err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE cache_coverage_state_v1 SET source_version=gen_random_uuid(),copy=NULL;
        INSERT INTO cache_coverage_state_v1(session_id,source_version)
        SELECT id,gen_random_uuid() FROM sessions ON CONFLICT(session_id) DO NOTHING`); err != nil {
		return err
	}
	return tx.Commit()
}

type CacheCoverageRefresh struct {
	Checked   int  `json:"checked"`
	Verified  int  `json:"verified"`
	Changed   int  `json:"changed_during_check"`
	Remaining int  `json:"remaining"`
	TimedOut  bool `json:"time_budget_reached"`
}

// RefreshCacheCoverage checkpoints completed batches. The snapshot contains
// both source versions and full content; installing a digest uses compare-and-
// set against that version after the read. Any intervening writer invalidates
// it transactionally, so neither a race nor an interrupted run can certify stale
// data. Inconsistent sessions get an empty check until the next source change.
func RefreshCacheCoverage(ctx context.Context, pg *sql.DB, budget time.Duration) (*CacheCoverageRefresh, error) {
	if budget <= 0 {
		return nil, errors.New("coverage refresh time budget must be positive")
	}
	if err := EnsureCacheCoverageStore(ctx, pg); err != nil {
		return nil, err
	}
	workCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	rows, err := pg.QueryContext(workCtx, `SELECT s.id,s.message_count FROM sessions s
        JOIN cache_coverage_state_v1 c ON c.session_id=s.id
        WHERE (c.copy IS NULL OR c.format<>$1) AND s.deleted_at IS NULL AND s.source_deleted_at IS NULL
        AND NOT s.is_truncated AND s.message_count>0 AND coalesce(s.source_archive_id,'')<>''
        ORDER BY coalesce(s.ended_at,s.started_at,s.created_at),s.id`, CacheCoverageFormat)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		id    string
		count int
	}
	var pending []candidate
	for rows.Next() {
		var c candidate
		if err = rows.Scan(&c.id, &c.count); err != nil {
			rows.Close()
			return nil, err
		}
		pending = append(pending, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	result := &CacheCoverageRefresh{Remaining: len(pending)}
	for start := 0; start < len(pending); {
		var ids []string
		messages := 0
		for start < len(pending) && len(ids) < 64 {
			c := pending[start]
			if len(ids) > 0 && messages+c.count > 8192 {
				break
			}
			ids = append(ids, c.id)
			messages += c.count
			start++
		}
		checked, verified, err := refreshCacheCoverageBatch(workCtx, pg, ids)
		if err != nil {
			if workCtx.Err() != nil && ctx.Err() == nil {
				result.TimedOut = true
				return result, nil
			}
			return result, err
		}
		result.Checked += checked
		result.Verified += verified
		result.Changed += len(ids) - checked
		result.Remaining -= checked
	}
	return result, nil
}

func refreshCacheCoverageBatch(ctx context.Context, pg *sql.DB, ids []string) (int, int, error) {
	tx, err := pg.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, "SELECT session_id,source_version FROM cache_coverage_state_v1 WHERE session_id=ANY($1)", ids)
	if err != nil {
		return 0, 0, err
	}
	type checkedCopy struct {
		ID      string    `json:"id"`
		Version string    `json:"version"`
		Copy    CacheCopy `json:"copy"`
	}
	var checks []checkedCopy
	for rows.Next() {
		var c checkedCopy
		if err = rows.Scan(&c.ID, &c.Version); err != nil {
			rows.Close()
			return 0, 0, err
		}
		checks = append(checks, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, 0, err
	}
	coverage, err := readCacheCoverageTx(ctx, tx, ids)
	if err != nil {
		return 0, 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, 0, err
	}
	for i := range checks {
		checks[i].Copy = coverage.Sessions[checks[i].ID]
	}
	data, err := json.Marshal(checks)
	if err != nil {
		return 0, 0, err
	}
	var checked, verified int
	err = pg.QueryRowContext(ctx, `WITH installed AS (
        UPDATE cache_coverage_state_v1 c SET copy=x.copy,format=$2
        FROM jsonb_to_recordset($1::jsonb) AS x(id text,version uuid,copy jsonb)
        WHERE c.session_id=x.id AND c.source_version=x.version RETURNING c.copy)
        SELECT count(*),count(*) FILTER(WHERE copy->>'digest'<>'') FROM installed`, string(data), CacheCoverageFormat).Scan(&checked, &verified)
	return checked, verified, err
}

// ReadStoredCacheCoverage never reads transcript bodies. MVCC binds the source
// rows and their invalidations to the same snapshot, including an exported dump
// snapshot. Partial coverage is useful; an unchecked session remains protected.
func ReadStoredCacheCoverage(ctx context.Context, pg *sql.DB, snapshot string, selection []string) (*CacheCoverage, error) {
	if snapshot != "" && !exportedCacheSnapshot.MatchString(snapshot) {
		return nil, errors.New("invalid exported snapshot")
	}
	tx, err := pg.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if snapshot != "" {
		if _, err = tx.ExecContext(ctx, "SET TRANSACTION SNAPSHOT '"+snapshot+"'"); err != nil {
			return nil, err
		}
	}
	if ready, err := cacheCoverageStoreReady(ctx, tx); err != nil || !ready {
		return nil, errors.New("backup administrator must initialize cache coverage invalidation")
	}
	query := `SELECT s.id,c.copy FROM sessions s JOIN cache_coverage_state_v1 c ON c.session_id=s.id
        WHERE c.format=$1 AND c.copy->>'digest'<>'' AND s.deleted_at IS NULL AND s.source_deleted_at IS NULL
        AND NOT s.is_truncated AND s.message_count>0`
	args := []any{CacheCoverageFormat}
	if selection != nil {
		query += " AND s.id=ANY($2)"
		args = append(args, selection)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	coverage := &CacheCoverage{Format: CacheCoverageFormat, CapturedAt: time.Now().UTC().Format(time.RFC3339), Sessions: map[string]CacheCopy{}}
	for rows.Next() {
		var id, data string
		var c CacheCopy
		if err = rows.Scan(&id, &data); err == nil {
			err = json.Unmarshal([]byte(data), &c)
		}
		if err != nil {
			rows.Close()
			return nil, err
		}
		coverage.Sessions[id] = c
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	return coverage, tx.Commit()
}
