package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// CacheEviction is local bookkeeping, never a deletion or a mirror tombstone.
// The caller must verify ContentDigest against both the live central archive
// and a read-back-verified backup before calling EvictCachedSession. Direct
// maintenance holds the archive write-owner lock across verification and trim.
type CacheEviction struct {
	SessionID     string `json:"session_id"`
	FileHash      string `json:"file_hash"`
	Revision      string `json:"revision"`
	Modified      string `json:"modified"`
	MessageCount  int    `json:"message_count"`
	ContentDigest string `json:"content_digest"`
	BackupID      string `json:"backup_id"`
}

func (d *DB) CacheEviction(ctx context.Context, id string) (*CacheEviction, error) {
	var e CacheEviction
	err := d.getReader().QueryRowContext(ctx, `SELECT session_id, file_hash,
        revision, modified, message_count, content_digest, backup_id
        FROM local_session_cache_evictions WHERE session_id=?`, id).Scan(
		&e.SessionID, &e.FileHash, &e.Revision, &e.Modified, &e.MessageCount,
		&e.ContentDigest, &e.BackupID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &e, err
}

func (d *DB) CacheEvictedSessionIDs(ctx context.Context) (map[string]struct{}, error) {
	rows, err := d.getReader().QueryContext(ctx, `SELECT session_id FROM local_session_cache_evictions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids[id] = struct{}{}
	}
	return ids, rows.Err()
}

// CacheSourceUnchanged survives parser upgrades and forced resyncs. Only a
// nonempty content fingerprint is authority to skip an evicted source. All
// sessions sharing this path must be evicted at exactly this source hash.
func (d *DB) CacheSourceUnchanged(ctx context.Context, agent, path, hash string) (bool, error) {
	if hash == "" {
		return false, nil
	}
	var total, matching int
	err := d.getReader().QueryRowContext(ctx, `SELECT count(*),
        coalesce(sum(CASE WHEN e.file_hash=? THEN 1 ELSE 0 END),0)
        FROM sessions s LEFT JOIN local_session_cache_evictions e ON e.session_id=s.id
        WHERE s.agent=? AND s.file_path=? AND s.deleted_at IS NULL`, hash, agent, path).Scan(&total, &matching)
	return total > 0 && total == matching, err
}

// EvictCachedSession removes only the rebuildable local transcript. Session
// identity, source freshness, revision, metadata, usage events and Recall stay
// intact. Pins are protected because their local message foreign key cascades.
func (d *DB) EvictCachedSession(ctx context.Context, e CacheEviction) error {
	if err := d.requireWritable(); err != nil {
		return err
	}
	if e.SessionID == "" || e.FileHash == "" || e.Revision == "" || e.MessageCount < 1 || len(e.ContentDigest) != 64 || e.BackupID == "" {
		return errors.New("complete verified cache eviction proof is required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	tx, err := d.getWriter().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var matched int
	err = tx.QueryRowContext(ctx, `SELECT count(*) FROM sessions s WHERE id=?
        AND file_hash=? AND transcript_revision=? AND coalesce(local_modified_at,'')=?
        AND deleted_at IS NULL AND is_truncated=0
        AND (SELECT count(*) FROM messages WHERE session_id=s.id)=?
        AND NOT EXISTS(SELECT 1 FROM pinned_messages WHERE session_id=s.id)
        AND NOT EXISTS(SELECT 1 FROM local_session_cache_evictions WHERE session_id=s.id)`,
		e.SessionID, e.FileHash, e.Revision, e.Modified, e.MessageCount).Scan(&matched)
	if err != nil {
		return err
	}
	if matched != 1 {
		return errors.New("cache eviction source changed or is protected")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO local_session_cache_evictions
        (session_id,file_hash,revision,modified,message_count,content_digest,backup_id)
        VALUES(?,?,?,?,?,?,?)`, e.SessionID, e.FileHash, e.Revision, e.Modified, e.MessageCount, e.ContentDigest, e.BackupID); err != nil {
		return err
	}
	if err = deleteSessionMessagesTx(contextTransaction{ctx: ctx, tx: tx}, e.SessionID); err != nil {
		return err
	}
	// Message triggers may enqueue exports. An empty local cache is never an
	// artifact update, including after a crash or a later metadata-only rename.
	if _, err = tx.ExecContext(ctx, `DELETE FROM artifact_export_queue WHERE session_id=?`, e.SessionID); err != nil {
		return err
	}
	return tx.Commit()
}

// CompleteCacheRestore is called only after full content and usage writes.
// The count check prevents an interrupted or suffix-only write from exposing
// a partial transcript to publication. It is harmless for ordinary sessions.
func (d *DB) CompleteCacheRestore(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.getWriter().ExecContext(ctx, `DELETE FROM local_session_cache_evictions
        WHERE session_id=? AND message_count <= (SELECT count(*) FROM messages WHERE session_id=?)`, id, id)
	return err
}

// CacheUsedBytes includes the main file's live pages: indexes, FTS, tool
// payloads and metadata, rather than estimating disk use from message text.
func (d *DB) CacheUsedBytes(ctx context.Context) (int64, error) {
	var pages, free, size int64
	for _, v := range []struct {
		q string
		n *int64
	}{{"PRAGMA page_count", &pages}, {"PRAGMA freelist_count", &free}, {"PRAGMA page_size", &size}} {
		if err := d.getReader().QueryRowContext(ctx, v.q).Scan(v.n); err != nil {
			return 0, err
		}
	}
	return (pages - free) * size, nil
}

// CompactCache requires exclusive process write ownership. VACUUM preserves
// archive identity and schema and safely leaves the old file on failure.
func (d *DB) CompactCache(ctx context.Context) error {
	if err := d.requireWritable(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.HasFTS() {
		if _, err := d.getWriter().ExecContext(ctx, `INSERT INTO messages_fts(messages_fts) VALUES('optimize')`); err != nil {
			return err
		}
	}
	if _, err := d.getWriter().ExecContext(ctx, "VACUUM"); err != nil {
		return fmt.Errorf("compacting history cache: %w", err)
	}
	_, err := d.getWriter().ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

// CopyCacheEvictionsFrom runs before source discovery during a full rebuild.
// Receipt rows and the small identity/freshness rows move together; no message
// data is reintroduced. Failure aborts the rebuild rather than dropping proof.
func (d *DB) CopyCacheEvictionsFrom(sourcePath string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	ctx := context.Background()
	conn, err := d.getWriter().Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "ATTACH DATABASE ? AS old_db", sourcePath); err != nil {
		return err
	}
	defer func() { _, _ = conn.ExecContext(ctx, "DETACH DATABASE old_db") }()
	var exists int
	if err = conn.QueryRowContext(ctx, `SELECT count(*) FROM old_db.sqlite_master WHERE name='local_session_cache_evictions'`).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return nil
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	cols := orphanSessionCols(ctx, tx)
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO sessions ("+cols+") SELECT "+cols+` FROM old_db.sessions
        WHERE id IN(SELECT session_id FROM old_db.local_session_cache_evictions)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO local_session_cache_evictions SELECT * FROM old_db.local_session_cache_evictions`); err != nil {
		return err
	}
	return tx.Commit()
}
