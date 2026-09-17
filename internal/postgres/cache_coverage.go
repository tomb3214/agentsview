package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"go.kenn.io/agentsview/internal/db"
)

const CacheCoverageFormat = "agentsview-cache-coverage-v1"

// CacheCopy records an exact normalized transcript in one PostgreSQL snapshot.
// It contains no transcript text. Backup tooling must capture it in the SAME
// exported snapshot as pg_dump, then verify the encrypted backup's readback.
type CacheCopy struct {
	ArchiveID    string `json:"archive_id"`
	Machine      string `json:"machine"`
	Revision     string `json:"revision"`
	MessageCount int    `json:"message_count"`
	Digest       string `json:"digest"`
}

type CacheCoverage struct {
	Format     string               `json:"format"`
	CapturedAt string               `json:"captured_at"`
	BackupID   string               `json:"backup_id"`
	Sessions   map[string]CacheCopy `json:"sessions"`
}

var exportedCacheSnapshot = regexp.MustCompile(`^[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9]+$`)

func cacheComparisonDigest(id string, p *pushMessageComparison) (string, error) {
	a, t := p.MessageAggregates[id], p.ToolCallAggregates[id]
	return hashLocalDependencyPayload(pushLocalMessageFingerprint{
		Sum: a.Sum, Max: a.Max, Min: a.Min, ContentHashFP: p.MessageContentHash[id],
		RoleTimeFP: p.MessageRoleTime[id], FlagsFP: p.MessageFlags[id], SystemFP: p.MessageSystemOrdinals[id],
		TokenFP: p.MessageTokenFingerprint[id], ToolCallCount: t.Count, ToolCallSum: t.Sum,
		ToolCallFP: p.ToolCallFingerprint[id], ToolResultFP: p.ToolResultFingerprint[id], UsageEventFP: p.UsageEventFingerprint[id],
	}, nil, nil)
}

// ReadCacheCoverage is read-only. A nil selection covers the complete archive;
// an empty, non-nil selection covers nothing. Queries are bounded per batch.
func ReadCacheCoverage(ctx context.Context, pg *sql.DB, snapshot string, selection []string) (*CacheCoverage, error) {
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
	coverage := &CacheCoverage{Format: CacheCoverageFormat, CapturedAt: time.Now().UTC().Format(time.RFC3339), Sessions: map[string]CacheCopy{}}
	query := `SELECT id, coalesce(source_archive_id,''),machine,coalesce(transcript_revision,''),message_count
        FROM sessions WHERE deleted_at IS NULL AND source_deleted_at IS NULL AND NOT is_truncated AND message_count>0`
	args := []any{}
	if selection != nil {
		query += " AND id=ANY($1)"
		args = append(args, selection)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		var c CacheCopy
		if err = rows.Scan(&id, &c.ArchiveID, &c.Machine, &c.Revision, &c.MessageCount); err != nil {
			rows.Close()
			return nil, err
		}
		coverage.Sessions[id] = c
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for start := 0; start < len(ids); start += 64 {
		batch := ids[start:min(start+64, len(ids))]
		cmp, err := readPushSessionMessageComparisons(ctx, tx, batch)
		if err != nil {
			return nil, err
		}
		for _, id := range batch {
			c := coverage.Sessions[id]
			if cmp.MessageAggregates[id].Count != c.MessageCount || c.ArchiveID == "" {
				delete(coverage.Sessions, id)
				continue
			}
			c.Digest, err = cacheComparisonDigest(id, cmp)
			if err != nil {
				return nil, err
			}
			coverage.Sessions[id] = c
		}
	}
	return coverage, tx.Commit()
}

// VerifyCachedSession reuses the publication path's complete normalized
// message/tool/result/usage fingerprint, not timestamps or row counts alone.
// The offline caller holds archive write ownership throughout this operation.
func VerifyCachedSession(ctx context.Context, local *db.DB, sess db.Session, current, backup CacheCopy) (*db.CacheEviction, error) {
	archive, err := local.GetArchiveID(ctx)
	if err != nil {
		return nil, err
	}
	if current != backup || backup.ArchiveID != archive || backup.Machine != sess.Machine || backup.Revision != stringValue(sess.TranscriptRevision) || backup.MessageCount != sess.MessageCount || backup.Digest == "" {
		return nil, errors.New("session has no matching central and backup copy")
	}
	fp, err := localPushMessageFingerprint(local, sess.ID, "", false)
	if err != nil {
		return nil, err
	}
	digest, err := hashLocalDependencyPayload(fp, nil, nil)
	if err != nil {
		return nil, err
	}
	if digest != backup.Digest {
		return nil, errors.New("local transcript differs from verified copies")
	}
	if stringValue(sess.FileHash) == "" {
		return nil, fmt.Errorf("session has no source fingerprint")
	}
	return &db.CacheEviction{SessionID: sess.ID, FileHash: stringValue(sess.FileHash), Revision: stringValue(sess.TranscriptRevision), Modified: stringValue(sess.LocalModifiedAt), MessageCount: sess.MessageCount, ContentDigest: digest}, nil
}
