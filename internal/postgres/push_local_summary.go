package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.kenn.io/agentsview/internal/db"
)

// Keep only a digest, not the tool payload strings in the preparation maps.
// The summaries belong to one push and are never persisted or shared by Sync.
type localPushMessageSummary struct {
	digest, revision, modified string
	count                      int
}

type localPushMessageSummaries struct {
	archiveID, databaseID string
	sessions              map[string]localPushMessageSummary
}

func (s *localPushMessageSummaries) compare(
	ctx context.Context, local *db.DB, sessionID string, count int,
	pg *pushMessageComparison,
) (equal, used bool, err error) {
	if s == nil || pg == nil {
		return false, false, nil
	}
	summary, ok := s.sessions[sessionID]
	if !ok || summary.revision == "" || summary.count != count {
		return false, false, nil
	}
	// Normal transcript writers advance the revision and local_modified_at.
	// Recheck them and both archive identities rather than treating a session
	// ID (or a metadata-only rename) as proof that preparation is still current.
	var revision, modified, archiveID, databaseID string
	var currentCount int
	err = local.Reader().QueryRowContext(ctx, `
		SELECT COALESCE(transcript_revision,''), COALESCE(local_modified_at,''), message_count,
			COALESCE((SELECT value FROM archive_metadata WHERE key='archive_id'),''),
			COALESCE((SELECT value FROM archive_metadata WHERE key='database_id'),'')
		FROM sessions WHERE id=?`, sessionID,
	).Scan(&revision, &modified, &currentCount, &archiveID, &databaseID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("checking prepared local summary: %w", err)
	}
	if revision != summary.revision || modified != summary.modified || currentCount != count ||
		archiveID != s.archiveID || databaseID != s.databaseID {
		return false, false, nil
	}
	agg, tools := pg.MessageAggregates[sessionID], pg.ToolCallAggregates[sessionID]
	if agg.Count != count || count == 0 {
		return false, true, nil
	}
	digest, err := hashLocalDependencyPayload(pushLocalMessageFingerprint{
		Sum: agg.Sum, Max: agg.Max, Min: agg.Min,
		ContentHashFP: pg.MessageContentHash[sessionID], RoleTimeFP: pg.MessageRoleTime[sessionID],
		FlagsFP: pg.MessageFlags[sessionID], SystemFP: pg.MessageSystemOrdinals[sessionID],
		TokenFP: pg.MessageTokenFingerprint[sessionID], ToolCallCount: tools.Count, ToolCallSum: tools.Sum,
		ToolCallFP: pg.ToolCallFingerprint[sessionID], ToolResultFP: pg.ToolResultFingerprint[sessionID],
		UsageEventFP: pg.UsageEventFingerprint[sessionID],
	}, nil, nil)
	return digest == summary.digest, true, err
}
