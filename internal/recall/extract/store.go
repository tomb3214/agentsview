package extract

import (
	"context"
	"time"

	"go.kenn.io/agentsview/internal/db"
)

// Store is the transcript and checkpoint boundary used by the extractor.
// Implementations must commit a unit's evidence and cursor atomically, rejecting
// a changed source snapshot or progress cursor before publishing any output.
type Store interface {
	GetSession(context.Context, string) (*db.Session, error)
	GetSessionFull(context.Context, string) (*db.Session, error)
	GetAllMessages(context.Context, string) ([]db.Message, error)
	EnsureExtractGeneration(context.Context, db.ExtractGeneration) (db.ExtractGeneration, error)
	ExtractGenerations(context.Context) ([]db.ExtractGeneration, error)
	ActivateExtractGeneration(context.Context, string, []string, time.Time) error
	RetireExtractGeneration(context.Context, string, bool) error
	ExtractCandidates(context.Context, db.ExtractCandidateQuery) ([]string, error)
	ExtractProgress(context.Context, string, string) (db.ExtractProgress, bool, error)
	UpsertExtractProgress(context.Context, db.ExtractProgressUpsert) (db.ExtractProgress, error)
	MarkExtractProgressFailed(context.Context, db.ExtractFailure) error
	RefreshExtractedSessionCoverage(context.Context, db.ExtractCoverageRefresh) (db.ExtractProgress, error)
	CommitExtractedUnit(context.Context, db.ExtractUnitCommit) (int, error)
	ReconcileIneligibleExtractSessions(context.Context, time.Time) (int, int, error)
	DiscardExtractedSessionOutput(context.Context, db.ExtractFailure) error
	ExtractProgressStats(context.Context, string) (db.ExtractProgressStats, error)
}

var _ Store = (*db.DB)(nil)
