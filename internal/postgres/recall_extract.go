package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/recall/extract"
)

// RecallExtractStore scopes extraction to one admitted source machine. It uses
// the caller's existing connection and permissions; it neither creates schema
// nor grants access. Model execution remains the extractor's responsibility.
type RecallExtractStore struct {
	pg      *sql.DB
	machine string
	reader  *Store
}

var _ extract.Store = (*RecallExtractStore)(nil)

func NewRecallExtractStore(pg *sql.DB, machine string) (*RecallExtractStore, error) {
	if pg == nil || strings.TrimSpace(machine) == "" {
		return nil, fmt.Errorf("recall extraction requires a database and source machine")
	}
	return &RecallExtractStore{pg: pg, machine: machine, reader: &Store{pg: pg}}, nil
}

// scanExtractSession adds the central write timestamp to the existing session
// decoder in the same query. A second query could pair old metadata with a new
// stamp and hide a source update from the manager's snapshot bracket.
type extractSessionScanner struct {
	row     interface{ Scan(...any) error }
	updated *time.Time
}

func (s extractSessionScanner) Scan(dest ...any) error {
	return s.row.Scan(append(dest, s.updated)...)
}

func (s *RecallExtractStore) GetSessionFull(ctx context.Context, id string) (*db.Session, error) {
	var updated time.Time
	row := s.pg.QueryRowContext(ctx, "SELECT "+pgSessionCols+`, updated_at
		FROM sessions WHERE id = $1 AND machine = $2`, id, s.machine)
	session, err := scanPGSession(extractSessionScanner{row: row, updated: &updated})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	stamp := updated.UTC().Format(time.RFC3339Nano)
	session.LocalModifiedAt = &stamp
	return &session, nil
}

func (s *RecallExtractStore) GetSession(ctx context.Context, id string) (*db.Session, error) {
	session, err := s.GetSessionFull(ctx, id)
	if session != nil && session.DeletedAt != nil {
		return nil, err
	}
	return session, err
}

func (s *RecallExtractStore) GetAllMessages(ctx context.Context, id string) ([]db.Message, error) {
	session, err := s.GetSessionFull(ctx, id)
	if err != nil || session == nil {
		return nil, err
	}
	return s.reader.GetAllMessages(ctx, id)
}

func scanExtractGeneration(row interface{ Scan(...any) error }) (db.ExtractGeneration, error) {
	var gen db.ExtractGeneration
	var created, updated time.Time
	err := row.Scan(&gen.Fingerprint, &gen.State, &gen.Model, &gen.Segmenter,
		&gen.ParamsJSON, &created, &updated)
	gen.CreatedAt = created.UTC().Format(time.RFC3339Nano)
	gen.UpdatedAt = updated.UTC().Format(time.RFC3339Nano)
	return gen, err
}

const extractGenerationColumns = `fingerprint, state, model, segmenter, params_json, created_at, updated_at`

func (s *RecallExtractStore) EnsureExtractGeneration(ctx context.Context, gen db.ExtractGeneration) (db.ExtractGeneration, error) {
	gen.Fingerprint = strings.TrimSpace(gen.Fingerprint)
	if gen.Fingerprint == "" || strings.TrimSpace(gen.Model) == "" || strings.TrimSpace(gen.Segmenter) == "" {
		return db.ExtractGeneration{}, fmt.Errorf("extract generation requires fingerprint, model and segmenter")
	}
	if gen.ParamsJSON == "" {
		gen.ParamsJSON = "{}"
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return db.ExtractGeneration{}, err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `INSERT INTO recall_extract_generations
		(machine, fingerprint, state, model, segmenter, params_json,coordinated)
		VALUES ($1, $2, 'building', $3, $4, $5,TRUE)
		ON CONFLICT (machine, fingerprint) DO UPDATE SET coordinated=TRUE`,
		s.machine, gen.Fingerprint, gen.Model, gen.Segmenter, gen.ParamsJSON)
	if err != nil {
		return db.ExtractGeneration{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE recall_entries SET publication_owner='central'
		WHERE machine=$1 AND review_state='unreviewed_auto' AND publication_owner<>'central'`, s.machine); err != nil {
		return db.ExtractGeneration{}, err
	}
	if err := tx.Commit(); err != nil {
		return db.ExtractGeneration{}, err
	}
	return scanExtractGeneration(s.pg.QueryRowContext(ctx,
		"SELECT "+extractGenerationColumns+" FROM recall_extract_generations WHERE machine=$1 AND fingerprint=$2",
		s.machine, gen.Fingerprint))
}

func (s *RecallExtractStore) ExtractGenerations(ctx context.Context) ([]db.ExtractGeneration, error) {
	rows, err := s.pg.QueryContext(ctx, "SELECT "+extractGenerationColumns+`
		FROM recall_extract_generations WHERE machine=$1 ORDER BY created_at DESC, fingerprint`, s.machine)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var generations []db.ExtractGeneration
	for rows.Next() {
		gen, err := scanExtractGeneration(rows)
		if err != nil {
			return nil, err
		}
		generations = append(generations, gen)
	}
	return generations, rows.Err()
}

const pgExtractEligible = `s.deleted_at IS NULL AND NOT s.is_automated
	AND s.message_count > 0 AND s.ended_at IS NOT NULL AND s.ended_at <= `

// ExtractCandidates keeps discovery, ready progress and delayed retries as
// separate indexed arms. PG writes use transaction-start timestamps, so time
// watermarks cannot safely exclude a transaction that commits late. Compare
// the exact covered source timestamp instead; no transcripts are loaded here.
func (s *RecallExtractStore) ExtractCandidates(ctx context.Context, q db.ExtractCandidateQuery) ([]string, error) {
	if strings.TrimSpace(q.Fingerprint) == "" {
		return nil, fmt.Errorf("extract candidate query requires a fingerprint")
	}
	pb := paramBuilder{}
	machine, fp := pb.add(s.machine), pb.add(q.Fingerprint)
	eligible := "s.machine=" + machine + " AND " + pgExtractEligible + pb.add(q.QuietCutoff)
	parts := []string{`SELECT s.id, s.ended_at FROM sessions s WHERE ` + eligible + `
		AND NOT EXISTS (SELECT 1 FROM recall_extract_progress p
		WHERE p.machine=` + machine + ` AND p.session_id=s.id AND p.generation_fingerprint=` + fp + `)`}
	queue := `SELECT s.id, s.ended_at FROM recall_extract_progress p
		JOIN sessions s ON s.id=p.session_id AND s.machine=p.machine
		WHERE p.machine=` + machine + ` AND p.generation_fingerprint=` + fp + ` AND ` + eligible
	parts = append(parts, queue+" AND p.state IN ('pending','partial')",
		queue+" AND p.state='failed' AND p.updated_at <= "+pb.add(q.FailedRetryCutoff))
	if q.IncludeDone {
		done := queue + " AND p.state='done' AND s.updated_at IS DISTINCT FROM p.source_modified_at"
		parts = append(parts, done)
	}
	query := "SELECT id FROM (" + strings.Join(parts, " UNION ") + ") candidates ORDER BY ended_at, id"
	if q.Limit > 0 {
		query += " LIMIT " + pb.add(q.Limit)
	}
	rows, err := s.pg.QueryContext(ctx, query, pb.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *RecallExtractStore) ExtractProgress(ctx context.Context, id, fingerprint string) (db.ExtractProgress, bool, error) {
	var p db.ExtractProgress
	var updated time.Time
	err := s.pg.QueryRowContext(ctx, `SELECT session_id, generation_fingerprint,
		unit_cursor, units_total, state, content_digest, last_error, updated_at
		FROM recall_extract_progress WHERE machine=$1 AND session_id=$2 AND generation_fingerprint=$3`,
		s.machine, id, fingerprint).Scan(&p.SessionID, &p.GenerationFingerprint,
		&p.UnitCursor, &p.UnitsTotal, &p.State, &p.ContentDigest, &p.LastError, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return p, false, nil
	}
	p.UpdatedAt = updated.UTC().Format(time.RFC3339Nano)
	return p, err == nil, err
}

// Serialize only database mutations for this machine, never model calls. This
// keeps activation, retirement and cursor commits in one order across processes.
func (s *RecallExtractStore) begin(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.pg.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"agentsview:recall-extract:"+s.machine); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func (s *RecallExtractStore) UpsertExtractProgress(ctx context.Context, u db.ExtractProgressUpsert) (db.ExtractProgress, error) {
	var zero db.ExtractProgress
	if u.UnitsTotal < 0 || u.StampedAt.IsZero() {
		return zero, fmt.Errorf("extract progress requires a nonnegative unit count and transcript-read cutoff")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer func() { _ = tx.Rollback() }()
	if u.Session == nil || u.Session.ID != u.SessionID {
		return zero, fmt.Errorf("PG extraction requires the bracketed source snapshot")
	}
	if err := s.verifyGuard(ctx, tx, db.ExtractSessionGuard{
		SessionID: u.Session.ID, MessageCount: u.Session.MessageCount,
		TranscriptRevision: u.Session.TranscriptRevision, LocalModifiedAt: u.Session.LocalModifiedAt, EndedAt: u.Session.EndedAt,
	}); err != nil {
		return zero, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM recall_entries e
		WHERE e.machine=$1 AND e.source_session_id=$2 AND e.source_run_id=$3 AND e.review_state='unreviewed_auto'
		AND EXISTS (SELECT 1 FROM recall_extract_progress p WHERE p.machine=$1
		AND p.session_id=$2 AND p.generation_fingerprint=$3 AND p.content_digest<>$4)`,
		s.machine, u.SessionID, u.Fingerprint, u.ContentDigest); err != nil {
		return zero, err
	}
	state := db.ExtractProgressPending
	if u.UnitsTotal == 0 {
		state = db.ExtractProgressDone
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO recall_extract_progress AS p
		(machine, session_id, generation_fingerprint, unit_cursor, units_total, state, content_digest, content_stamped_at, source_modified_at)
		VALUES ($1,$2,$3,0,$4,$5,$6,$7,$8)
		ON CONFLICT (machine, session_id, generation_fingerprint) DO UPDATE SET
		unit_cursor=CASE WHEN p.content_digest=excluded.content_digest THEN p.unit_cursor ELSE 0 END,
		units_total=CASE WHEN p.content_digest=excluded.content_digest THEN p.units_total ELSE excluded.units_total END,
		state=CASE WHEN p.content_digest<>excluded.content_digest THEN $5
			WHEN excluded.units_total=0 THEN 'done' ELSE p.state END,
		last_error=CASE WHEN p.content_digest=excluded.content_digest AND excluded.units_total<>0 THEN p.last_error ELSE '' END,
		content_digest=excluded.content_digest, content_stamped_at=excluded.content_stamped_at,
		source_modified_at=excluded.source_modified_at,
		updated_at=CASE WHEN p.content_digest=excluded.content_digest AND p.state='failed' AND excluded.units_total<>0
			THEN p.updated_at ELSE clock_timestamp() END`,
		s.machine, u.SessionID, u.Fingerprint, u.UnitsTotal, state, u.ContentDigest, u.StampedAt, u.Session.LocalModifiedAt)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(); err != nil {
		return zero, err
	}
	p, found, err := s.ExtractProgress(ctx, u.SessionID, u.Fingerprint)
	if err == nil && !found {
		err = db.ErrStaleExtractProgress
	}
	return p, err
}

func (s *RecallExtractStore) markFailed(ctx context.Context, tx *sql.Tx, f db.ExtractFailure) error {
	result, err := tx.ExecContext(ctx, `UPDATE recall_extract_progress
		SET last_error=$4, unit_cursor=CASE WHEN $5 THEN 0 ELSE unit_cursor END,
		state='failed', updated_at=clock_timestamp()
		WHERE machine=$1 AND session_id=$2 AND generation_fingerprint=$3
		AND content_digest=$6 AND unit_cursor=$7 AND ($5 OR state<>'done')`,
		s.machine, f.SessionID, f.Fingerprint, f.LastError, f.Reopen, f.ExpectedDigest, f.ExpectedCursor)
	return extractAffected(result, err)
}

func extractAffected(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n == 0 {
		return db.ErrStaleExtractProgress
	}
	return err
}

func (s *RecallExtractStore) MarkExtractProgressFailed(ctx context.Context, f db.ExtractFailure) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.markFailed(ctx, tx, f); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *RecallExtractStore) DiscardExtractedSessionOutput(ctx context.Context, f db.ExtractFailure) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.markFailed(ctx, tx, f); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM recall_entries WHERE machine=$1
		AND source_session_id=$2 AND source_run_id=$3 AND review_state='unreviewed_auto'`,
		s.machine, f.SessionID, f.Fingerprint); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *RecallExtractStore) verifyGuard(ctx context.Context, tx *sql.Tx, g db.ExtractSessionGuard) error {
	var deleted, ended *time.Time
	var updated time.Time
	var automated bool
	var count int
	var revision string
	err := tx.QueryRowContext(ctx, `SELECT deleted_at, is_automated, message_count,
		transcript_revision, updated_at, ended_at FROM sessions
		WHERE id=$1 AND machine=$2 FOR SHARE`, g.SessionID, s.machine).
		Scan(&deleted, &automated, &count, &revision, &updated, &ended)
	if errors.Is(err, sql.ErrNoRows) {
		return db.ErrExtractSessionDrifted
	}
	if err != nil {
		return err
	}
	var endStamp *string
	if ended != nil {
		value := FormatISO8601(*ended)
		endStamp = &value
	}
	if deleted != nil || automated || count != g.MessageCount ||
		g.TranscriptRevision == nil || revision != *g.TranscriptRevision ||
		g.LocalModifiedAt == nil || updated.UTC().Format(time.RFC3339Nano) != *g.LocalModifiedAt ||
		!equalExtractString(endStamp, g.EndedAt) {
		return db.ErrExtractSessionDrifted
	}
	return nil
}

func equalExtractString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func pgExtractEvidence(ctx context.Context, tx *sql.Tx, session string, start, end int) (db.RecallEvidenceSelectionMetadata, error) {
	var empty db.RecallEvidenceSelectionMetadata
	rows, err := tx.QueryContext(ctx, `SELECT ordinal, role, content, source_uuid FROM messages
		WHERE session_id=$1 AND ordinal BETWEEN $2 AND $3 ORDER BY ordinal`, session, start, end)
	if err != nil {
		return empty, err
	}
	var messages []db.RecallEvidenceWindowMessage
	for rows.Next() {
		var message db.RecallEvidenceWindowMessage
		if err := rows.Scan(&message.Ordinal, &message.Role, &message.Content, &message.SourceUUID); err != nil {
			_ = rows.Close()
			return empty, err
		}
		messages = append(messages, message)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return empty, err
	}
	toolRows, err := tx.QueryContext(ctx, `SELECT message_ordinal, tool_name, category,
		COALESCE(tool_use_id,''), COALESCE(input_json,''), COALESCE(skill_name,''),
		COALESCE(result_content_length,0), COALESCE(result_content,''), COALESCE(subagent_session_id,'')
		FROM tool_calls WHERE session_id=$1 AND message_ordinal BETWEEN $2 AND $3
		ORDER BY message_ordinal, COALESCE(call_index,2147483647), id`, session, start, end)
	if err != nil {
		return empty, err
	}
	for toolRows.Next() {
		var ordinal int
		var call db.RecallEvidenceWindowToolCall
		if err := toolRows.Scan(&ordinal, &call.ToolName, &call.Category, &call.ToolUseID,
			&call.InputJSON, &call.SkillName, &call.ResultContentLength, &call.ResultContent, &call.SubagentSessionID); err != nil {
			_ = toolRows.Close()
			return empty, err
		}
		i := ordinal - start
		if i < 0 || i >= len(messages) || messages[i].Ordinal != ordinal {
			_ = toolRows.Close()
			return empty, db.ErrExtractSessionDrifted
		}
		messages[i].ToolCalls = append(messages[i].ToolCalls, call)
	}
	_ = toolRows.Close()
	if err := toolRows.Err(); err != nil {
		return empty, err
	}
	window, err := db.NewRecallEvidenceWindow(session, start, end, messages)
	if err != nil {
		return empty, fmt.Errorf("binding source evidence: %v: %w", err, db.ErrExtractSessionDrifted)
	}
	metadata, err := window.BindSelection(db.RecallEvidenceSelection{MessageStartOrdinal: start, MessageEndOrdinal: end})
	if err != nil {
		return empty, fmt.Errorf("binding source evidence: %v: %w", err, db.ErrExtractSessionDrifted)
	}
	return metadata, nil
}

func (s *RecallExtractStore) CommitExtractedUnit(ctx context.Context, u db.ExtractUnitCommit) (int, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.verifyGuard(ctx, tx, db.ExtractSessionGuard{
		SessionID: u.SessionID, MessageCount: u.MessageCount, TranscriptRevision: u.TranscriptRevision,
		LocalModifiedAt: u.LocalModifiedAt, EndedAt: u.EndedAt,
	}); err != nil {
		return 0, err
	}
	// Check the cursor before loading evidence. A rejected replay performs no
	// provenance work and cannot disturb already accepted entries.
	result, err := tx.ExecContext(ctx, `UPDATE recall_extract_progress SET unit_cursor=$4+1,
		state=CASE WHEN $4+1>=units_total THEN 'done' ELSE 'partial' END,
		last_error='', updated_at=clock_timestamp()
		WHERE machine=$1 AND session_id=$2 AND generation_fingerprint=$3
		AND unit_cursor=$4 AND content_digest=$5 AND $4+1<=units_total`,
		s.machine, u.SessionID, u.Fingerprint, u.Cursor, u.Digest)
	if err := extractAffected(result, err); err != nil {
		return 0, err
	}
	var state string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM recall_extract_generations WHERE machine=$1 AND fingerprint=$2`,
		s.machine, u.Fingerprint).Scan(&state); err != nil {
		return 0, err
	}
	bound := make(map[[2]int]db.RecallEvidenceSelectionMetadata)
	entries := make([]db.RecallEntry, 0, len(u.Entries))
	for _, entry := range u.Entries {
		if entry.ID == "" || entry.SourceSessionID != u.SessionID || entry.SourceRunID != u.Fingerprint ||
			entry.ReviewState != "unreviewed_auto" {
			return 0, fmt.Errorf("extracted entry has invalid source ownership")
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM recall_entries WHERE machine=$1 AND id=$2)`,
			s.machine, entry.ID).Scan(&exists); err != nil {
			return 0, err
		}
		if exists {
			continue
		}
		entry.Status = "archived"
		if state == db.ExtractGenerationActive {
			entry.Status = "accepted"
		}
		entry.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		entry.UpdatedAt = entry.CreatedAt
		entry.Evidence = append([]db.RecallEvidence(nil), entry.Evidence...)
		for i, ev := range entry.Evidence {
			if ev.SessionID != u.SessionID || (ev.EntryID != "" && ev.EntryID != entry.ID) {
				return 0, fmt.Errorf("extracted evidence has invalid source ownership")
			}
			key := [2]int{ev.MessageStartOrdinal, ev.MessageEndOrdinal}
			metadata, found := bound[key]
			if !found {
				metadata, err = pgExtractEvidence(ctx, tx, u.SessionID, key[0], key[1])
				if err != nil {
					return 0, err
				}
				bound[key] = metadata
			}
			entry.Evidence[i].ContentDigest = metadata.ContentDigest
			entry.Evidence[i].MessageStartSourceUUID = metadata.MessageStartSourceUUID
			entry.Evidence[i].MessageEndSourceUUID = metadata.MessageEndSourceUUID
		}
		entries = append(entries, entry)
	}
	if err := insertPGRecallPublication(ctx, tx, s.machine, entries); err != nil {
		return 0, err
	}
	for _, entry := range entries {
		if _, err := tx.ExecContext(ctx, `UPDATE recall_entries SET publication_owner='central' WHERE machine=$1 AND id=$2`, s.machine, entry.ID); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(entries), nil
}

func (s *RecallExtractStore) RefreshExtractedSessionCoverage(ctx context.Context, u db.ExtractCoverageRefresh) (db.ExtractProgress, error) {
	var zero db.ExtractProgress
	if u.Session == nil || u.StampedAt.IsZero() {
		return zero, fmt.Errorf("coverage refresh requires a source snapshot and transcript-read cutoff")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.verifyGuard(ctx, tx, db.ExtractSessionGuard{
		SessionID: u.Session.ID, MessageCount: u.Session.MessageCount,
		TranscriptRevision: u.Session.TranscriptRevision, LocalModifiedAt: u.Session.LocalModifiedAt, EndedAt: u.Session.EndedAt,
	}); err != nil {
		return zero, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE recall_extract_progress SET content_stamped_at=$5,
		source_modified_at=$6, updated_at=CASE WHEN state='failed' THEN updated_at ELSE clock_timestamp() END
		WHERE machine=$1 AND session_id=$2 AND generation_fingerprint=$3 AND content_digest=$4`,
		s.machine, u.Session.ID, u.Fingerprint, u.Digest, u.StampedAt, u.Session.LocalModifiedAt)
	if err := extractAffected(result, err); err != nil {
		return zero, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE recall_entries SET project=$4,cwd=$5,git_branch=$6,agent=$7,updated_at=clock_timestamp()
		WHERE machine=$1 AND source_session_id=$2 AND source_run_id=$3 AND review_state='unreviewed_auto'
		AND (project,cwd,git_branch,agent) IS DISTINCT FROM ($4,$5,$6,$7)`,
		s.machine, u.Session.ID, u.Fingerprint, u.Session.Project, u.Session.Cwd, u.Session.GitBranch, u.Session.Agent); err != nil {
		return zero, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT ev.id, ev.entry_id, ev.message_start_ordinal, ev.message_end_ordinal
		FROM recall_evidence ev JOIN recall_entries e ON e.id=ev.entry_id
		WHERE e.machine=$1 AND e.source_session_id=$2 AND e.source_run_id=$3 AND e.review_state='unreviewed_auto'
		AND ev.session_id=$2`, s.machine, u.Session.ID, u.Fingerprint)
	if err != nil {
		return zero, err
	}
	type evidenceRow struct {
		id         int64
		entry      string
		start, end int
	}
	var evidence []evidenceRow
	for rows.Next() {
		var ev evidenceRow
		if err := rows.Scan(&ev.id, &ev.entry, &ev.start, &ev.end); err != nil {
			_ = rows.Close()
			return zero, err
		}
		evidence = append(evidence, ev)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return zero, err
	}
	bound := make(map[[2]int]db.RecallEvidenceSelectionMetadata)
	for _, ev := range evidence {
		key := [2]int{ev.start, ev.end}
		metadata, found := bound[key]
		if !found {
			metadata, err = pgExtractEvidence(ctx, tx, u.Session.ID, ev.start, ev.end)
			if err != nil {
				return zero, err
			}
			bound[key] = metadata
		}
		if _, err := tx.ExecContext(ctx, `UPDATE recall_evidence SET content_digest=$2,
			message_start_source_uuid=$3,message_end_source_uuid=$4 WHERE id=$1`,
			ev.id, metadata.ContentDigest, metadata.MessageStartSourceUUID, metadata.MessageEndSourceUUID); err != nil {
			return zero, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE recall_entries SET provenance_ok=TRUE,updated_at=clock_timestamp()
			WHERE id=$1 AND NOT provenance_ok`, ev.entry); err != nil {
			return zero, err
		}
	}
	if err := tx.Commit(); err != nil {
		return zero, err
	}
	p, found, err := s.ExtractProgress(ctx, u.Session.ID, u.Fingerprint)
	if err == nil && !found {
		err = db.ErrStaleExtractProgress
	}
	return p, err
}

func (s *RecallExtractStore) ReconcileIneligibleExtractSessions(ctx context.Context, _ time.Time) (int, int, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback() }()
	// Recheck all source metadata: updated_at records transaction start, and
	// using a wall-clock watermark would miss late-committing exclusions.
	result, err := tx.ExecContext(ctx, `DELETE FROM recall_entries e WHERE e.machine=$1
		AND e.review_state='unreviewed_auto' AND EXISTS (SELECT 1 FROM recall_extract_generations g
		WHERE g.machine=e.machine AND g.fingerprint=e.source_run_id)
		AND EXISTS (SELECT 1 FROM sessions s WHERE s.id=e.source_session_id AND s.machine=$1
		AND (s.deleted_at IS NOT NULL OR s.is_automated))`, s.machine)
	if err != nil {
		return 0, 0, err
	}
	entries, err := result.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	result, err = tx.ExecContext(ctx, `DELETE FROM recall_extract_progress p WHERE p.machine=$1
		AND EXISTS (SELECT 1 FROM sessions s WHERE s.id=p.session_id AND s.machine=$1
		AND (s.deleted_at IS NOT NULL OR s.is_automated))`, s.machine)
	if err != nil {
		return 0, 0, err
	}
	progress, err := result.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return int(progress), int(entries), nil
}

func (s *RecallExtractStore) ExtractProgressStats(ctx context.Context, fingerprint string) (db.ExtractProgressStats, error) {
	var stats db.ExtractProgressStats
	err := s.pg.QueryRowContext(ctx, `SELECT
		count(*) FILTER(WHERE state='pending'),count(*) FILTER(WHERE state='partial'),
		count(*) FILTER(WHERE state='done'),count(*) FILTER(WHERE state='failed'),
		COALESCE(sum(unit_cursor),0),COALESCE(sum(units_total),0)
		FROM recall_extract_progress WHERE machine=$1 AND generation_fingerprint=$2`, s.machine, fingerprint).
		Scan(&stats.Pending, &stats.Partial, &stats.Done, &stats.Failed, &stats.UnitsDone, &stats.UnitsTotal)
	if err != nil {
		return stats, err
	}
	err = s.pg.QueryRowContext(ctx, `SELECT count(*) FROM recall_entries WHERE machine=$1 AND source_run_id=$2`,
		s.machine, fingerprint).Scan(&stats.Entries)
	return stats, err
}

func (s *RecallExtractStore) ActivateExtractGeneration(ctx context.Context, fingerprint string, _ []string, quietCutoff time.Time) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// Hold source rows through publication. Ingestion updates the parent row
	// before replacing messages, so it cannot change checked evidence mid-switch.
	rows, err := tx.QueryContext(ctx, `SELECT id FROM sessions WHERE machine=$1 FOR SHARE`, s.machine)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return err
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	eligible := pgExtractEligible + "$3"
	var blocked bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sessions s
		LEFT JOIN recall_extract_progress p ON p.session_id=s.id AND p.machine=s.machine AND p.generation_fingerprint=$2
		WHERE s.machine=$1 AND (
			(`+eligible+` AND (p.session_id IS NULL OR p.state IN ('pending','partial')))
			OR (s.deleted_at IS NULL AND NOT s.is_automated AND p.state='done' AND s.updated_at IS DISTINCT FROM p.source_modified_at)
			OR (`+eligible+` AND p.state='failed' AND s.updated_at IS DISTINCT FROM p.source_modified_at
				AND EXISTS(SELECT 1 FROM recall_entries e WHERE e.machine=$1 AND e.source_session_id=s.id AND e.source_run_id=$2 AND e.status='archived'))
		))`, s.machine, fingerprint, quietCutoff).Scan(&blocked)
	if err != nil {
		return err
	}
	if blocked {
		return db.ErrExtractActivationBlocked
	}
	if _, err := tx.ExecContext(ctx, `UPDATE recall_extract_generations SET state='retired',updated_at=clock_timestamp()
		WHERE machine=$1 AND state='active' AND fingerprint<>$2`, s.machine, fingerprint); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE recall_extract_generations SET state='active',updated_at=clock_timestamp()
		WHERE machine=$1 AND fingerprint=$2`, s.machine, fingerprint)
	if err := extractAffected(result, err); err != nil {
		if errors.Is(err, db.ErrStaleExtractProgress) {
			return db.ErrExtractGenerationNotFound
		}
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE recall_entries e SET status='archived',updated_at=clock_timestamp()
		WHERE e.machine=$1 AND e.review_state='unreviewed_auto' AND e.status='accepted' AND e.source_run_id<>$2
		AND EXISTS(SELECT 1 FROM recall_extract_generations g WHERE g.machine=e.machine AND g.fingerprint=e.source_run_id)`,
		s.machine, fingerprint); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM recall_entries e WHERE e.machine=$1
		AND e.source_run_id=$2 AND e.status='archived' AND e.review_state='unreviewed_auto'
		AND NOT EXISTS(SELECT 1 FROM sessions s WHERE s.id=e.source_session_id AND s.machine=$1 AND `+eligible+`)`,
		s.machine, fingerprint, quietCutoff); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM recall_extract_progress p WHERE p.machine=$1 AND p.generation_fingerprint=$2
		AND NOT EXISTS(SELECT 1 FROM sessions s WHERE s.id=p.session_id AND s.machine=$1 AND `+eligible+`)`,
		s.machine, fingerprint, quietCutoff); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE recall_entries SET status='accepted',updated_at=clock_timestamp()
		WHERE machine=$1 AND source_run_id=$2 AND review_state='unreviewed_auto' AND status='archived' AND superseded_by_entry_id=''`,
		s.machine, fingerprint); err != nil {
		return err
	}
	var servable bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM recall_entries WHERE machine=$1 AND source_run_id=$2
		AND review_state='unreviewed_auto' AND status='accepted' AND superseded_by_entry_id='' AND provenance_ok)`,
		s.machine, fingerprint).Scan(&servable); err != nil {
		return err
	}
	if !servable {
		return db.ErrExtractActivationBlocked
	}
	return tx.Commit()
}

func (s *RecallExtractStore) RetireExtractGeneration(ctx context.Context, fingerprint string, force bool) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var state string
	err = tx.QueryRowContext(ctx, `SELECT state FROM recall_extract_generations WHERE machine=$1 AND fingerprint=$2`,
		s.machine, fingerprint).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return db.ErrExtractGenerationNotFound
	}
	if err != nil {
		return err
	}
	if state == db.ExtractGenerationActive && !force {
		return db.ErrExtractGenerationActive
	}
	if _, err := tx.ExecContext(ctx, `UPDATE recall_extract_generations SET state='retired',updated_at=clock_timestamp()
		WHERE machine=$1 AND fingerprint=$2`, s.machine, fingerprint); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE recall_entries SET status='archived',updated_at=clock_timestamp()
		WHERE machine=$1 AND source_run_id=$2 AND status='accepted' AND review_state='unreviewed_auto'`, s.machine, fingerprint); err != nil {
		return err
	}
	return tx.Commit()
}
