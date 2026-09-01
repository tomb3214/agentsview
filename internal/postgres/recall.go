package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.kenn.io/agentsview/internal/db"
	corerecall "go.kenn.io/agentsview/internal/recall"
)

const pgRecallCandidateLimit = 50000

const pgRecallCols = `id, machine, type, scope, status, review_state, title,
	body, trigger, confidence, uncertainty, project, cwd, git_branch, agent,
	source_session_id, source_episode_id, source_run_id, extractor_method,
	model, transferable, provenance_ok, supersedes_entry_id,
	superseded_by_entry_id, created_at, updated_at`

type pgRecallRowScanner interface {
	Scan(dest ...any) error
}

func scanPGRecallEntry(row pgRecallRowScanner) (db.RecallEntry, error) {
	var entry db.RecallEntry
	var confidence sql.NullFloat64
	var createdAt, updatedAt time.Time
	err := row.Scan(
		&entry.ID, &entry.Machine, &entry.Type, &entry.Scope, &entry.Status,
		&entry.ReviewState, &entry.Title, &entry.Body, &entry.Trigger,
		&confidence, &entry.Uncertainty, &entry.Project, &entry.CWD,
		&entry.GitBranch, &entry.Agent, &entry.SourceSessionID,
		&entry.SourceEpisodeID, &entry.SourceRunID, &entry.ExtractorMethod,
		&entry.Model, &entry.Transferable, &entry.ProvenanceOK,
		&entry.SupersedesEntryID, &entry.SupersededByEntryID,
		&createdAt, &updatedAt,
	)
	if confidence.Valid {
		entry.Confidence = &confidence.Float64
	}
	entry.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
	entry.UpdatedAt = updatedAt.UTC().Format(time.RFC3339Nano)
	return entry, err
}

func (s *Store) GetRecallEntry(
	ctx context.Context, id string,
) (*db.RecallEntry, error) {
	entry, err := scanPGRecallEntry(s.pg.QueryRowContext(
		ctx, "SELECT "+pgRecallCols+" FROM recall_entries WHERE id = $1", id,
	))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting PostgreSQL recall entry %s: %w", id, err)
	}
	if err := s.attachPGRecallEvidence(ctx, []db.RecallEntry{entry}, func(got []db.RecallEntry) {
		entry = got[0]
	}); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (s *Store) ListRecallEntries(
	ctx context.Context, query db.RecallQuery,
) ([]db.RecallEntry, error) {
	if err := db.ValidateRecallQuery(query); err != nil {
		return nil, err
	}
	query = db.NormalizeRecallQuery(query)
	where, args := pgRecallWhere(query)
	limit := query.Limit
	if limit <= 0 {
		limit = db.DefaultRecallEntryLimit
	}
	if limit > db.MaxRecallEntryLimit {
		limit = db.MaxRecallEntryLimit
	}
	args = append(args, limit)
	rows, err := s.pg.QueryContext(ctx,
		"SELECT "+pgRecallCols+" FROM recall_entries WHERE "+where+
			fmt.Sprintf(" ORDER BY updated_at DESC, id ASC LIMIT $%d", len(args)),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("listing PostgreSQL recall entries: %w", err)
	}
	entries, err := scanPGRecallEntries(rows)
	if err != nil {
		return nil, err
	}
	if err := s.attachPGRecallEvidence(ctx, entries, func(got []db.RecallEntry) {
		entries = got
	}); err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *Store) QueryRecallEntries(
	ctx context.Context, query db.RecallQuery,
) (db.RecallPage, error) {
	if err := db.ValidateRecallQuery(query); err != nil {
		return db.RecallPage{}, err
	}
	query = db.NormalizeRecallQuery(query)
	if query.Mode != db.RecallQueryModeLexical {
		return db.RecallPage{}, db.NewSemanticUnavailableError(
			"remote PostgreSQL Recall currently supports lexical queries only",
		)
	}
	if strings.TrimSpace(query.Text) == "" {
		entries, err := s.ListRecallEntries(ctx, query)
		if err != nil {
			return db.RecallPage{}, err
		}
		page := db.RecallPage{RecallEntries: make([]db.RecallResult, 0, len(entries))}
		for _, entry := range entries {
			page.RecallEntries = append(page.RecallEntries, db.RecallResult{RecallEntry: entry})
		}
		return page, nil
	}
	terms := corerecall.ScoringQueryTerms(query.Text)
	if len(terms) == 0 && !corerecall.QueryUsesTemporalSignals(query.Text) {
		return db.RecallPage{RecallEntries: []db.RecallResult{}}, nil
	}

	where, args := pgRecallWhere(query)
	if !corerecall.QueryUsesTemporalSignals(query.Text) {
		args = append(args, strings.Join(terms, " | "))
		placeholder := fmt.Sprintf("$%d", len(args))
		where += ` AND (
			to_tsvector('simple', concat_ws(' ', title, body, trigger,
				project, cwd, git_branch, agent, source_session_id,
				source_episode_id, source_run_id, extractor_method, model))
				@@ to_tsquery('simple', ` + placeholder + `)
			OR EXISTS (
				SELECT 1 FROM recall_evidence
				WHERE recall_evidence.entry_id = recall_entries.id
				  AND to_tsvector('simple', recall_evidence.snippet)
				      @@ to_tsquery('simple', ` + placeholder + `)
			)
		)`
	}
	args = append(args, pgRecallCandidateLimit)
	rows, err := s.pg.QueryContext(ctx,
		"SELECT "+pgRecallCols+" FROM recall_entries WHERE "+where+
			fmt.Sprintf(" ORDER BY updated_at DESC, id ASC LIMIT $%d", len(args)),
		args...,
	)
	if err != nil {
		return db.RecallPage{}, fmt.Errorf("querying PostgreSQL recall candidates: %w", err)
	}
	entries, err := scanPGRecallEntries(rows)
	if err != nil {
		return db.RecallPage{}, err
	}
	if err := s.attachPGRecallEvidence(ctx, entries, func(got []db.RecallEntry) {
		entries = got
	}); err != nil {
		return db.RecallPage{}, err
	}
	return db.RankRecallEntries(query, entries), nil
}

func scanPGRecallEntries(rows *sql.Rows) ([]db.RecallEntry, error) {
	defer rows.Close()
	var entries []db.RecallEntry
	for rows.Next() {
		entry, err := scanPGRecallEntry(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning PostgreSQL recall entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating PostgreSQL recall entries: %w", err)
	}
	return entries, nil
}

func (s *Store) attachPGRecallEvidence(
	ctx context.Context, entries []db.RecallEntry, apply func([]db.RecallEntry),
) error {
	if len(entries) == 0 {
		apply(entries)
		return nil
	}
	args := make([]any, 0, len(entries))
	placeholders := make([]string, 0, len(entries))
	for i, entry := range entries {
		args = append(args, entry.ID)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}
	rows, err := s.pg.QueryContext(ctx, `
		SELECT id, entry_id, session_id, message_start_ordinal,
		       message_end_ordinal, message_start_source_uuid,
		       message_end_source_uuid, content_digest, tool_use_id, snippet
		FROM recall_evidence
		WHERE entry_id IN (`+strings.Join(placeholders, ",")+`)
		ORDER BY entry_id, id`, args...)
	if err != nil {
		return fmt.Errorf("listing PostgreSQL recall evidence: %w", err)
	}
	defer rows.Close()
	byEntry := make(map[string][]db.RecallEvidence)
	for rows.Next() {
		var evidence db.RecallEvidence
		if err := rows.Scan(
			&evidence.ID, &evidence.EntryID, &evidence.SessionID,
			&evidence.MessageStartOrdinal, &evidence.MessageEndOrdinal,
			&evidence.MessageStartSourceUUID, &evidence.MessageEndSourceUUID,
			&evidence.ContentDigest, &evidence.ToolUseID, &evidence.Snippet,
		); err != nil {
			return fmt.Errorf("scanning PostgreSQL recall evidence: %w", err)
		}
		byEntry[evidence.EntryID] = append(byEntry[evidence.EntryID], evidence)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating PostgreSQL recall evidence: %w", err)
	}
	for i := range entries {
		entries[i].Evidence = byEntry[entries[i].ID]
	}
	apply(entries)
	return nil
}

func pgRecallWhere(query db.RecallQuery) (string, []any) {
	predicates := []string{"1=1"}
	args := []any{}
	add := func(sql string, value any) {
		args = append(args, value)
		predicates = append(predicates, fmt.Sprintf(sql, len(args)))
	}
	status := query.Status
	if status == "" {
		status = corerecall.StatusAccepted
	}
	add("status = $%d", status)
	filters := []struct {
		column string
		value  string
	}{
		{"machine", query.Machine}, {"project", query.Project},
		{"cwd", query.CWD}, {"git_branch", query.GitBranch},
		{"agent", query.Agent}, {"type", query.Type}, {"scope", query.Scope},
		{"review_state", query.ReviewState},
		{"extractor_method", query.ExtractorMethod},
		{"source_session_id", query.SourceSessionID},
		{"source_episode_id", query.SourceEpisodeID},
		{"source_run_id", query.SourceRunID},
		{"supersedes_entry_id", query.SupersedesEntryID},
		{"superseded_by_entry_id", query.SupersededByEntryID},
	}
	for _, filter := range filters {
		if filter.value != "" {
			add(filter.column+" = $%d", filter.value)
		}
	}
	if query.CursorUpdatedAt != "" {
		args = append(args, query.CursorUpdatedAt, query.CursorUpdatedAt, query.CursorID)
		start := len(args) - 2
		predicates = append(predicates, fmt.Sprintf(
			"(updated_at < $%d OR (updated_at = $%d AND id > $%d))",
			start, start+1, start+2,
		))
	}
	if query.TrustedOnly {
		add("review_state = $%d", corerecall.ReviewStateHumanReviewed)
		predicates = append(predicates, "transferable", "provenance_ok")
	}
	return strings.Join(predicates, " AND "), args
}
