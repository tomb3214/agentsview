package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"strconv"
	"strings"
)

// RecallPublicationSnapshot is one transactionally consistent view of the
// local derived Recall corpus. It is used by the existing PostgreSQL push path
// and deliberately excludes query measurements and vector state.
type RecallPublicationSnapshot struct {
	Revision           string
	Entries            []RecallEntry
	ExtractionRevision string
	Generations        []ExtractGeneration
	Progress           []ExtractProgress
}

// RecallPublicationSnapshot returns all Recall entries whose source session
// is inside the configured PostgreSQL publication scope, including evidence.
func (db *DB) RecallPublicationSnapshot(
	ctx context.Context, projects, excludeProjects []string,
) (RecallPublicationSnapshot, error) {
	if len(projects) > 0 && len(excludeProjects) > 0 {
		return RecallPublicationSnapshot{}, fmt.Errorf(
			"recall publication projects and exclude projects are mutually exclusive",
		)
	}
	tx, err := db.getReader().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return RecallPublicationSnapshot{}, fmt.Errorf(
			"beginning recall publication snapshot: %w", err,
		)
	}
	defer func() { _ = tx.Rollback() }()

	var revision int64
	if err := tx.QueryRowContext(ctx,
		"SELECT revision FROM recall_query_state WHERE singleton = 1",
	).Scan(&revision); err != nil {
		return RecallPublicationSnapshot{}, fmt.Errorf(
			"reading recall publication revision: %w", err,
		)
	}

	scopeSQL, scopeArgs := recallPublicationScopeSQL(projects, excludeProjects)
	rows, err := tx.QueryContext(ctx,
		"SELECT "+recallBaseColsQualified+`
		 FROM recall_entries
		 JOIN sessions ON sessions.id = recall_entries.source_session_id
		 WHERE `+scopeSQL+`
		 ORDER BY recall_entries.id`, scopeArgs...)
	if err != nil {
		return RecallPublicationSnapshot{}, fmt.Errorf(
			"querying recall publication entries: %w", err,
		)
	}
	entries, err := scanRecallEntryRows(rows)
	_ = rows.Close()
	if err != nil {
		return RecallPublicationSnapshot{}, err
	}

	evidenceRows, err := tx.QueryContext(ctx, `
		SELECT recall_evidence.id, recall_evidence.entry_id,
		       recall_evidence.session_id,
		       recall_evidence.message_start_ordinal,
		       recall_evidence.message_end_ordinal,
		       recall_evidence.message_start_source_uuid,
		       recall_evidence.message_end_source_uuid,
		       recall_evidence.content_digest,
		       recall_evidence.tool_use_id,
		       recall_evidence.snippet
		FROM recall_evidence
		JOIN recall_entries ON recall_entries.id = recall_evidence.entry_id
		JOIN sessions ON sessions.id = recall_entries.source_session_id
		WHERE `+scopeSQL+`
		ORDER BY recall_evidence.entry_id, recall_evidence.id`, scopeArgs...)
	if err != nil {
		return RecallPublicationSnapshot{}, fmt.Errorf(
			"querying recall publication evidence: %w", err,
		)
	}
	evidenceByEntry := make(map[string][]RecallEvidence)
	for evidenceRows.Next() {
		evidence, scanErr := scanRecallEvidenceRow(evidenceRows)
		if scanErr != nil {
			_ = evidenceRows.Close()
			return RecallPublicationSnapshot{}, fmt.Errorf(
				"scanning recall publication evidence: %w", scanErr,
			)
		}
		evidenceByEntry[evidence.EntryID] = append(
			evidenceByEntry[evidence.EntryID], evidence,
		)
	}
	if err := evidenceRows.Err(); err != nil {
		_ = evidenceRows.Close()
		return RecallPublicationSnapshot{}, fmt.Errorf(
			"iterating recall publication evidence: %w", err,
		)
	}
	_ = evidenceRows.Close()
	for i := range entries {
		entries[i].Evidence = evidenceByEntry[entries[i].ID]
	}
	snapshot := RecallPublicationSnapshot{
		Revision: recallQueryRevisionPrefix + strconv.FormatInt(revision, 10), Entries: entries,
	}
	if err := readRecallExtractionPublication(ctx, tx, scopeSQL, scopeArgs, &snapshot); err != nil {
		return RecallPublicationSnapshot{}, err
	}

	if err := tx.Commit(); err != nil {
		return RecallPublicationSnapshot{}, fmt.Errorf(
			"committing recall publication snapshot: %w", err,
		)
	}
	return snapshot, nil
}

// Checkpoints and entries share the same SQLite read transaction. A cursor must
// never be published ahead of the entries accepted by its corresponding unit.
func readRecallExtractionPublication(ctx context.Context, tx *sql.Tx, scope string, args []any, snapshot *RecallPublicationSnapshot) error {
	rows, err := tx.QueryContext(ctx, `SELECT fingerprint,state,model,segmenter,params_json,created_at,updated_at
		FROM recall_extract_generations ORDER BY fingerprint`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var gen ExtractGeneration
		if err := rows.Scan(&gen.Fingerprint, &gen.State, &gen.Model, &gen.Segmenter, &gen.ParamsJSON, &gen.CreatedAt, &gen.UpdatedAt); err != nil {
			_ = rows.Close()
			return err
		}
		snapshot.Generations = append(snapshot.Generations, gen)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	rows, err = tx.QueryContext(ctx, `SELECT p.session_id,p.generation_fingerprint,p.unit_cursor,p.units_total,
		p.state,p.content_digest,p.last_error,p.updated_at FROM recall_extract_progress p
		JOIN sessions ON sessions.id=p.session_id WHERE `+scope+` ORDER BY p.session_id,p.generation_fingerprint`, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var p ExtractProgress
		if err := rows.Scan(&p.SessionID, &p.GenerationFingerprint, &p.UnitCursor, &p.UnitsTotal, &p.State, &p.ContentDigest, &p.LastError, &p.UpdatedAt); err != nil {
			_ = rows.Close()
			return err
		}
		snapshot.Progress = append(snapshot.Progress, p)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	// The entry query revision does not change for a zero-entry unit. Include
	// checkpoint state in the push version without invalidating read cursors.
	raw, err := json.Marshal(struct {
		Generations []ExtractGeneration
		Progress    []ExtractProgress
	}{snapshot.Generations, snapshot.Progress})
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	snapshot.ExtractionRevision = hex.EncodeToString(digest[:])
	return nil
}

func recallPublicationScopeSQL(
	projects, excludeProjects []string,
) (string, []any) {
	values := projects
	operator := "IN"
	if len(excludeProjects) > 0 {
		values = excludeProjects
		operator = "NOT IN"
	}
	if len(values) == 0 {
		return "1=1", nil
	}
	args := make([]any, 0, len(values))
	placeholders := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		args = append(args, value)
		placeholders = append(placeholders, "?")
	}
	if len(args) == 0 {
		return "1=1", nil
	}
	return "sessions.project " + operator + " (" +
		strings.Join(placeholders, ",") + ")", args
}
