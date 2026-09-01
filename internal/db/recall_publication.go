package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// RecallPublicationSnapshot is one transactionally consistent view of the
// local derived Recall corpus. It is used by the existing PostgreSQL push path
// and deliberately excludes query measurements and vector state.
type RecallPublicationSnapshot struct {
	Revision string
	Entries  []RecallEntry
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

	if err := tx.Commit(); err != nil {
		return RecallPublicationSnapshot{}, fmt.Errorf(
			"committing recall publication snapshot: %w", err,
		)
	}
	return RecallPublicationSnapshot{
		Revision: recallQueryRevisionPrefix + strconv.FormatInt(revision, 10),
		Entries:  entries,
	}, nil
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
