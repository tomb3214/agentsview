package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"go.kenn.io/agentsview/internal/db"
)

const recallPublicationRevisionStateKey = "recall_publication_revision_v1"

// PushRecall publishes only the local Recall corpus and its bounded evidence.
// It deliberately skips session, message, curation, pricing, and vector
// publication so a verified recovery archive can restore derived Recall state
// without presenting itself as a replacement source archive.
func (s *Sync) PushRecall(ctx context.Context, full bool) error {
	return s.syncRecallPublication(ctx, s.effectiveSyncState(), full)
}

func (s *Sync) syncRecallPublication(
	ctx context.Context, state syncStateStore, full bool,
) error {
	snapshot, err := s.local.RecallPublicationSnapshot(
		ctx, s.projects, s.excludeProjects,
	)
	if err != nil {
		return fmt.Errorf("reading local Recall publication: %w", err)
	}
	version := s.databaseGeneration + "\x00" + snapshot.Revision
	previous, err := state.GetSyncState(recallPublicationRevisionStateKey)
	if err != nil {
		return fmt.Errorf("reading Recall publication state: %w", err)
	}
	if !full && previous == version {
		return nil
	}

	tx, err := s.pg.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning Recall publication: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := deletePGRecallPublicationScope(
		ctx, tx, s.machine, s.projects, s.excludeProjects,
	); err != nil {
		return err
	}
	if err := insertPGRecallPublication(ctx, tx, s.machine, snapshot.Entries); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing Recall publication: %w", err)
	}
	if err := state.SetSyncState(recallPublicationRevisionStateKey, version); err != nil {
		return fmt.Errorf("recording Recall publication state: %w", err)
	}
	log.Printf(
		"pgsync: published %d Recall entr%s for machine %s",
		len(snapshot.Entries), pluralSuffix(len(snapshot.Entries), "y", "ies"), s.machine,
	)
	return nil
}

// Batch requests without changing the encompassing publication transaction.
// Insert parents first, then evidence in its original entry/evidence order.
func insertPGRecallPublication(
	ctx context.Context, tx pgSessionExecer, machine string, entries []db.RecallEntry,
) error {
	const batchSize = 100
	for start := 0; start < len(entries); start += batchSize {
		batch := entries[start:min(start+batchSize, len(entries))]
		values := make([]string, 0, len(batch))
		args := make([]any, 0, len(batch)*26)
		for _, entry := range batch {
			base := len(args)
			placeholders := make([]string, 26)
			for i := range placeholders {
				placeholders[i] = fmt.Sprintf("$%d", base+i+1)
			}
			placeholders[24] += "::timestamptz"
			placeholders[25] += "::timestamptz"
			values = append(values, "("+strings.Join(placeholders, ",")+")")
			args = append(args,
				entry.ID, machine, entry.Type, entry.Scope, entry.Status,
				entry.ReviewState, entry.Title, entry.Body, entry.Trigger,
				entry.Confidence, entry.Uncertainty, entry.Project, entry.CWD,
				entry.GitBranch, entry.Agent, entry.SourceSessionID,
				entry.SourceEpisodeID, entry.SourceRunID, entry.ExtractorMethod,
				entry.Model, entry.Transferable, entry.ProvenanceOK,
				entry.SupersedesEntryID, entry.SupersededByEntryID,
				entry.CreatedAt, entry.UpdatedAt,
			)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO recall_entries (
				id, machine, type, scope, status, review_state, title, body,
				trigger, confidence, uncertainty, project, cwd, git_branch,
				agent, source_session_id, source_episode_id, source_run_id,
				extractor_method, model, transferable, provenance_ok,
				supersedes_entry_id, superseded_by_entry_id, created_at, updated_at
			) VALUES `+strings.Join(values, ","), args...); err != nil {
			return fmt.Errorf("publishing Recall entries starting at %s: %w", batch[0].ID, err)
		}
	}
	values := make([]string, 0, batchSize)
	args := make([]any, 0, batchSize*9)
	flushEvidence := func() error {
		if len(values) == 0 {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO recall_evidence (
				entry_id, session_id, message_start_ordinal,
				message_end_ordinal, message_start_source_uuid,
				message_end_source_uuid, content_digest, tool_use_id, snippet
			) VALUES `+strings.Join(values, ","), args...); err != nil {
			return fmt.Errorf("publishing Recall evidence starting at %s: %w", args[0], err)
		}
		clear(args)
		args = args[:0]
		values = values[:0]
		return nil
	}
	for _, entry := range entries {
		for _, evidence := range entry.Evidence {
			base := len(args)
			placeholders := make([]string, 9)
			for i := range placeholders {
				placeholders[i] = fmt.Sprintf("$%d", base+i+1)
			}
			values = append(values, "("+strings.Join(placeholders, ",")+")")
			args = append(args,
				entry.ID, evidence.SessionID, evidence.MessageStartOrdinal,
				evidence.MessageEndOrdinal, evidence.MessageStartSourceUUID,
				evidence.MessageEndSourceUUID, evidence.ContentDigest,
				evidence.ToolUseID, evidence.Snippet,
			)
			if len(values) == batchSize {
				if err := flushEvidence(); err != nil {
					return err
				}
			}
		}
	}
	return flushEvidence()
}

func deletePGRecallPublicationScope(
	ctx context.Context,
	tx *sql.Tx,
	machine string,
	projects, excludeProjects []string,
) error {
	args := []any{machine}
	predicate := "TRUE"
	values := projects
	operator := "IN"
	if len(excludeProjects) > 0 {
		values = excludeProjects
		operator = "NOT IN"
	}
	if len(values) > 0 {
		placeholders := make([]string, 0, len(values))
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			args = append(args, value)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		if len(placeholders) > 0 {
			predicate = "sessions.project " + operator + " (" +
				strings.Join(placeholders, ",") + ")"
		}
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM recall_entries
		WHERE machine = $1
		  AND EXISTS (
			SELECT 1 FROM sessions
			WHERE sessions.id = recall_entries.source_session_id
			  AND `+predicate+`
		)`, args...)
	if err != nil {
		return fmt.Errorf("clearing prior Recall publication scope: %w", err)
	}
	return nil
}

func pluralSuffix(count int, singular, plural string) string {
	if count == 1 {
		return singular
	}
	return plural
}
