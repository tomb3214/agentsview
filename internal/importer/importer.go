package importer

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/parser"
)

// ImportStats reports the outcome of an import operation.
type ImportStats struct {
	Imported int `json:"imported"`
	Updated  int `json:"updated"`
	Skipped  int `json:"skipped"`
	Errors   int `json:"errors"`
}

// ImportCallbacks provides optional progress reporting.
type ImportCallbacks struct {
	// OnProgress fires after each conversation with current
	// cumulative stats.
	OnProgress func(ImportStats)
	// OnIndexing fires before the FTS index rebuild starts.
	OnIndexing func()
}

func (c *ImportCallbacks) progress(s ImportStats) {
	if c != nil && c.OnProgress != nil {
		c.OnProgress(s)
	}
}

func (c *ImportCallbacks) indexing() {
	if c != nil && c.OnIndexing != nil {
		c.OnIndexing()
	}
}

// ftsSuspender is optionally implemented by stores that
// support dropping and rebuilding FTS indexes.
type ftsSuspender interface {
	DropFTS() error
	RebuildFTS() error
}

// lazyFTS suspends FTS triggers on first call to suspend()
// and rebuilds on restore(). If suspend() is never called
// (no message work happened), restore() is a no-op. This
// avoids the expensive FTS rebuild when re-importing an
// unchanged archive.
type lazyFTS struct {
	sus        ftsSuspender
	dropped    bool
	onIndexing func()
}

func newLazyFTS(
	store db.Store, onIndexing func(),
) *lazyFTS {
	s, ok := store.(ftsSuspender)
	if !ok || !store.HasFTS() {
		return nil
	}
	return &lazyFTS{sus: s, onIndexing: onIndexing}
}

func (f *lazyFTS) suspend() {
	if f == nil || f.dropped {
		return
	}
	if err := f.sus.DropFTS(); err != nil {
		log.Printf("import: drop FTS: %v", err)
		return
	}
	f.dropped = true
}

func (f *lazyFTS) restore() error {
	if f == nil || !f.dropped {
		return nil
	}
	if f.onIndexing != nil {
		f.onIndexing()
	}
	if err := f.sus.RebuildFTS(); err != nil {
		return fmt.Errorf("rebuilding FTS index: %w", err)
	}
	return nil
}

// ImportClaudeAI reads a Claude.ai conversations.json export
// and upserts each conversation into the store. Existing
// sessions are updated (messages replaced); user-renamed
// display names are preserved. Excluded (deleted) sessions
// are counted as skipped.
func ImportClaudeAI(
	ctx context.Context,
	store db.Store,
	r io.Reader,
	cb *ImportCallbacks,
	machine ...string,
) (stats ImportStats, retErr error) {
	fts := newLazyFTS(store, cb.indexing)
	defer func() {
		if err := fts.restore(); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()

	provider, ok := parser.NewProvider(
		parser.AgentClaudeAI, parser.ProviderConfig{},
	)
	if !ok {
		return stats, fmt.Errorf("claude.ai provider unavailable")
	}
	exporter, ok := provider.(parser.ClaudeAIExportParser)
	if !ok {
		return stats, fmt.Errorf(
			"claude.ai provider does not support exports",
		)
	}

	err := exporter.ParseClaudeAIExport(r, func(
		result parser.ParseResult,
	) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		result.Session.Machine = resolvedImportMachine(
			result.Session.Machine, machine,
		)
		status, err := upsertConversation(
			ctx, store, result, fts,
		)
		if err != nil {
			stats.Errors++
			log.Printf(
				"import: skipping %s: %v",
				result.Session.ID, err,
			)
			cb.progress(stats)
			return nil
		}

		switch status {
		case importNew:
			stats.Imported++
		case importUpdated:
			stats.Updated++
		case importSkipped:
			stats.Skipped++
		}

		cb.progress(stats)
		return nil
	})

	retErr = err
	return
}

type importStatus int

const (
	importNew importStatus = iota
	importUpdated
	importSkipped
)

func upsertConversation(
	ctx context.Context,
	store db.Store,
	result parser.ParseResult,
	fts *lazyFTS,
) (importStatus, error) {
	s := result.Session

	msgs := make([]db.Message, len(result.Messages))
	for i, m := range result.Messages {
		msgs[i] = db.Message{
			SessionID:     s.ID,
			Ordinal:       m.Ordinal,
			Role:          string(m.Role),
			Content:       m.Content,
			Timestamp:     m.Timestamp.UTC().Format(time.RFC3339Nano),
			ContentLength: m.ContentLength,
		}
	}

	existing, err := store.GetSession(ctx, s.ID)
	if err != nil {
		return importNew, fmt.Errorf("checking session: %w", err)
	}
	isNew := existing == nil

	sess := db.Session{
		ID:               s.ID,
		Project:          s.Project,
		Machine:          s.Machine,
		FirstMessage:     strPtr(s.FirstMessage),
		SessionName:      db.ParsedSessionName(s),
		StartedAt:        timeStr(s.StartedAt),
		EndedAt:          timeStr(s.EndedAt),
		MessageCount:     s.MessageCount,
		UserMessageCount: s.UserMessageCount,
	}
	db.ApplyParsedSessionIdentity(&sess, s)

	if err := store.UpsertSession(sess); err != nil {
		if errors.Is(err, db.ErrSessionExcluded) {
			return importSkipped, nil
		}
		return importNew, fmt.Errorf("upserting session: %w", err)
	}

	// Bump local_modified_at so incremental PG push picks up session_name
	// changes even when the skip path below returns importSkipped (message
	// count unchanged) and ReplaceSessionMessages is never called.
	if localDB, ok := store.(*db.DB); ok {
		if err := localDB.BumpLocalModifiedAt(s.ID); err != nil {
			log.Printf("import: bumping local_modified_at for %s: %v", s.ID, err)
		}
	}

	// Skip expensive message replacement when the conversation
	// has not changed since the last import. Compare both
	// message count and ended_at (source updated_at) to detect
	// content/metadata changes even when count is unchanged.
	if !isNew && existing != nil && existing.MessageCount == s.MessageCount {
		newEnd := timeStr(s.EndedAt)
		if ptrEqual(existing.EndedAt, newEnd) {
			existingMsgs, err := store.GetAllMessages(ctx, s.ID)
			if err != nil {
				return importNew,
					fmt.Errorf("loading existing messages: %w", err)
			}
			if sameMessages(existingMsgs, msgs) {
				return importSkipped, nil
			}
		}
	}

	// Suspend FTS before first message-changing operation to
	// avoid per-row trigger overhead during bulk work.
	fts.suspend()

	if err := store.ReplaceSessionMessages(s.ID, msgs); err != nil {
		return importNew, fmt.Errorf("replacing messages: %w", err)
	}

	if isNew {
		return importNew, nil
	}
	return importUpdated, nil
}

// assetResolverAdapter bridges the importer's AssetIndex / CopyAsset
// pair to the parser.AssetResolver interface.
type assetResolverAdapter struct {
	index     AssetIndex
	assetsDir string
}

func (a *assetResolverAdapter) Resolve(
	pointer string,
) (string, bool) {
	return a.index.Resolve(pointer)
}

func (a *assetResolverAdapter) Copy(
	srcPath string,
) (string, error) {
	return CopyAsset(srcPath, a.assetsDir)
}

// ImportChatGPT reads a ChatGPT export directory (containing
// conversations-*.json files) and imports each conversation into
// the store. Repeated exports append unseen messages while retaining
// previously archived branches and message revisions.
func ImportChatGPT(
	ctx context.Context,
	store db.Store,
	dir string,
	assetsDir string,
	cb *ImportCallbacks,
	machine ...string,
) (stats ImportStats, retErr error) {
	fts := newLazyFTS(store, cb.indexing)
	defer func() {
		if err := fts.restore(); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()

	index := BuildAssetIndex(dir)
	resolver := &assetResolverAdapter{
		index:     index,
		assetsDir: assetsDir,
	}

	provider, ok := parser.NewProvider(
		parser.AgentChatGPT, parser.ProviderConfig{},
	)
	if !ok {
		return stats, fmt.Errorf("chatgpt provider unavailable")
	}
	exporter, ok := provider.(parser.ChatGPTExportParser)
	if !ok {
		return stats, fmt.Errorf(
			"chatgpt provider does not support exports",
		)
	}

	err := exporter.ParseChatGPTExport(dir, resolver,
		func(result parser.ParseResult) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			s := result.Session
			s.Machine = resolvedImportMachine(s.Machine, machine)

			existing, err := store.GetSession(ctx, s.ID)
			if err != nil {
				stats.Errors++
				log.Printf(
					"import: skipping %s: %v", s.ID, err,
				)
				cb.progress(stats)
				return nil
			}

			sess := db.Session{
				ID:               s.ID,
				Project:          s.Project,
				Machine:          s.Machine,
				FirstMessage:     strPtr(s.FirstMessage),
				SessionName:      db.ParsedSessionName(s),
				StartedAt:        timeStr(s.StartedAt),
				EndedAt:          timeStr(s.EndedAt),
				MessageCount:     s.MessageCount,
				UserMessageCount: s.UserMessageCount,
			}
			db.ApplyParsedSessionIdentity(&sess, s)

			msgs := make([]db.Message, len(result.Messages))
			for i, m := range result.Messages {
				msgs[i] = db.Message{
					SessionID:  s.ID,
					SourceUUID: m.SourceUUID,
					Ordinal:    m.Ordinal,
					Role:       string(m.Role),
					Content:    m.Content,
					Timestamp: m.Timestamp.UTC().Format(
						time.RFC3339Nano,
					),
					HasThinking:   m.HasThinking,
					HasToolUse:    m.HasToolUse,
					ContentLength: m.ContentLength,
					IsSystem:      m.IsSystem,
					Model:         m.Model,
					ToolCalls: convertToolCalls(
						s.ID, m.ToolCalls,
					),
				}
			}

			if existing != nil {
				archived, err := store.GetAllMessages(ctx, s.ID)
				if err != nil {
					return fmt.Errorf("loading archived messages: %w", err)
				}
				msgs = mergeChatGPTMessages(archived, msgs)
				if len(msgs) == len(archived) {
					if localDB, ok := store.(*db.DB); ok {
						if err := localDB.RefreshSessionName(s.ID, sess.SessionName); err != nil {
							return err
						}
					}
					stats.Skipped++
					cb.progress(stats)
					return nil
				}
				name, end := sess.SessionName, sess.EndedAt
				sess = *existing
				sess.SessionName = name
				if end != nil && (sess.EndedAt == nil || *end > *sess.EndedAt) {
					sess.EndedAt = end
				}
			}
			sess.MessageCount = len(msgs)
			sess.UserMessageCount = 0
			for _, m := range msgs {
				if m.Role == "user" && !m.IsSystem {
					sess.UserMessageCount++
				}
			}
			fts.suspend()
			written, err := store.WriteSessionBatchAtomic([]db.SessionBatchWrite{{
				Session: sess, Messages: msgs, ReplaceMessages: true, SkipSignalUpdates: true,
			}})
			if errors.Is(err, db.ErrSessionExcluded) || written.ExcludedSessions > 0 {
				stats.Skipped++
			} else if err != nil {
				stats.Errors++
				log.Printf("import: skipping %s: %v", s.ID, err)
			} else if existing != nil {
				stats.Updated++
			} else {
				stats.Imported++
			}

			cb.progress(stats)
			return nil
		},
	)

	retErr = err
	return
}

// Preserve archive order, including branches absent from the latest export.
// Consume matches once so repeated equal messages remain distinct. Older
// imports have no source UUID; match those by their complete exported content.
func mergeChatGPTMessages(archived, incoming []db.Message) []db.Message {
	counts := make(map[string]int)
	for _, m := range archived {
		counts[chatGPTMessageKey(m, m.SourceUUID)]++
	}
	merged := append([]db.Message(nil), archived...)
	next := 0
	for _, m := range archived {
		if m.Ordinal >= next {
			next = m.Ordinal + 1
		}
	}
	for _, m := range incoming {
		db.SanitizeMessage(&m)
		key := chatGPTMessageKey(m, m.SourceUUID)
		legacy := chatGPTMessageKey(m, "")
		if counts[key] > 0 {
			counts[key]--
			continue
		}
		if m.SourceUUID != "" && counts[legacy] > 0 {
			counts[legacy]--
			continue
		}
		m.Ordinal = next
		next++
		merged = append(merged, m)
	}
	return merged
}

func chatGPTMessageKey(m db.Message, uuid string) string {
	m.ID, m.Ordinal = 0, 0
	m.SourceUUID = uuid
	// Tool database IDs and call indices are excluded by their JSON tags.
	b, _ := json.Marshal(m)
	return string(b)
}

func resolvedImportMachine(current string, override []string) string {
	if len(override) > 0 && strings.TrimSpace(override[0]) != "" {
		return override[0]
	}
	return current
}

func ptrEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func sameMessages(existing, incoming []db.Message) bool {
	if len(existing) != len(incoming) {
		return false
	}
	for i := range existing {
		if existing[i].Ordinal != incoming[i].Ordinal ||
			existing[i].Role != incoming[i].Role ||
			existing[i].Content != incoming[i].Content ||
			existing[i].Timestamp != incoming[i].Timestamp ||
			existing[i].ContentLength != incoming[i].ContentLength {
			return false
		}
	}
	return true
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func timeStr(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s
}

func convertToolCalls(
	sessionID string, parsed []parser.ParsedToolCall,
) []db.ToolCall {
	if len(parsed) == 0 {
		return nil
	}
	calls := make([]db.ToolCall, len(parsed))
	for i, tc := range parsed {
		filePath := tc.FilePath
		if filePath == "" {
			filePath = parser.ResolveFilePathFromJSON(tc.InputJSON)
		}
		calls[i] = db.ToolCall{
			SessionID: sessionID,
			ToolName:  tc.ToolName,
			Category:  tc.Category,
			ToolUseID: tc.ToolUseID,
			InputJSON: tc.InputJSON,
			FilePath:  filePath,
			SkillName: tc.SkillName,
		}
		// Map execution output from ResultEvents to
		// ResultContent for display in the UI.
		for _, ev := range tc.ResultEvents {
			if ev.Content != "" {
				calls[i].ResultContent = ev.Content
				calls[i].ResultContentLength = len(ev.Content)
				break
			}
		}
	}
	return calls
}
