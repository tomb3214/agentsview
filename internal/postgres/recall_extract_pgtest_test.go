//go:build pgtest

package postgres

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/recall/extract"
)

func recallExtractFixture(t *testing.T, schema string) (*RecallExtractStore, *sql.DB) {
	t.Helper()
	pgURL := testPGURL(t)
	cleanNamedPGSchema(t, pgURL, schema)
	t.Cleanup(func() { cleanNamedPGSchema(t, pgURL, schema) })
	pg, err := Open(pgURL, schema, true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pg.Close()) })
	require.NoError(t, EnsureSchema(context.Background(), pg, schema))
	_, err = pg.Exec(`INSERT INTO sessions(id,machine,project,agent,ended_at,message_count,transcript_revision)
		VALUES('source','device','project','codex',now()-interval '1 hour',2,'1'),
		('other','other-device','project','codex',now()-interval '1 hour',2,'1');
		INSERT INTO messages(session_id,ordinal,role,content,source_uuid)
		VALUES('source',0,'user','Choose a storage format','user-uuid'),
		('source',1,'assistant','Use the established format','assistant-uuid'),
		('other',0,'user','Another machine','other-user'),('other',1,'assistant','Other answer','other-assistant')`)
	require.NoError(t, err)
	store, err := NewRecallExtractStore(pg, "device")
	require.NoError(t, err)
	return store, pg
}

func pgRecallManager(t *testing.T, store extract.Store, endpoint string) *extract.Manager {
	t.Helper()
	manager, err := extract.NewManager(extract.ManagerConfig{
		DB: store, Concurrency: 8,
		Client:    &extract.Client{BaseURL: endpoint, Model: "test-model", Request: extract.RequestShape{MaxTokens: 100}},
		Segmenter: extract.TurnsV1{MaxWindowChars: 50000},
		Prompts:   map[extract.PromptRole]string{extract.RoleIntent: "intent prompt", extract.RoleAction: "action prompt"},
		Identity:  extract.ModelIdentity{Model: "test-model"}, QuietPeriod: 30 * time.Minute,
		MaxAttempts: 1, FailureBackoff: time.Nanosecond,
	})
	require.NoError(t, err)
	return manager
}

func pgRecallResponse(w http.ResponseWriter) {
	_ = json.MarshalWrite(w, map[string]any{"choices": []any{map[string]any{
		"finish_reason": "stop", "message": map[string]string{"role": "assistant",
			"content": `{"entries":[{"type":"decision","title":"Storage","body":"Use the established format","entities":[]}]}`},
	}}})
}

func TestPGRecallExtractionResumeActivationAndNoop(t *testing.T) {
	store, pg := recallExtractFixture(t, "agentsview_recall_resume_test")
	ctx := context.Background()
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 2 {
			http.Error(w, "temporary endpoint failure", http.StatusServiceUnavailable)
			return
		}
		pgRecallResponse(w)
	}))
	t.Cleanup(endpoint.Close)
	manager := pgRecallManager(t, store, endpoint.URL)
	_, _ = manager.RunPass(ctx, extract.PassOptions{Full: true})
	progress, found, err := store.ExtractProgress(ctx, "source", manager.Fingerprint())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, progress.UnitCursor)
	require.Equal(t, int32(2), calls.Load())
	var staged int
	require.NoError(t, pg.QueryRow(`SELECT count(*) FROM recall_entries WHERE status='archived'`).Scan(&staged))
	assert.Equal(t, 1, staged)

	// A fresh manager resumes the accepted first unit after an endpoint failure.
	manager = pgRecallManager(t, store, endpoint.URL)
	result, err := manager.RunPass(ctx, extract.PassOptions{Full: true})
	require.NoError(t, err)
	assert.Equal(t, 1, result.Units)
	assert.True(t, result.Activated)
	assert.Equal(t, int32(3), calls.Load())
	stats, err := store.ExtractProgressStats(ctx, manager.Fingerprint())
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Done)
	assert.Equal(t, 2, stats.Entries)
	var accepted, validEvidence int
	require.NoError(t, pg.QueryRow(`SELECT count(*) FROM recall_entries WHERE machine='device' AND status='accepted' AND provenance_ok`).Scan(&accepted))
	require.NoError(t, pg.QueryRow(`SELECT count(*) FROM recall_evidence WHERE content_digest<>'' AND message_start_source_uuid<>''`).Scan(&validEvidence))
	assert.Equal(t, 2, accepted)
	assert.Equal(t, 2, validEvidence)
	_, err = manager.RunPass(ctx, extract.PassOptions{Full: true})
	require.NoError(t, err)
	assert.Equal(t, int32(3), calls.Load(), "a no-op pass must not call the model")
	other, err := store.GetSessionFull(ctx, "other")
	require.NoError(t, err)
	assert.Nil(t, other)
	assert.ErrorIs(t, store.RetireExtractGeneration(ctx, manager.Fingerprint(), false), db.ErrExtractGenerationActive)
	assert.ErrorIs(t, store.RetireExtractGeneration(ctx, "missing", false), db.ErrExtractGenerationNotFound)
	require.NoError(t, store.RetireExtractGeneration(ctx, manager.Fingerprint(), true))
}

func TestPGRecallExtractionRejectsChangedSourceAndCursor(t *testing.T) {
	store, pg := recallExtractFixture(t, "agentsview_recall_guards_test")
	ctx := context.Background()
	_, err := store.EnsureExtractGeneration(ctx, db.ExtractGeneration{Fingerprint: "generation", Model: "model", Segmenter: "turns-v1"})
	require.NoError(t, err)
	snapshot, err := store.GetSessionFull(ctx, "source")
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	_, err = store.UpsertExtractProgress(ctx, db.ExtractProgressUpsert{
		SessionID: "source", Fingerprint: "generation", ContentDigest: "digest", UnitsTotal: 2, StampedAt: time.Now(), Session: snapshot,
	})
	require.NoError(t, err)
	unit := db.ExtractUnitCommit{SessionID: "source", Fingerprint: "generation", Digest: "digest", Cursor: 0,
		MessageCount: snapshot.MessageCount, TranscriptRevision: snapshot.TranscriptRevision, LocalModifiedAt: snapshot.LocalModifiedAt, EndedAt: snapshot.EndedAt,
		Entries: []db.RecallEntry{{ID: "entry", Type: "decision", Scope: "project", ReviewState: "unreviewed_auto", Title: "Storage", Body: "Use the format",
			SourceSessionID: "source", SourceRunID: "generation", ProvenanceOK: true,
			Evidence: []db.RecallEvidence{{SessionID: "source", MessageStartOrdinal: 0, MessageEndOrdinal: 0}}}},
	}
	_, err = pg.Exec(`UPDATE sessions SET project='moved',updated_at=clock_timestamp() WHERE id='source'`)
	require.NoError(t, err)
	_, err = store.CommitExtractedUnit(ctx, unit)
	assert.ErrorIs(t, err, db.ErrExtractSessionDrifted)
	var entries int
	require.NoError(t, pg.QueryRow(`SELECT count(*) FROM recall_entries`).Scan(&entries))
	assert.Zero(t, entries)
	current, err := store.GetSessionFull(ctx, "source")
	require.NoError(t, err)
	unit.LocalModifiedAt = current.LocalModifiedAt
	n, err := store.CommitExtractedUnit(ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	_, err = store.CommitExtractedUnit(ctx, unit)
	assert.ErrorIs(t, err, db.ErrStaleExtractProgress)
	assert.ErrorIs(t, store.ActivateExtractGeneration(ctx, "generation", nil, time.Now()), db.ErrExtractActivationBlocked)
	other, err := NewRecallExtractStore(pg, "other-device")
	require.NoError(t, err)
	_, err = other.CommitExtractedUnit(ctx, unit)
	assert.ErrorIs(t, err, db.ErrExtractSessionDrifted)
}

func TestPGRecallExtractionMetadataRefreshAndRetraction(t *testing.T) {
	store, pg := recallExtractFixture(t, "agentsview_recall_refresh_test")
	ctx := context.Background()
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		pgRecallResponse(w)
	}))
	t.Cleanup(endpoint.Close)
	manager := pgRecallManager(t, store, endpoint.URL)
	_, err := manager.RunPass(ctx, extract.PassOptions{Full: true})
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
	// The source transaction timestamp can predate the discovery watermark.
	// Exact source stamps still detect the change and repair context/provenance
	// without repeating any model work.
	_, err = pg.Exec(`UPDATE sessions SET project='moved',updated_at=now()-interval '2 hours' WHERE id='source';
		UPDATE recall_entries SET provenance_ok=FALSE; UPDATE recall_evidence SET content_digest='old'`)
	require.NoError(t, err)
	_, err = manager.RunPass(ctx, extract.PassOptions{Full: true})
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
	var repaired int
	require.NoError(t, pg.QueryRow(`SELECT count(*) FROM recall_entries WHERE project='moved' AND provenance_ok`).Scan(&repaired))
	assert.Equal(t, 2, repaired)
	_, err = pg.Exec(`UPDATE sessions SET is_automated=TRUE,updated_at=now()-interval '3 hours' WHERE id='source'`)
	require.NoError(t, err)
	_, err = manager.RunPass(ctx, extract.PassOptions{})
	require.NoError(t, err)
	stats, err := store.ExtractProgressStats(ctx, manager.Fingerprint())
	require.NoError(t, err)
	assert.Zero(t, stats.Entries)
	assert.Zero(t, stats.Done)
}

func TestPGRecallExtractionEvidenceMatchesSQLite(t *testing.T) {
	_, pg := recallExtractFixture(t, "agentsview_recall_evidence_test")
	ctx := context.Background()
	local := testDB(t)
	require.NoError(t, local.UpsertSession(db.Session{ID: "source", Machine: "device", Project: "project", Agent: "codex", MessageCount: 2}))
	require.NoError(t, local.InsertMessages([]db.Message{
		{SessionID: "source", Ordinal: 0, Role: "user", Content: "Choose a storage format", SourceUUID: "user-uuid"},
		{SessionID: "source", Ordinal: 1, Role: "assistant", Content: "Use the established format", SourceUUID: "assistant-uuid",
			ToolCalls: []db.ToolCall{{ToolName: "read", Category: "read", ToolUseID: "tool-1", InputJSON: `{"path":"format.txt"}`, ResultContent: "format", ResultContentLength: 6}}},
	}))
	_, err := pg.Exec(`INSERT INTO tool_calls(session_id,message_ordinal,tool_name,category,tool_use_id,input_json,result_content,result_content_length)
		VALUES('source',1,'read','read','tool-1','{"path":"format.txt"}','format',6)`)
	require.NoError(t, err)
	window, err := local.BuildRecallEvidenceWindow(ctx, "source", 0, 1)
	require.NoError(t, err)
	expected, err := window.BindSelection(db.RecallEvidenceSelection{MessageStartOrdinal: 0, MessageEndOrdinal: 1})
	require.NoError(t, err)
	tx, err := pg.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback()) }()
	actual, err := pgExtractEvidence(ctx, tx, "source", 0, 1)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}
