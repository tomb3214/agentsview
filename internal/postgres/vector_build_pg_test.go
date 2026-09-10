//go:build pgtest

package postgres

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/db"
	avvec "go.kenn.io/agentsview/internal/vector"
	kitvec "go.kenn.io/kit/vector"
	"net/url"
	"testing"
	"time"
)

func TestCentralVectorBuildResumeDriftReuseAndOwnership(t *testing.T) {
	ctx := context.Background()
	syncer, local, pg := newVectorPushTestSync(t, testPGURL(t), "agentsview_central_build_test")
	seedVectorSession(t, local, "a")
	seedVectorSession(t, local, "b")
	_, err := syncer.Push(ctx, false, nil)
	require.NoError(t, err)
	_, err = pg.Exec(`ALTER TABLE vector_push_state ADD COLUMN IF NOT EXISTS source_revision TEXT`)
	require.NoError(t, err)
	gen := kitvec.Generation{Model: "test-model", Dimensions: 4, Params: map[string]string{"max_input_chars": "8192", "doc_unit_scheme": "run_v1", "chunk_overlap_chars": "1228"}}
	id, err := ensureVectorGeneration(ctx, pg, gen.Fingerprint(), gen.Model, gen.Dimensions)
	require.NoError(t, err)
	require.NoError(t, ensureVectorChunkTable(ctx, pg, id, 4))
	calls := 0
	encode := func(ctx context.Context, texts []string) ([][]float32, error) {
		calls++
		v := make([][]float32, len(texts))
		for i := range v {
			v[i] = []float32{1, 0, 0, 0}
		}
		return v, nil
	}
	o := VectorBuildOptions{Machine: "test-machine", Generation: gen, MaxSources: 1, MaxChunks: 10, BatchSize: 4, MaxInputChars: 8192, MaxSourceBytes: 1 << 20, Timeout: time.Minute, Encode: encode}
	first, err := BuildCentralVectors(ctx, pg, o)
	require.NoError(t, err)
	assert.Equal(t, 1, first.Published)
	next, err := BuildCentralVectors(ctx, pg, o)
	require.NoError(t, err)
	assert.Equal(t, 1, next.Published)
	noop, err := BuildCentralVectors(ctx, pg, o)
	require.NoError(t, err)
	assert.Zero(t, noop.Examined)
	assert.Equal(t, 2, calls)
	var owner, machine, project string
	require.NoError(t, pg.QueryRow(`SELECT owner_marker,machine,project FROM sessions WHERE id='a'`).Scan(&owner, &machine, &project))
	assert.NotEmpty(t, owner)
	assert.Equal(t, "test-machine", machine)
	assert.Equal(t, "proj", project)
	// Simulate a prior local publisher: current generation/hashes already exist,
	// but central relational checkpoint has not yet been installed.
	_, err = pg.Exec(`UPDATE vector_push_state SET source_revision=NULL`)
	require.NoError(t, err)
	reused, err := BuildCentralVectors(ctx, pg, o)
	require.NoError(t, err)
	assert.Equal(t, 1, reused.Reused)
	assert.Equal(t, 2, calls)
	reused, err = BuildCentralVectors(ctx, pg, o)
	require.NoError(t, err)
	assert.Equal(t, 1, reused.Reused)
	// A changed source invalidates its checkpoint. An interruption leaves its
	// previous complete vectors in place and next identical pass resumes it.
	_, err = pg.Exec(`UPDATE messages SET content='changed' WHERE session_id='a'; UPDATE sessions SET transcript_revision='2',updated_at=now() WHERE id='a'`)
	require.NoError(t, err)
	o.Encode = func(context.Context, []string) ([][]float32, error) { return nil, errors.New("interrupted") }
	failed, err := BuildCentralVectors(ctx, pg, o)
	require.ErrorContains(t, err, "interrupted")
	assert.Zero(t, failed.Published)
	o.Encode = func(c context.Context, texts []string) ([][]float32, error) {
		_, e := pg.Exec(`UPDATE sessions SET transcript_revision='3',updated_at=now() WHERE id='a'`)
		require.NoError(t, e)
		return encode(c, texts)
	}
	drifted, err := BuildCentralVectors(ctx, pg, o)
	require.NoError(t, err)
	assert.Equal(t, 1, drifted.Deferred)
	var body string
	require.NoError(t, pg.QueryRow(`SELECT content FROM vector_documents WHERE session_id='a'`).Scan(&body))
	assert.Equal(t, "a", body)
	o.Encode = encode
	resumed, err := BuildCentralVectors(ctx, pg, o)
	require.NoError(t, err)
	assert.Equal(t, 1, resumed.Published)
	require.NoError(t, pg.QueryRow(`SELECT content FROM vector_documents WHERE session_id='a'`).Scan(&body))
	assert.Equal(t, "changed", body)
	o.Machine = "another-source"
	foreign, err := BuildCentralVectors(ctx, pg, o)
	require.NoError(t, err)
	assert.Zero(t, foreign.Examined)
	// Duplicate worker lock is server-owned and automatically releasable.
	lock, err := pg.Conn(ctx)
	require.NoError(t, err)
	defer lock.Close()
	_, err = lock.ExecContext(ctx, `SELECT pg_advisory_lock(hashtextextended(current_schema() || ':central-message-vectors',0))`)
	require.NoError(t, err)
	_, err = BuildCentralVectors(ctx, pg, o)
	require.ErrorContains(t, err, "already running")
	_, err = lock.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtextextended(current_schema() || ':central-message-vectors',0))`)
	require.NoError(t, err)
	_, err = BuildCentralVectors(ctx, pg, o)
	require.NoError(t, err)
}

func TestCentralVectorGroupingMatchesArchive(t *testing.T) {
	ctx := context.Background()
	syncer, local, pg := newVectorPushTestSync(t, testPGURL(t), "agentsview_central_group_test")
	seedVectorSession(t, local, "group")
	require.NoError(t, local.InsertMessages([]db.Message{{SessionID: "group", Ordinal: 1, Role: "assistant", SourceUUID: "reply", Content: "First"}, {SessionID: "group", Ordinal: 2, Role: "assistant", SourceUUID: "reply-2", Content: "Second"}, {SessionID: "group", Ordinal: 3, Role: "user", Content: "next"}}))
	_, err := syncer.Push(ctx, false, nil)
	require.NoError(t, err)
	o := VectorBuildOptions{Machine: "test-machine", MaxInputChars: 8192, MaxSourceBytes: 1 << 20}
	docs, _, err := centralVectorSource(ctx, pg, "group", o)
	require.NoError(t, err)
	var units []db.EmbeddableUnit
	_, err = local.ScanEmbeddableUnits(ctx, "", false, func(u db.EmbeddableUnit) error { units = append(units, u); return nil })
	require.NoError(t, err)
	require.Len(t, docs, len(units))
	for i, u := range units {
		assert.Equal(t, u.Content, docs[i].Content)
		assert.Equal(t, avvec.DocKey(u.Kind, u.SessionID, u.SourceUUID, u.Ordinal, 1), docs[i].DocKey)
		assert.Equal(t, u.OrdinalEnd, docs[i].OrdinalEnd)
	}
}

func TestCentralVectorCheckpointMigrationPreservesState(t *testing.T) {
	ctx := context.Background()
	_, _, pg := newVectorPushTestSync(t, testPGURL(t), "agentsview_central_migrate_test")
	_, err := pg.Exec(`ALTER TABLE vector_push_state DROP COLUMN source_revision; INSERT INTO vector_push_state(generation_id,session_id,doc_agg_hash) VALUES(91,'retained','hash')`)
	require.NoError(t, err)
	require.NoError(t, ensureVectorSourceRevision(ctx, pg))
	require.NoError(t, ensureVectorSourceRevision(ctx, pg))
	var hash string
	require.NoError(t, pg.QueryRow(`SELECT doc_agg_hash FROM vector_push_state WHERE generation_id=91 AND session_id='retained'`).Scan(&hash))
	assert.Equal(t, "hash", hash)
}

func TestCentralVectorSourceRLSRemainsAuthoritative(t *testing.T) {
	ctx := context.Background()
	schema := "agentsview_central_rls_test"
	syncer, local, pg := newVectorPushTestSync(t, testPGURL(t), schema)
	seedVectorSession(t, local, "owned")
	seedVectorSession(t, local, "foreign")
	_, err := syncer.Push(ctx, false, nil)
	require.NoError(t, err)
	_, err = pg.Exec(`UPDATE sessions SET machine='foreign-machine' WHERE id='foreign'`)
	require.NoError(t, err)
	gen := kitvec.Generation{Model: "rls-model", Dimensions: 4, Params: map[string]string{"max_input_chars": "8192", "doc_unit_scheme": "run_v1", "chunk_overlap_chars": "1228"}}
	id, err := ensureVectorGeneration(ctx, pg, gen.Fingerprint(), gen.Model, 4)
	require.NoError(t, err)
	require.NoError(t, ensureVectorChunkTable(ctx, pg, id, 4))
	role := "agentsview_central_rls_writer"
	_, err = pg.Exec(`CREATE ROLE ` + role + ` NOLOGIN`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = pg.Exec(`DROP OWNED BY ` + role); _, _ = pg.Exec(`DROP ROLE ` + role) })
	_, err = pg.Exec(`GRANT USAGE ON SCHEMA ` + schema + ` TO ` + role + `; GRANT SELECT ON ALL TABLES IN SCHEMA ` + schema + ` TO ` + role + `; GRANT UPDATE ON sessions TO ` + role + `; GRANT INSERT,UPDATE,DELETE ON vector_documents,vector_push_state,vector_generation_machines,` + vectorChunkTable(id) + ` TO ` + role)
	require.NoError(t, err)
	for _, table := range []string{"sessions", "messages", "vector_documents", "vector_push_state", "vector_generation_machines", vectorChunkTable(id)} {
		predicate := "machine='test-machine'"
		if table == "messages" || table == "vector_documents" || table == "vector_push_state" {
			predicate = "session_id IN (SELECT id FROM sessions WHERE machine='test-machine')"
		}
		if table == vectorChunkTable(id) {
			predicate = "doc_key IN (SELECT doc_key FROM vector_documents WHERE session_id IN (SELECT id FROM sessions WHERE machine='test-machine'))"
		}
		_, err = pg.Exec(`ALTER TABLE ` + table + ` ENABLE ROW LEVEL SECURITY; CREATE POLICY source_only ON ` + table + ` TO ` + role + ` USING (` + predicate + `) WITH CHECK (` + predicate + `)`)
		require.NoError(t, err)
	}
	u, err := url.Parse(testPGURL(t))
	require.NoError(t, err)
	q := u.Query()
	q.Set("options", "-c role="+role)
	u.RawQuery = q.Encode()
	writer, err := Open(u.String(), schema, true)
	require.NoError(t, err)
	defer writer.Close()
	o := VectorBuildOptions{Machine: "test-machine", Generation: gen, MaxSources: 10, MaxChunks: 10, BatchSize: 4, MaxInputChars: 8192, MaxSourceBytes: 1 << 20, Timeout: time.Minute, Encode: func(_ context.Context, texts []string) ([][]float32, error) {
		v := make([][]float32, len(texts))
		for i := range v {
			v[i] = []float32{1, 0, 0, 0}
		}
		return v, nil
	}}
	result, err := BuildCentralVectors(ctx, writer, o)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Published)
	o.Machine = "foreign-machine"
	result, err = BuildCentralVectors(ctx, writer, o)
	require.NoError(t, err)
	assert.Zero(t, result.Examined)
	var machine string
	require.NoError(t, pg.QueryRow(`SELECT machine FROM sessions WHERE id='foreign'`).Scan(&machine))
	assert.Equal(t, "foreign-machine", machine)
}
