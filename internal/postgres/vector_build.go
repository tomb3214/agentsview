package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"go.kenn.io/agentsview/internal/db"
	avvec "go.kenn.io/agentsview/internal/vector"
	kitvec "go.kenn.io/kit/vector"
)

// VectorBuildOptions bounds one central pass. Machine is the original source
// identity, never the encoder host. Schema and generation must be provisioned.
type VectorBuildOptions struct {
	Machine                                                         string
	Generation                                                      kitvec.Generation
	MaxSources, MaxChunks, BatchSize, MaxInputChars, MaxSourceBytes int
	Timeout                                                         time.Duration
	IncludeAutomated                                                bool
	Encode                                                          kitvec.EncodeFunc
}

type VectorBuildResult struct {
	Examined, Published, Reused, Deferred, Chunks, Requests int
	BoundReached                                            bool
}

// sourceRevision includes metadata affecting grouping, scope and attribution.
// Normal relational publication commits transcript_revision and updated_at with
// messages. The session row is locked and this witness rechecked at install.
const vectorSourceRevision = `jsonb_build_array(s.transcript_revision,s.updated_at,s.owner_marker,s.machine,s.project,s.relationship_type,s.parent_session_id,s.deleted_at,s.is_automated)::text`

// BuildCentralVectors completes a bounded current-source pass. The connection
// advisory lock disappears when a worker dies; it is not a durable queue lease.
// Installed source_revision checkpoints and generation hashes survive restarts.
func BuildCentralVectors(ctx context.Context, pg *sql.DB, o VectorBuildOptions) (r VectorBuildResult, err error) {
	if o.Machine == "" || o.MaxSources < 1 || o.MaxChunks < 1 || o.BatchSize < 1 || o.MaxInputChars < 1 || o.MaxSourceBytes < 1 || o.Timeout <= 0 || o.Encode == nil {
		return r, errors.New("machine, positive bounds and encoder are required")
	}
	if o.Generation.Params["max_input_chars"] != strconv.Itoa(o.MaxInputChars) || o.Generation.Params["doc_unit_scheme"] != "run_v1" || o.Generation.Params["chunk_overlap_chars"] != strconv.Itoa(avvec.ChunkOverlap(o.MaxInputChars)) {
		return r, errors.New("chunk recipe differs from generation")
	}
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()
	id, dim, ok, err := LookupVectorGeneration(ctx, pg, o.Generation.Fingerprint())
	if err != nil {
		return r, err
	}
	if !ok || dim != o.Generation.Dimensions {
		return r, errors.New("matching vector generation must be provisioned before build")
	}
	// Probe the additive checkpoint contract before any inference or writes.
	rows, err := pg.QueryContext(ctx, `SELECT source_revision FROM vector_push_state LIMIT 0`)
	if err != nil {
		return r, fmt.Errorf("central vector checkpoint schema required: %w", err)
	}
	rows.Close()
	exists, err := VectorChunkTableExists(ctx, pg, id)
	if err != nil {
		return r, err
	}
	if !exists {
		return r, errors.New("generation chunk table must be provisioned")
	}
	ext, err := vectorExtensionSchema(ctx, pg)
	if err != nil {
		return r, err
	}
	lease, err := pg.Conn(ctx)
	if err != nil {
		return r, err
	}
	defer lease.Close()
	var locked bool
	if err = lease.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtextextended(current_schema() || ':central-message-vectors',0))`).Scan(&locked); err != nil {
		return r, err
	}
	if !locked {
		return r, errors.New("central vector producer already running")
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, e := lease.ExecContext(c, `SELECT pg_advisory_unlock(hashtextextended(current_schema() || ':central-message-vectors',0))`)
		if e != nil {
			err = errors.Join(err, e)
		}
	}()
	// One bounded metadata page; unchanged sessions never load message contents.
	rows, err = lease.QueryContext(ctx, `SELECT s.id FROM sessions s LEFT JOIN vector_push_state p ON p.session_id=s.id AND p.generation_id=$1 WHERE s.machine=$2 AND ($3 OR NOT s.is_automated) AND s.deleted_at IS NULL AND p.source_revision IS DISTINCT FROM `+vectorSourceRevision+` ORDER BY s.updated_at,s.id LIMIT $4`, id, o.Machine, o.IncludeAutomated, o.MaxSources)
	if err != nil {
		return r, err
	}
	var ids []string
	for rows.Next() {
		var v string
		if err = rows.Scan(&v); err != nil {
			rows.Close()
			return r, err
		}
		ids = append(ids, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	var schema string
	if err = lease.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return r, fmt.Errorf("resolving central vector schema: %w", err)
	}
	if schema == "" {
		return r, errors.New("central vector schema must not be empty")
	}
	syncer := &Sync{pg: pg, schema: schema}
	all, err := syncer.allVectorGenerationIDs(ctx)
	if err != nil {
		return r, err
	}
	gens, err := syncer.existingChunkGenerations(ctx, all)
	if err != nil {
		return r, err
	}
	for _, sid := range ids {
		if err = ctx.Err(); err != nil {
			return r, err
		}
		// A dead advisory-lock connection must never continue via another pool conn.
		if err = lease.PingContext(ctx); err != nil {
			return r, err
		}
		r.Examined++
		docs, rev, err := centralVectorSource(ctx, pg, sid, o)
		if err != nil {
			return r, err
		}
		if rev == "" {
			r.Deferred++
			continue
		}
		export := make([]avvec.ExportDoc, len(docs))
		for i, d := range docs {
			export[i] = avvec.ExportDoc{DocKey: d.DocKey, SourceUUID: d.SourceUUID, Ordinal: d.Ordinal, OrdinalEnd: d.OrdinalEnd, Subordinate: d.Subordinate, OffsetsJSON: d.OffsetsJSON, ContentHash: d.ContentHash}
		}
		hash := avvec.AggregateEmbeddedDocHash(export)
		reuse, err := centralVectorReusable(ctx, pg, id, sid, hash, docs, o)
		if err != nil {
			return r, err
		}
		if !reuse {
			needed := 0
			for _, d := range docs {
				needed += len(kitvec.Split(d.Content, kitvec.SplitOptions{MaxRunes: o.MaxInputChars, Overlap: avvec.ChunkOverlap(o.MaxInputChars)}))
			}
			if needed > o.MaxChunks {
				return r, errors.New("source exceeds max-chunks; increase explicit bound to admit it")
			}
			if needed > o.MaxChunks-r.Chunks {
				r.BoundReached = true
				break
			}
			// Fill batches across documents while retaining the source as the
			// atomic publication and revision-check boundary.
			type inputChunk struct {
				docIndex, chunkIndex int
				text                 string
			}
			inputs := make([]inputChunk, 0, needed)
			for i := range docs {
				parts := kitvec.Split(docs[i].Content, kitvec.SplitOptions{MaxRunes: o.MaxInputChars, Overlap: avvec.ChunkOverlap(o.MaxInputChars)})
				for _, part := range parts {
					inputs = append(inputs, inputChunk{i, part.Index, part.Text})
				}
			}
			for start := 0; start < len(inputs); start += o.BatchSize {
				end := min(start+o.BatchSize, len(inputs))
				texts := make([]string, end-start)
				for j, input := range inputs[start:end] {
					texts[j] = input.text
				}
				r.Requests++
				vectors, e := o.Encode(ctx, texts)
				if e != nil {
					return r, e
				}
				if len(vectors) != len(texts) {
					return r, errors.New("encoder returned wrong vector count")
				}
				for j, v := range vectors {
					if len(v) != dim {
						return r, errors.New("encoder returned wrong vector dimension")
					}
					norm := 0.0
					for _, x := range v {
						norm += float64(x) * float64(x)
					}
					if norm == 0 || math.IsInf(norm, 0) || math.IsNaN(norm) {
						return r, errors.New("encoder returned invalid vector norm")
					}
					if _, e = halfvecLiteral(v); e != nil {
						return r, e
					}
					input := inputs[start+j]
					docs[input.docIndex].Chunks = append(docs[input.docIndex].Chunks, VectorPushChunk{ChunkIndex: input.chunkIndex, Embedding: v})
					r.Chunks++
				}
			}
		}
		// Use the lease connection for commit, preventing publication after lock loss.
		applied, e := installCentralVectors(ctx, lease, vectorGeneration{id: id, halfvecType: ext + ".halfvec"}, gens, sid, rev, hash, docs, reuse, o)
		if e != nil {
			return r, e
		}
		if !applied {
			r.Deferred++
		} else if reuse {
			r.Reused++
		} else {
			r.Published++
		}
	}
	return r, nil
}

func centralVectorSource(ctx context.Context, pg *sql.DB, sid string, o VectorBuildOptions) (docs []VectorPushDoc, rev string, err error) {
	tx, err := pg.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = tx.Rollback() }()
	err = tx.QueryRowContext(ctx, `SELECT `+vectorSourceRevision+` FROM sessions s WHERE s.id=$1 AND s.machine=$2 AND s.deleted_at IS NULL AND ($3 OR NOT s.is_automated)`, sid, o.Machine, o.IncludeAutomated).Scan(&rev)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	var sourceBytes int64
	if err = tx.QueryRowContext(ctx, `SELECT coalesce(sum(octet_length(content)),0) FROM messages WHERE session_id=$1`, sid).Scan(&sourceBytes); err != nil {
		return nil, "", err
	}
	if sourceBytes > int64(o.MaxSourceBytes) {
		return nil, "", errors.New("source exceeds max-source-bytes; increase explicit bound to admit it")
	}
	rows, err := tx.QueryContext(ctx, `SELECT m.session_id,m.role,m.source_uuid,m.ordinal,m.content,m.is_sidechain,s.relationship_type,s.parent_session_id,s.ended_at::text FROM messages m JOIN sessions s ON s.id=m.session_id WHERE m.session_id=$1 AND m.role IN ('user','assistant') AND NOT m.is_system AND `+db.PostgresSystemPrefixSQL("m.content", "m.role")+` ORDER BY m.session_id,m.ordinal`, sid)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	seen := map[string]int{}
	err = db.ReduceEmbeddingRows(rows, func(u db.EmbeddableUnit) error {
		occurrence := 1
		if u.SourceUUID != "" {
			seen[u.SourceUUID]++
			occurrence = seen[u.SourceUUID]
		}
		if len(kitvec.Split(u.Content, kitvec.SplitOptions{MaxRunes: o.MaxInputChars, Overlap: avvec.ChunkOverlap(o.MaxInputChars)})) == 0 {
			return nil
		}
		offsets := "[]"
		if u.Offsets != nil {
			v, e := json.Marshal(u.Offsets)
			if e != nil {
				return e
			}
			offsets = string(v)
		}
		sum := sha256.Sum256([]byte(u.Content))
		docs = append(docs, VectorPushDoc{DocKey: avvec.DocKey(u.Kind, u.SessionID, u.SourceUUID, u.Ordinal, occurrence), SessionID: u.SessionID, SourceUUID: u.SourceUUID, Ordinal: u.Ordinal, OrdinalEnd: u.OrdinalEnd, Subordinate: u.Subordinate, OffsetsJSON: offsets, Content: u.Content, ContentHash: hex.EncodeToString(sum[:])})
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return docs, rev, tx.Commit()
}

// Existing publication aggregate proves the generation's content identity;
// shared vector_documents hashes alone cannot prove a different generation.
type vectorBuildReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func centralVectorReusable(ctx context.Context, pg vectorBuildReader, id int64, sid, hash string, docs []VectorPushDoc, o VectorBuildOptions) (bool, error) {
	var prior string
	err := pg.QueryRowContext(ctx, `SELECT doc_agg_hash FROM vector_push_state WHERE generation_id=$1 AND session_id=$2`, id, sid).Scan(&prior)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if prior != hash {
		return false, nil
	}
	expected := map[string]map[int]bool{}
	identities := map[string]VectorPushDoc{}
	for _, d := range docs {
		expected[d.DocKey] = map[int]bool{}
		identities[d.DocKey] = d
		for _, p := range kitvec.Split(d.Content, kitvec.SplitOptions{MaxRunes: o.MaxInputChars, Overlap: avvec.ChunkOverlap(o.MaxInputChars)}) {
			expected[d.DocKey][p.Index] = true
		}
	}
	rows, err := pg.QueryContext(ctx, fmt.Sprintf(`SELECT c.doc_key,c.chunk_index,d.content_hash,d.source_uuid,d.ordinal,d.ordinal_end,d.subordinate,d.offsets FROM %s c JOIN vector_documents d ON d.doc_key=c.doc_key WHERE d.session_id=$1`, vectorChunkTable(id)), sid)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var index int
		var stored VectorPushDoc
		if err = rows.Scan(&key, &index, &stored.ContentHash, &stored.SourceUUID, &stored.Ordinal, &stored.OrdinalEnd, &stored.Subordinate, &stored.OffsetsJSON); err != nil {
			return false, err
		}
		want := identities[key]
		if stored.ContentHash != want.ContentHash || stored.SourceUUID != want.SourceUUID || stored.Ordinal != want.Ordinal || stored.OrdinalEnd != want.OrdinalEnd || stored.Subordinate != want.Subordinate || stored.OffsetsJSON != want.OffsetsJSON {
			return false, nil
		}
		if !expected[key][index] {
			return false, nil
		}
		delete(expected[key], index)
	}
	if err = rows.Err(); err != nil {
		return false, err
	}
	for _, indices := range expected {
		if len(indices) > 0 {
			return false, nil
		}
	}
	return true, nil
}

func installCentralVectors(ctx context.Context, conn *sql.Conn, gen vectorGeneration, gens []int64, sid, rev, hash string, docs []VectorPushDoc, reuse bool, o VectorBuildOptions) (bool, error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var current string
	err = tx.QueryRowContext(ctx, `SELECT `+vectorSourceRevision+` FROM sessions s WHERE s.id=$1 AND s.machine=$2 AND s.deleted_at IS NULL AND ($3 OR NOT s.is_automated) FOR UPDATE`, sid, o.Machine, o.IncludeAutomated).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current != rev {
		return false, nil
	}
	if reuse {
		valid, e := centralVectorReusable(ctx, tx, gen.id, sid, hash, docs, o)
		if e != nil {
			return false, e
		}
		if !valid {
			return false, nil
		}
	}
	if !reuse {
		if err = parkSessionVectorDocs(ctx, tx, sid); err != nil {
			return false, err
		}
		if err = upsertVectorDocs(ctx, tx, docs); err != nil {
			return false, err
		}
		if _, err = replaceVectorChunks(ctx, tx, gen, sid, docs); err != nil {
			return false, err
		}
		if _, err = deleteParkedVectorDocs(ctx, tx, sid, gens); err != nil {
			return false, err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO vector_push_state(generation_id,session_id,doc_agg_hash,source_revision) VALUES($1,$2,$3,$4) ON CONFLICT(generation_id,session_id) DO UPDATE SET doc_agg_hash=excluded.doc_agg_hash,source_revision=excluded.source_revision`, gen.id, sid, hash, rev)
	if err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO vector_generation_machines(generation_id,machine) VALUES($1,$2) ON CONFLICT(generation_id,machine) DO UPDATE SET last_push_at=now()`, gen.id, o.Machine)
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
}
