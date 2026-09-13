package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	avvec "go.kenn.io/agentsview/internal/vector"
	kitvec "go.kenn.io/kit/vector"
)

type centralInputChunk struct {
	docIndex, chunkIndex int
	text                 string
}

type centralChunkKey struct {
	docKey     string
	chunkIndex int
}

// Staging is scoped to one source revision and generation, outside all search
// tables. The lease connection owns every write, including each short batch
// transaction. Completed batches survive cancellation of a later request.
func stageCentralVectorChunks(ctx context.Context, conn *sql.Conn, id int64, sid, rev, hash string, docs []VectorPushDoc, o VectorBuildOptions, budgetEnd time.Time, r *VectorBuildResult) (bool, error) {
	if _, err := conn.ExecContext(ctx, `DELETE FROM vector_build_chunks WHERE generation_id=$1 AND session_id=$2 AND (source_revision<>$3 OR doc_agg_hash<>$4)`, id, sid, rev, hash); err != nil {
		return false, err
	}
	rows, err := conn.QueryContext(ctx, `SELECT doc_key,chunk_index,embedding FROM vector_build_chunks WHERE generation_id=$1 AND session_id=$2 AND source_revision=$3 AND doc_agg_hash=$4`, id, sid, rev, hash)
	if err != nil {
		return false, err
	}
	cached := map[centralChunkKey][]float32{}
	for rows.Next() {
		var key centralChunkKey
		var raw string
		if err = rows.Scan(&key.docKey, &key.chunkIndex, &raw); err != nil {
			break
		}
		var v []float32
		if err = json.Unmarshal([]byte(raw), &v); err != nil {
			break
		}
		if _, err = centralVectorLiteral(v, o.Generation.Dimensions); err != nil {
			break
		}
		cached[key] = v
	}
	err = errors.Join(err, rows.Err())
	rows.Close()
	if err != nil {
		return false, err
	}
	var inputs []centralInputChunk
	for i := range docs {
		parts := kitvec.Split(docs[i].Content, kitvec.SplitOptions{MaxRunes: o.MaxInputChars, Overlap: avvec.ChunkOverlap(o.MaxInputChars)})
		for _, part := range parts {
			if v, ok := cached[centralChunkKey{docs[i].DocKey, part.Index}]; ok {
				docs[i].Chunks = append(docs[i].Chunks, VectorPushChunk{ChunkIndex: part.Index, Embedding: v})
				r.CachedChunks++
			} else {
				inputs = append(inputs, centralInputChunk{i, part.Index, part.Text})
			}
		}
	}
	for start := 0; start < len(inputs); start += o.BatchSize {
		if r.Chunks > 0 && (r.Chunks >= o.MaxChunks || !time.Now().Before(budgetEnd)) {
			r.BoundReached = true
			return false, nil
		}
		batch := inputs[start:min(start+o.BatchSize, len(inputs))]
		texts := make([]string, len(batch))
		for i, input := range batch {
			texts[i] = input.text
		}
		r.Requests++
		vectors, err := o.Encode(ctx, texts)
		if err != nil {
			return false, err
		}
		if len(vectors) != len(batch) {
			return false, errors.New("encoder returned wrong vector count")
		}
		literals := make([]string, len(vectors))
		for i, v := range vectors {
			if literals[i], err = centralVectorLiteral(v, o.Generation.Dimensions); err != nil {
				return false, err
			}
		}
		stored, err := storeCentralVectorBatch(ctx, conn, id, sid, rev, hash, docs, batch, literals, o)
		if err != nil || !stored {
			return false, err
		}
		for i, input := range batch {
			docs[input.docIndex].Chunks = append(docs[input.docIndex].Chunks, VectorPushChunk{ChunkIndex: input.chunkIndex, Embedding: vectors[i]})
		}
		r.Chunks += len(batch)
		r.StagedChunks += len(batch)
	}
	return true, nil
}

func centralVectorLiteral(v []float32, dim int) (string, error) {
	if len(v) != dim {
		return "", errors.New("encoder returned wrong vector dimension")
	}
	norm := 0.0
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm == 0 || math.IsInf(norm, 0) || math.IsNaN(norm) {
		return "", errors.New("encoder returned invalid vector norm")
	}
	return halfvecLiteral(v)
}

func storeCentralVectorBatch(ctx context.Context, conn *sql.Conn, id int64, sid, rev, hash string, docs []VectorPushDoc, batch []centralInputChunk, literals []string, o VectorBuildOptions) (bool, error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var current string
	err = tx.QueryRowContext(ctx, `SELECT `+vectorSourceRevision+` FROM sessions s WHERE s.id=$1 AND s.machine=$2 AND s.deleted_at IS NULL AND ($3 OR NOT s.is_automated) FOR SHARE`, sid, o.Machine, o.IncludeAutomated).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current != rev {
		return false, nil
	}
	for i, input := range batch {
		_, err = tx.ExecContext(ctx, `INSERT INTO vector_build_chunks(generation_id,session_id,source_revision,doc_agg_hash,doc_key,chunk_index,embedding) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(generation_id,session_id,doc_key,chunk_index) DO UPDATE SET source_revision=excluded.source_revision,doc_agg_hash=excluded.doc_agg_hash,embedding=excluded.embedding`, id, sid, rev, hash, docs[input.docIndex].DocKey, input.chunkIndex, literals[i])
		if err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}
