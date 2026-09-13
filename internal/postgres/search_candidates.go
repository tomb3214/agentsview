package postgres

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"fmt"
	"time"
	"unicode/utf8"

	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/secrets"
	"go.kenn.io/agentsview/internal/vector"
	kitvec "go.kenn.io/kit/vector"
)

// searchCandidatesPG deliberately does not fuse or rerank: the caller merges
// these two bounded rankings with other authorized sources before one rerank.
// It requires the separately commissioned native BM25 index. Ordinary local
// and PostgreSQL search modes retain their existing response contract.
func (s *Store) searchCandidatesPG(ctx context.Context, f db.ContentSearchFilter) (db.ContentSearchPage, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	v, ok := s.getVectorSearcher().(*vectorSearcher)
	if !ok {
		return db.ContentSearchPage{}, s.semanticUnavailableError()
	}
	if v.maxInputChars <= 0 || v.maxInputChars > 8192 {
		return db.ContentSearchPage{}, fmt.Errorf("candidate passage recipe must use 1..8192 runes per chunk")
	}
	if f.Pattern == "" || len(f.Pattern) > 2000 {
		return db.ContentSearchPage{}, &db.SearchInputError{Msg: "candidate query must be 1..2000 bytes"}
	}
	vec, err := v.encode(ctx, f.Pattern)
	if err != nil {
		return db.ContentSearchPage{}, fmt.Errorf("%w: %v", db.ErrSemanticTransient, err)
	}
	if len(vec) != v.dimension {
		return db.ContentSearchPage{}, fmt.Errorf("query dimension does not match selected generation")
	}
	literal, err := halfvecLiteral(vec)
	if err != nil {
		return db.ContentSearchPage{}, err
	}
	ext, err := v.resolveExtSchema(ctx)
	if err != nil {
		return db.ContentSearchPage{}, err
	}
	tx, err := s.pg.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return db.ContentSearchPage{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if err = tuneHNSWRecall(ctx, tx, 100); err != nil {
		return db.ContentSearchPage{}, err
	}
	where, args := buildPGSessionBaseFilter(semanticPGSessionFilter(f))
	scope := "d.ordinal >= 0 AND d.session_id IN (SELECT id FROM sessions WHERE " + where + ")"
	if f.Scope == "top" {
		scope += " AND NOT d.subordinate"
	}
	if f.Scope == "subordinate" {
		scope += " AND d.subordinate"
	}
	fields := "d.doc_key,d.session_id,d.content_hash,d.ordinal,d.ordinal_end,d.content,s.machine,s.project,s.agent"
	from := " FROM vector_documents d JOIN sessions s ON s.id=d.session_id "
	param := fmt.Sprintf("$%d", len(args)+1)
	// Select qualified keys before highlighting so full passage extraction is
	// bounded by the ranked page. Both reads share this repeatable-read snapshot.
	keyword := "SELECT d.doc_key FROM vector_documents d WHERE " + scope +
		" AND EXISTS (SELECT 1 FROM " + v.chunkTable + " c WHERE c.doc_key=d.doc_key) AND d.content ||| " + param +
		" ORDER BY pdb.score(d.doc_key) DESC,d.doc_key COLLATE \"C\" LIMIT 100"
	keys, err := scanKeywordCandidateKeys(ctx, tx, keyword, append(append([]any{}, args...), f.Pattern))
	if err != nil {
		return db.ContentSearchPage{}, fmt.Errorf("BM25 candidate keys: %w", err)
	}
	lexical := []db.SearchCandidate{}
	if len(keys) > 0 {
		passages := "SELECT " + fields + ",to_json(pdb.snippet_positions(d.content,1))::text" + from +
			"WHERE d.doc_key=ANY($2) AND d.content ||| $1 ORDER BY array_position($2,d.doc_key)"
		lexical, err = scanCandidateRows(ctx, tx, passages, []any{f.Pattern, keys}, v.maxInputChars, true)
		if err != nil {
			return db.ContentSearchPage{}, fmt.Errorf("BM25 candidate passages: %w", err)
		}
	}

	distance := "c.embedding OPERATOR(" + ext + ".<=>) " + param + "::" + ext + ".halfvec"
	semantic := "SELECT " + fields + ",c.chunk_index" + from + " JOIN " + v.chunkTable + " c ON c.doc_key=d.doc_key WHERE " + scope +
		" ORDER BY " + distance + ",d.doc_key COLLATE \"C\",c.chunk_index LIMIT 100"
	vectors, err := scanCandidateRows(ctx, tx, semantic, append(append([]any{}, args...), literal), v.maxInputChars, false)
	if err != nil {
		return db.ContentSearchPage{}, fmt.Errorf("vector candidates: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return db.ContentSearchPage{}, err
	}
	return db.ContentSearchPage{Matches: []db.ContentMatch{}, Rankings: [][]db.SearchCandidate{lexical, vectors}, Generation: v.genID, LexicalMethod: "bm25"}, nil
}

func scanKeywordCandidateKeys(ctx context.Context, tx *sql.Tx, query string, args []any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := make([]string, 0, 100)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func scanCandidateRows(ctx context.Context, tx *sql.Tx, query string, args []any, maxRunes int, keyword bool) ([]db.SearchCandidate, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]db.SearchCandidate, 0, 100)
	for rows.Next() {
		var c db.SearchCandidate
		var content string
		var positions string
		last := any(&c.ChunkIndex)
		if keyword {
			last = &positions
		}
		if err = rows.Scan(&c.DocKey, &c.SessionID, &c.ContentHash, &c.OrdinalRange[0], &c.OrdinalRange[1], &content, &c.Machine, &c.Project, &c.Agent, last); err != nil {
			return nil, err
		}
		if keyword {
			var spans [][]int
			if err = json.Unmarshal([]byte(positions), &spans); err != nil {
				return nil, fmt.Errorf("BM25 positions: %w", err)
			}
			if len(spans) == 0 || len(spans[0]) != 2 {
				continue
			}
			c.ChunkIndex = candidateChunkAt(content, spans[0][0], maxRunes)
		}
		c.Text = candidateChunkText(content, c.ChunkIndex, maxRunes)
		if c.Text != "" {
			out = append(out, c)
		}
	}
	return out, rows.Err()
}

// Native BM25 offsets are byte offsets. Resolve them onto the unchanged
// embedding recipe's rune windows, including omitted blank-window indexes.
func candidateChunkAt(content string, byteOffset, maxRunes int) int {
	if byteOffset < 0 || byteOffset >= len(content) {
		return -1
	}
	if maxRunes <= 0 {
		return 0
	}
	position := utf8.RuneCountInString(content[:byteOffset])
	for _, chunk := range kitvec.Split(content, kitvec.SplitOptions{MaxRunes: maxRunes, Overlap: vector.ChunkOverlap(maxRunes)}) {
		start := chunk.Index * (maxRunes - vector.ChunkOverlap(maxRunes))
		if position >= start && position < start+utf8.RuneCountInString(chunk.Text) {
			return chunk.Index
		}
	}
	return -1
}

func candidateChunkText(content string, index, maxRunes int) string {
	if index < 0 {
		return ""
	}
	options := kitvec.SplitOptions{MaxRunes: maxRunes, Overlap: vector.ChunkOverlap(maxRunes)}
	for _, chunk := range kitvec.Split(content, options) {
		if chunk.Index != index {
			continue
		}
		// Scan the complete source before masking the selected window so a secret
		// crossing either boundary cannot become visible in a reranker request.
		start := 0
		if maxRunes > 0 {
			start = index * (maxRunes - vector.ChunkOverlap(maxRunes))
		}
		runes := []rune(content)
		lo := len(string(runes[:start]))
		hi := lo + len(chunk.Text)
		return secrets.RedactWindow(content, lo, hi)
	}
	return ""
}
