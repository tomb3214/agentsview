package postgres

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/secrets"
	"go.kenn.io/agentsview/internal/vector"
	kitvec "go.kenn.io/kit/vector"
)

func hasExplicitSessionFilters(f db.ContentSearchFilter) bool {
	return f.Project != "" || f.ExcludeProject != "" || f.Machine != "" || f.Agent != "" ||
		f.Date != "" || f.DateFrom != "" || f.DateTo != "" || f.ActiveSince != "" ||
		f.GitBranch != "" || f.IncludeAutomated
}

// searchCandidatesPG deliberately does not fuse or rerank: the caller merges
// these two bounded rankings with other authorized sources before one rerank.
// It requires the separately commissioned native BM25 index. Ordinary local
// and PostgreSQL search modes retain their existing response contract.
func (s *Store) searchCandidatesPG(ctx context.Context, f db.ContentSearchFilter) (db.ContentSearchPage, error) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
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

	limit := 100
	if f.Limit > 0 && f.Limit < 100 {
		limit = f.Limit
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

	var lexical []db.SearchCandidate
	var lexicalErr error
	var vectors []db.SearchCandidate
	var vectorErr error

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		tx, err := s.pg.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
		if err != nil {
			lexicalErr = err
			return
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, "SET LOCAL max_parallel_workers_per_gather = 0"); err != nil {
			lexicalErr = fmt.Errorf("disabling parallel workers: %w", err)
			return
		}
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			lexicalErr = fmt.Errorf("disabling sequential scans: %w", err)
			return
		}

		var keywordScope string
		var keywordArgs []any
		if hasExplicitSessionFilters(f) {
			keywordScope = scope
			keywordArgs = append(append([]any{}, args...), f.Pattern)
		} else {
			keywordScope = "d.ordinal >= 0"
			if f.Scope == "top" {
				keywordScope += " AND NOT d.subordinate"
			}
			if f.Scope == "subordinate" {
				keywordScope += " AND d.subordinate"
			}
			keywordArgs = []any{f.Pattern}
		}

		keyword := fmt.Sprintf("SELECT d.doc_key FROM vector_documents d WHERE %s AND d.content ||| $%d ORDER BY pdb.score(d.doc_key) DESC,d.doc_key COLLATE \"C\" LIMIT %d", keywordScope, len(keywordArgs), limit)
		keys, err := scanKeywordCandidateKeys(ctx, tx, keyword, keywordArgs)
		if err != nil {
			lexicalErr = fmt.Errorf("BM25 candidate keys: %w", err)
			return
		}
		if len(keys) > 0 {
			passagesWhere := "d.doc_key=ANY($2) AND EXISTS (SELECT 1 FROM " + v.chunkTable + " c WHERE c.doc_key=d.doc_key) AND d.content ||| $1"
			if !hasExplicitSessionFilters(f) {
				passagesWhere += " AND s.deleted_at IS NULL AND s.message_count > 0"
			}
			passages := "SELECT " + fields + ",to_json(pdb.snippet_positions(d.content,1))::text" + from +
				"WHERE " + passagesWhere + " ORDER BY array_position($2,d.doc_key)"
			lexical, lexicalErr = scanCandidateRows(ctx, tx, passages, []any{f.Pattern, keys}, v.maxInputChars, true)
			if lexicalErr != nil {
				lexicalErr = fmt.Errorf("BM25 candidate passages: %w", lexicalErr)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		tx, err := s.pg.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
		if err != nil {
			vectorErr = err
			return
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, "SET LOCAL max_parallel_workers_per_gather = 0"); err != nil {
			vectorErr = fmt.Errorf("disabling parallel workers: %w", err)
			return
		}
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			vectorErr = fmt.Errorf("disabling sequential scans: %w", err)
			return
		}
		if err = tuneHNSWRecall(ctx, tx, limit); err != nil {
			vectorErr = err
			return
		}

		param := fmt.Sprintf("$%d", len(args)+1)
		distance := "c.embedding OPERATOR(" + ext + ".<=>) " + param + "::" + ext + ".halfvec"
		semantic := fmt.Sprintf("SELECT %s,c.chunk_index%s JOIN %s c ON c.doc_key=d.doc_key WHERE %s ORDER BY %s,d.doc_key COLLATE \"C\",c.chunk_index LIMIT %d", fields, from, v.chunkTable, scope, distance, limit)
		vectors, vectorErr = scanCandidateRows(ctx, tx, semantic, append(append([]any{}, args...), literal), v.maxInputChars, false)
		if vectorErr != nil {
			vectorErr = fmt.Errorf("vector candidates: %w", vectorErr)
			return
		}
	}()

	wg.Wait()

	if lexicalErr != nil {
		return db.ContentSearchPage{}, lexicalErr
	}
	if vectorErr != nil {
		return db.ContentSearchPage{}, vectorErr
	}
	if lexical == nil {
		lexical = []db.SearchCandidate{}
	}
	if vectors == nil {
		vectors = []db.SearchCandidate{}
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
