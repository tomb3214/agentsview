package main

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/postgres"
	"go.kenn.io/agentsview/internal/vector"
)

func newCacheCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "cache", Short: "Maintain an opt-in local history cache", GroupID: groupData}
	var snapshot string
	coverage := &cobra.Command{Use: "coverage", Short: "Fingerprint a PostgreSQL backup snapshot without transcript output", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := config.LoadMinimal()
		if err != nil {
			return err
		}
		pg, err := cachePostgres(cfg)
		if err != nil {
			return err
		}
		defer pg.Close()
		c, err := postgres.ReadCacheCoverage(cmd.Context(), pg, snapshot, nil)
		if err != nil {
			return err
		}
		b, err := json.Marshal(c)
		if err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), string(b))
		return nil
	}}
	coverage.Flags().StringVar(&snapshot, "snapshot", "", "Exported snapshot also supplied to pg_dump")
	var budget int64
	var proofPath string
	trim := &cobra.Command{Use: "trim", Short: "Evict oldest local transcripts covered by central storage and a verified backup (daemon must be stopped)", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if budget <= 0 {
			return errors.New("--max-bytes is required")
		}
		cfg, err := config.LoadMinimal()
		if err != nil {
			return err
		}
		if n, _, err := cacheStorageBytes(cfg); err != nil {
			return err
		} else if n <= budget {
			out, _ := json.Marshal(cacheTrimResult{BudgetBytes: budget, BeforeBytes: n, AfterBytes: n, WithinBudget: true})
			fmt.Fprintln(cmd.OutOrStdout(), string(out))
			return nil
		}
		var proof postgres.CacheCoverage
		if proofPath != "" {
			raw, err := os.ReadFile(proofPath)
			if err != nil {
				return err
			}
			if err = json.Unmarshal(raw, &proof); err != nil {
				return err
			}
		} else {
			pg, err := cachePostgres(cfg)
			if err != nil {
				return err
			}
			var raw string
			err = pg.QueryRowContext(cmd.Context(), "SELECT value FROM sync_metadata WHERE key='cache_backup_coverage_v1'").Scan(&raw)
			pg.Close()
			if err != nil {
				return errors.New("verified central backup coverage is unavailable; local history retained")
			}
			if err = json.Unmarshal([]byte(raw), &proof); err != nil {
				return err
			}
		}
		result, err := trimLocalCache(cmd.Context(), cfg, budget, proof)
		out, encErr := json.Marshal(result)
		if encErr != nil {
			return encErr
		}
		fmt.Fprintln(cmd.OutOrStdout(), string(out))
		if err == nil && !result.WithinBudget {
			return errors.New("protected history or auxiliary files exceed the local cache budget")
		}
		return err
	}}
	trim.Flags().Int64Var(&budget, "max-bytes", 0, "Maximum local data directory bytes; protected data may exceed this")
	trim.Flags().StringVar(&proofPath, "verified-backup", "", "Coverage from the exact read-back-verified encrypted backup")
	cmd.AddCommand(coverage, trim)
	return cmd
}

func cachePostgres(cfg config.Config) (*sql.DB, error) {
	// Backup jobs can supply a dedicated read-only DSN through their protected
	// environment. Credentials never appear in command arguments or output.
	if url := os.Getenv("AGENTSVIEW_CACHE_PG_URL"); url != "" {
		return postgres.Open(url, cfg.PG.Schema, cfg.PG.AllowInsecure)
	}
	targets, err := resolvePGTargetSelections(cfg, "", false)
	if err != nil {
		return nil, err
	}
	if len(targets) != 1 {
		return nil, errors.New("cache maintenance requires one default PostgreSQL archive")
	}
	target, err := resolvePGTargetConfig(cfg, targets[0])
	if err != nil {
		return nil, err
	}
	return postgres.Open(target.PG.URL, target.PG.Schema, target.PG.AllowInsecure)
}

type cacheTrimResult struct {
	BudgetBytes  int64 `json:"budget_bytes"`
	BeforeBytes  int64 `json:"before_bytes"`
	AfterBytes   int64 `json:"after_bytes"`
	Evicted      int   `json:"evicted"`
	Protected    int   `json:"protected"`
	WithinBudget bool  `json:"within_budget"`
}

// Count the configured archive/vector files even when an operator has moved
// them onto another volume. Other symlink targets (especially backup volumes)
// remain outside this cache policy. Canonical paths prevent double counting.
func cacheStorageBytes(cfg config.Config) (total, reserved int64, err error) {
	files := map[string]int64{}
	add := func(path string) (string, error) {
		canonical, e := filepath.EvalSymlinks(path)
		if errors.Is(e, os.ErrNotExist) {
			return "", nil
		}
		if e != nil {
			return "", e
		}
		canonical, e = filepath.Abs(canonical)
		if e != nil {
			return "", e
		}
		st, e := os.Stat(canonical)
		if e == nil && st.Mode().IsRegular() {
			files[canonical] = st.Size()
		}
		return canonical, e
	}
	root, err := filepath.EvalSymlinks(cfg.DataDir)
	if err != nil {
		return 0, 0, err
	}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if entry.Type().IsRegular() {
			_, e = add(path)
		}
		return e
	})
	if err != nil {
		return 0, 0, err
	}
	databases := map[string]bool{}
	for _, path := range []string{cfg.DBPath, cfg.Vector.ResolvedDBPath(cfg.DataDir)} {
		canonical, e := add(path)
		if e != nil {
			return 0, 0, e
		}
		if canonical == "" {
			continue
		}
		for _, base := range []string{path, canonical} {
			for _, suffix := range []string{"", "-wal", "-shm"} {
				key, e := add(base + suffix)
				if e != nil {
					return 0, 0, e
				}
				databases[key] = true
			}
		}
	}
	for path, size := range files {
		total += size
		if !databases[path] {
			reserved += size
		}
	}
	return total, reserved, nil
}

func cacheActivity(s db.Session) time.Time {
	for _, v := range []*string{s.EndedAt, s.StartedAt, &s.CreatedAt} {
		if v != nil {
			if parsed, ok := postgres.ParseSQLiteTimestamp(*v); ok {
				return parsed
			}
		}
	}
	// Unknown activity is protected as newest, never guessed to be old.
	return time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
}

func compareCacheActivity(a, b db.Session) int {
	if n := cacheActivity(a).Compare(cacheActivity(b)); n != 0 {
		return n
	}
	return strings.Compare(a.ID, b.ID)
}

func validateCacheProof(proof postgres.CacheCoverage, now time.Time) error {
	if proof.Format != postgres.CacheCoverageFormat || proof.BackupID == "" || len(proof.Sessions) == 0 {
		return errors.New("verified backup has no cache coverage")
	}
	captured, err := time.Parse(time.RFC3339, proof.CapturedAt)
	// Daily verified backups must be recent. A failed backup or publication
	// must not leave an indefinitely usable receipt after object retention.
	if err != nil || captured.After(now.Add(5*time.Minute)) || now.Sub(captured) > 72*time.Hour {
		return errors.New("verified backup coverage is stale; local history retained")
	}
	return nil
}

func trimLocalCache(ctx context.Context, cfg config.Config, budget int64, proof postgres.CacheCoverage) (cacheTrimResult, error) {
	result := cacheTrimResult{BudgetBytes: budget}
	before, _, err := cacheStorageBytes(cfg)
	result.BeforeBytes = before
	result.AfterBytes = before
	if err != nil {
		return result, err
	}
	if before <= budget {
		result.WithinBudget = true
		return result, nil
	}
	if err := validateCacheProof(proof, time.Now()); err != nil {
		return result, err
	}
	local, lock, err := openWriteDB(ctx, cfg)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	defer local.Close()
	vectorLock, err := tryAcquireNamedLock(cfg.DataDir, vectorsWriteLockFile)
	if err != nil {
		return result, err
	}
	defer vectorLock.Close()
	vectorPath := cfg.Vector.ResolvedDBPath(cfg.DataDir)
	var ix *vector.Index
	if _, err = os.Stat(vectorPath); err == nil {
		ix, err = vector.Open(ctx, vectorPath, false, cfg.Vector.Embeddings.MaxInputChars)
		if err != nil {
			return result, err
		}
		defer ix.Close()
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	_, reserved, err := cacheStorageBytes(cfg)
	if err != nil {
		return result, err
	}
	if reserved >= budget {
		return result, errors.New("auxiliary files alone exceed the budget; local transcripts retained")
	}
	if ix != nil {
		evicted, err := local.CacheEvictedSessionIDs(ctx)
		if err != nil {
			return result, err
		}
		if err = ix.EvictCachedSessions(ctx, evicted); err != nil {
			return result, err
		}
	}
	used := func() (int64, error) {
		n, e := local.CacheUsedBytes(ctx)
		if e != nil {
			return 0, e
		}
		if ix != nil {
			v, e := ix.CacheUsedBytes(ctx)
			if e != nil {
				return 0, e
			}
			n += v
		}
		return n + reserved, nil
	}
	pg, err := cachePostgres(cfg)
	if err != nil {
		return result, err
	}
	defer pg.Close()
	candidates, err := local.ListSessionsForMirrorWindow(ctx, "", nil, nil)
	if err != nil {
		return result, err
	}
	slices.SortFunc(candidates, compareCacheActivity)
	for start := 0; start < len(candidates); start += 32 {
		n, err := used()
		if err != nil {
			return result, err
		}
		if n <= budget {
			break
		}
		batch := candidates[start:min(start+32, len(candidates))]
		ids := []string{}
		for _, s := range batch {
			if _, ok := proof.Sessions[s.ID]; ok {
				ids = append(ids, s.ID)
			}
		}
		current, err := postgres.ReadCacheCoverage(ctx, pg, "", ids)
		if err != nil {
			return result, err
		}
		for _, s := range batch {
			if s.DeletedAt != nil || s.IsTruncated || s.FileHash == nil || *s.FileHash == "" {
				result.Protected++
				continue
			}
			var pins int
			if err := local.Reader().QueryRowContext(ctx, "SELECT count(*) FROM pinned_messages WHERE session_id=?", s.ID).Scan(&pins); err != nil {
				return result, err
			}
			if pins > 0 {
				result.Protected++
				continue
			}
			n, err := used()
			if err != nil {
				return result, err
			}
			if n <= budget {
				break
			}
			copy, ok := proof.Sessions[s.ID]
			if !ok {
				result.Protected++
				continue
			}
			eviction, err := postgres.VerifyCachedSession(ctx, local, s, current.Sessions[s.ID], copy)
			if err != nil {
				result.Protected++
				continue
			}
			eviction.BackupID = proof.BackupID
			if err = local.EvictCachedSession(ctx, *eviction); err != nil {
				return result, err
			}
			result.Evicted++
			if ix != nil {
				if err = ix.EvictCachedSessions(ctx, map[string]struct{}{s.ID: {}}); err != nil {
					return result, err
				}
			}
		}
	}
	n, err := used()
	if err != nil {
		return result, err
	}
	if result.Evicted > 0 || n <= budget {
		if err = local.CompactCache(ctx); err != nil {
			return result, err
		}
		if ix != nil {
			if err = ix.CompactCache(ctx); err != nil {
				return result, err
			}
		}
	}
	result.AfterBytes, _, err = cacheStorageBytes(cfg)
	result.WithinBudget = result.AfterBytes <= budget
	return result, err
}
