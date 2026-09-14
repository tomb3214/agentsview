package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/recall/extract"
)

// RecallExtractGroup feeds one extraction manager from machine-scoped stores.
// The manager, rather than each source, owns the concurrency limit. Publication
// remains atomic per machine and retries can finish a partially activated group.
type RecallExtractGroup struct {
	stores []*RecallExtractStore
	mu     sync.RWMutex
	routes map[string]*RecallExtractStore
}

var _ extract.Store = (*RecallExtractGroup)(nil)
var errRecallSourceNotFound = errors.New("recall source session not found")

func NewRecallExtractGroup(stores ...*RecallExtractStore) (*RecallExtractGroup, error) {
	if len(stores) == 0 {
		return nil, fmt.Errorf("recall extraction requires at least one source")
	}
	machines := make(map[string]bool)
	for _, store := range stores {
		if store == nil || machines[store.machine] {
			return nil, fmt.Errorf("recall sources must have distinct machine identities")
		}
		machines[store.machine] = true
	}
	return &RecallExtractGroup{stores: append([]*RecallExtractStore(nil), stores...), routes: make(map[string]*RecallExtractStore)}, nil
}

func (g *RecallExtractGroup) source(ctx context.Context, id string) (*RecallExtractStore, error) {
	g.mu.RLock()
	store := g.routes[id]
	g.mu.RUnlock()
	if store != nil {
		return store, nil
	}
	for _, store := range g.stores {
		session, err := store.GetSessionFull(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("source %s: %w", store.machine, err)
		}
		if session != nil {
			return store, nil
		}
	}
	return nil, errRecallSourceNotFound
}

func (g *RecallExtractGroup) GetSessionFull(ctx context.Context, id string) (*db.Session, error) {
	s, err := g.source(ctx, id)
	if errors.Is(err, errRecallSourceNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.GetSessionFull(ctx, id)
}
func (g *RecallExtractGroup) GetSession(ctx context.Context, id string) (*db.Session, error) {
	s, err := g.source(ctx, id)
	if errors.Is(err, errRecallSourceNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.GetSession(ctx, id)
}
func (g *RecallExtractGroup) GetAllMessages(ctx context.Context, id string) ([]db.Message, error) {
	s, err := g.source(ctx, id)
	if errors.Is(err, errRecallSourceNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.GetAllMessages(ctx, id)
}

func (g *RecallExtractGroup) ExtractCandidates(ctx context.Context, q db.ExtractCandidateQuery) ([]string, error) {
	lists := make([][]string, len(g.stores))
	routes := make(map[string]*RecallExtractStore)
	for i, s := range g.stores {
		ids, err := s.ExtractCandidates(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("source %s: %w", s.machine, err)
		}
		for _, id := range ids {
			if routes[id] != nil {
				return nil, fmt.Errorf("session %s belongs to more than one configured source", id)
			}
			routes[id] = s
		}
		lists[i] = ids
	}
	// Interleave source-local oldest-first queues, so a large source cannot
	// consume the entire initial worker window. Unit order stays in the manager.
	var ids []string
	for index := 0; ; index++ {
		added := false
		for _, list := range lists {
			if index < len(list) {
				ids = append(ids, list[index])
				added = true
				if q.Limit > 0 && len(ids) == q.Limit {
					break
				}
			}
		}
		if !added || (q.Limit > 0 && len(ids) == q.Limit) {
			break
		}
	}
	// Replace, rather than accumulate, the current discovery cache. A concurrent
	// status scan may evict an in-flight id; source() then resolves it normally.
	g.mu.Lock()
	g.routes = routes
	g.mu.Unlock()
	return ids, nil
}

func (g *RecallExtractGroup) ExtractGenerations(ctx context.Context) ([]db.ExtractGeneration, error) {
	byID := make(map[string]db.ExtractGeneration)
	for _, s := range g.stores {
		generations, err := s.ExtractGenerations(ctx)
		if err != nil {
			return nil, err
		}
		for _, gen := range generations {
			old, found := byID[gen.Fingerprint]
			if !found {
				byID[gen.Fingerprint] = gen
				continue
			}
			if old.State == db.ExtractGenerationBuilding || gen.State == db.ExtractGenerationBuilding {
				old.State = db.ExtractGenerationBuilding
			} else if old.State == db.ExtractGenerationActive || gen.State == db.ExtractGenerationActive {
				old.State = db.ExtractGenerationActive
			}
			byID[gen.Fingerprint] = old
		}
	}
	var out []db.ExtractGeneration
	for _, gen := range byID {
		out = append(out, gen)
	}
	sort.Slice(out, func(i, j int) bool {
		a, _ := time.Parse(time.RFC3339Nano, out[i].CreatedAt)
		b, _ := time.Parse(time.RFC3339Nano, out[j].CreatedAt)
		if a.Equal(b) {
			return out[i].Fingerprint < out[j].Fingerprint
		}
		return a.After(b)
	})
	return out, nil
}

func (g *RecallExtractGroup) EnsureExtractGeneration(ctx context.Context, gen db.ExtractGeneration) (db.ExtractGeneration, error) {
	for _, s := range g.stores {
		if _, err := s.EnsureExtractGeneration(ctx, gen); err != nil {
			return db.ExtractGeneration{}, err
		}
	}
	gens, err := g.ExtractGenerations(ctx)
	if err != nil {
		return db.ExtractGeneration{}, err
	}
	for _, stored := range gens {
		if stored.Fingerprint == gen.Fingerprint {
			return stored, nil
		}
	}
	return db.ExtractGeneration{}, db.ErrExtractGenerationNotFound
}

func (g *RecallExtractGroup) ExtractProgressStats(ctx context.Context, fp string) (db.ExtractProgressStats, error) {
	var sum db.ExtractProgressStats
	for _, s := range g.stores {
		p, err := s.ExtractProgressStats(ctx, fp)
		if err != nil {
			return sum, err
		}
		sum.Pending += p.Pending
		sum.Partial += p.Partial
		sum.Done += p.Done
		sum.Failed += p.Failed
		sum.UnitsDone += p.UnitsDone
		sum.UnitsTotal += p.UnitsTotal
		sum.Entries += p.Entries
	}
	return sum, nil
}

func (g *RecallExtractGroup) ActivateExtractGeneration(ctx context.Context, fp string, versions []string, cutoff time.Time) error {
	for _, s := range g.stores {
		ids, err := s.ExtractCandidates(ctx, db.ExtractCandidateQuery{Fingerprint: fp, QuietCutoff: cutoff, IncludeDone: true, Limit: 1})
		if err != nil {
			return err
		}
		stats, err := s.ExtractProgressStats(ctx, fp)
		if err != nil {
			return err
		}
		if len(ids) > 0 || stats.Done == 0 || stats.Entries == 0 {
			return fmt.Errorf("source %s: %w", s.machine, db.ErrExtractActivationBlocked)
		}
	}
	for _, s := range g.stores {
		if err := s.ActivateExtractGeneration(ctx, fp, versions, cutoff); err != nil {
			return fmt.Errorf("source %s: %w", s.machine, err)
		}
	}
	return nil
}

func (g *RecallExtractGroup) RetireExtractGeneration(ctx context.Context, fp string, force bool) error {
	var targets []*RecallExtractStore
	for _, s := range g.stores {
		gens, err := s.ExtractGenerations(ctx)
		if err != nil {
			return err
		}
		for _, gen := range gens {
			if gen.Fingerprint == fp {
				if gen.State == db.ExtractGenerationActive && !force {
					return db.ErrExtractGenerationActive
				}
				targets = append(targets, s)
			}
		}
	}
	if len(targets) == 0 {
		return db.ErrExtractGenerationNotFound
	}
	for _, s := range targets {
		if err := s.RetireExtractGeneration(ctx, fp, force); err != nil {
			return err
		}
	}
	return nil
}

func (g *RecallExtractGroup) ExtractProgress(ctx context.Context, id, fp string) (db.ExtractProgress, bool, error) {
	s, err := g.source(ctx, id)
	if errors.Is(err, errRecallSourceNotFound) {
		return db.ExtractProgress{}, false, nil
	}
	if err != nil {
		return db.ExtractProgress{}, false, err
	}
	return s.ExtractProgress(ctx, id, fp)
}
func (g *RecallExtractGroup) UpsertExtractProgress(ctx context.Context, u db.ExtractProgressUpsert) (db.ExtractProgress, error) {
	s, err := g.source(ctx, u.SessionID)
	if err != nil {
		return db.ExtractProgress{}, err
	}
	return s.UpsertExtractProgress(ctx, u)
}
func (g *RecallExtractGroup) RefreshExtractedSessionCoverage(ctx context.Context, u db.ExtractCoverageRefresh) (db.ExtractProgress, error) {
	if u.Session == nil {
		return db.ExtractProgress{}, fmt.Errorf("coverage refresh requires a source snapshot")
	}
	s, err := g.source(ctx, u.Session.ID)
	if err != nil {
		return db.ExtractProgress{}, err
	}
	return s.RefreshExtractedSessionCoverage(ctx, u)
}
func (g *RecallExtractGroup) CommitExtractedUnit(ctx context.Context, u db.ExtractUnitCommit) (int, error) {
	s, err := g.source(ctx, u.SessionID)
	if err != nil {
		return 0, err
	}
	return s.CommitExtractedUnit(ctx, u)
}
func (g *RecallExtractGroup) MarkExtractProgressFailed(ctx context.Context, f db.ExtractFailure) error {
	s, err := g.source(ctx, f.SessionID)
	if err != nil {
		return err
	}
	return s.MarkExtractProgressFailed(ctx, f)
}
func (g *RecallExtractGroup) DiscardExtractedSessionOutput(ctx context.Context, f db.ExtractFailure) error {
	s, err := g.source(ctx, f.SessionID)
	if err != nil {
		return err
	}
	return s.DiscardExtractedSessionOutput(ctx, f)
}
func (g *RecallExtractGroup) ReconcileIneligibleExtractSessions(ctx context.Context, since time.Time) (int, int, error) {
	var progress, entries int
	for _, s := range g.stores {
		p, e, err := s.ReconcileIneligibleExtractSessions(ctx, since)
		progress += p
		entries += e
		if err != nil {
			return progress, entries, err
		}
	}
	return progress, entries, nil
}
