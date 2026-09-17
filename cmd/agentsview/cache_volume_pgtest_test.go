//go:build pgtest

package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
)

// This opt-in disk benchmark measures the eviction/compaction primitive on an
// operator-created SQLite backup. It does NOT test backup eligibility: the
// PostgreSQL integration tests exercise that independent boundary. The marker
// and fixed basename prevent an accidental run against a normal data profile.
func TestLocalCacheVolumeClone(t *testing.T) {
	root := os.Getenv("AGENTSVIEW_TEST_CACHE_CLONE")
	if root == "" {
		t.Skip("requires an isolated production-scale clone")
	}
	require.Equal(t, "production-clone", filepath.Base(root))
	_, err := os.Stat(filepath.Join(root, ".isolated-cache-test"))
	require.NoError(t, err)
	ctx := context.Background()
	local, err := db.Open(filepath.Join(root, "sessions.db"))
	require.NoError(t, err)
	defer local.Close()
	candidates, err := local.ListSessionsForMirrorWindow(ctx, "", nil, nil)
	require.NoError(t, err)
	slices.SortFunc(candidates, compareCacheActivity)
	before, _, err := cacheStorageBytes(config.Config{DataDir: root, DBPath: filepath.Join(root, "sessions.db")})
	require.NoError(t, err)
	started := time.Now()
	evicted, protected := 0, 0
	for _, s := range candidates {
		n, err := local.CacheUsedBytes(ctx)
		require.NoError(t, err)
		if n <= 5_000_000_000 {
			break
		}
		if s.FileHash == nil || *s.FileHash == "" || s.TranscriptRevision == nil || s.DeletedAt != nil || s.IsTruncated {
			protected++
			continue
		}
		var count, pins int
		require.NoError(t, local.Reader().QueryRowContext(ctx, "SELECT count(*) FROM messages WHERE session_id=?", s.ID).Scan(&count))
		require.NoError(t, local.Reader().QueryRowContext(ctx, "SELECT count(*) FROM pinned_messages WHERE session_id=?", s.ID).Scan(&pins))
		if count == 0 || pins > 0 {
			protected++
			continue
		}
		e := db.CacheEviction{SessionID: s.ID, FileHash: *s.FileHash, Revision: *s.TranscriptRevision, MessageCount: count, ContentDigest: strings.Repeat("0", 64), BackupID: "isolated-compaction-benchmark"}
		if s.LocalModifiedAt != nil {
			e.Modified = *s.LocalModifiedAt
		}
		require.NoError(t, local.EvictCachedSession(ctx, e))
		evicted++
		if evicted%500 == 0 {
			t.Logf("evicted=%d used_bytes=%d elapsed=%s", evicted, n, time.Since(started))
		}
	}
	t.Logf("real clone eviction: evicted=%d protected=%d elapsed=%s", evicted, protected, time.Since(started))
	started = time.Now()
	require.NoError(t, local.CompactCache(ctx))
	after, _, err := cacheStorageBytes(config.Config{DataDir: root, DBPath: filepath.Join(root, "sessions.db")})
	require.NoError(t, err)
	t.Logf("real clone compaction: before=%d after=%d elapsed=%s", before, after, time.Since(started))
	assert.LessOrEqual(t, after, int64(5_000_000_000))
	// The policy's under-budget recurrence must avoid opening or scanning data.
	assert.Positive(t, evicted)
}
