package vector

import "context"

// EvictCachedSessions removes only local derived documents and their vectors.
// A failed partial eviction is retryable from durable archive cache receipts.
func (ix *Index) EvictCachedSessions(ctx context.Context, ids map[string]struct{}) error {
	for id := range ids {
		for {
			rows, err := ix.db.QueryContext(ctx, "SELECT doc_key FROM "+ix.spec.DocsTable+" WHERE session_id=? LIMIT 128", id)
			if err != nil {
				return err
			}
			keys := []string{}
			for rows.Next() {
				var key string
				if err = rows.Scan(&key); err != nil {
					rows.Close()
					return err
				}
				keys = append(keys, key)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			if len(keys) == 0 {
				break
			}
			for _, key := range keys {
				if _, err = ix.deleteMirrorDocument(ctx, key); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ix *Index) CacheUsedBytes(ctx context.Context) (int64, error) {
	var pages, free, size int64
	for _, v := range []struct {
		q string
		n *int64
	}{{"PRAGMA page_count", &pages}, {"PRAGMA freelist_count", &free}, {"PRAGMA page_size", &size}} {
		if err := ix.db.QueryRowContext(ctx, v.q).Scan(v.n); err != nil {
			return 0, err
		}
	}
	return (pages - free) * size, nil
}

func (ix *Index) CompactCache(ctx context.Context) error {
	if _, err := ix.db.ExecContext(ctx, "VACUUM"); err != nil {
		return err
	}
	_, err := ix.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}
