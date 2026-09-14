package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/postgres"
	"go.kenn.io/agentsview/internal/recall/extract"
)

// openConfiguredExtractStore opens only the configured source roles. It never
// changes schema or grants permissions. The daemon's existing writer lifetime
// owns these connections, and its existing scheduler owns the single manager.
func openConfiguredExtractStore(cfg config.RecallExtractConfig, local extract.Store) (extract.Store, func(), error) {
	if len(cfg.PostgresSources) == 0 {
		return local, func() {}, nil
	}
	var connections []*sql.DB
	closeStore := func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}
	var stores []*postgres.RecallExtractStore
	for _, source := range cfg.PostgresSources {
		raw, err := os.ReadFile(source.URLFile)
		if err != nil {
			closeStore()
			return nil, nil, fmt.Errorf("reading Recall source %s URL file: %w", source.Machine, err)
		}
		dsn := strings.TrimSpace(string(raw))
		if dsn == "" {
			closeStore()
			return nil, nil, fmt.Errorf("recall source %s URL file is empty", source.Machine)
		}
		schema := source.Schema
		if schema == "" {
			schema = "agentsview"
		}
		connection, err := postgres.Open(dsn, schema, source.AllowInsecure)
		if err != nil {
			closeStore()
			return nil, nil, fmt.Errorf("opening Recall source %s: %w", source.Machine, err)
		}
		connections = append(connections, connection)
		store, err := postgres.NewRecallExtractStore(connection, source.Machine)
		if err != nil {
			closeStore()
			return nil, nil, err
		}
		stores = append(stores, store)
	}
	group, err := postgres.NewRecallExtractGroup(stores...)
	if err != nil {
		closeStore()
		return nil, nil, err
	}
	return group, closeStore, nil
}

func buildConfiguredExtractManager(cfg config.RecallExtractConfig, local extract.Store) (*extract.Manager, func(), error) {
	store, closeStore, err := openConfiguredExtractStore(cfg, local)
	if err != nil {
		return nil, nil, err
	}
	manager, err := buildExtractManager(cfg, store)
	if err != nil {
		closeStore()
		return nil, nil, err
	}
	return manager, closeStore, nil
}
