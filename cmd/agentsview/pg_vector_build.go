package main

import (
	"encoding/json"
	"github.com/spf13/cobra"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/postgres"
	"time"
)

func newPGVectorsBuildCommand() *cobra.Command {
	var target, machine string
	var sources, chunks, sourceBytes int
	var timeout, hardTimeout time.Duration
	cmd := &cobra.Command{Use: "build", Short: "Embed a bounded pass of current PostgreSQL source sessions", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := config.LoadMinimal()
		if err != nil {
			return err
		}
		if err = requireVectorEnabled(cfg); err != nil {
			return err
		}
		_, server, err := cfg.Vector.Embeddings.Server("")
		if err != nil {
			return err
		}
		enc, err := newVectorDocumentEncoder(cfg.Vector.Embeddings, "")
		if err != nil {
			return err
		}
		pg, close, err := openPGVectorTarget(target)
		if err != nil {
			return err
		}
		defer close()
		result, err := postgres.BuildCentralVectors(cmd.Context(), pg, postgres.VectorBuildOptions{Machine: machine, Generation: vectorGeneration(cfg.Vector.Embeddings), MaxSources: sources, MaxSourceBytes: sourceBytes, MaxChunks: chunks, BatchSize: server.BatchSize, MaxInputChars: cfg.Vector.Embeddings.MaxInputChars, Timeout: timeout, HardTimeout: hardTimeout, IncludeAutomated: cfg.Vector.IncludeAutomated, Encode: enc})
		if e := json.NewEncoder(cmd.OutOrStdout()).Encode(result); e != nil {
			return e
		}
		return err
	}}
	cmd.Flags().StringVar(&target, "target", "", "Existing PostgreSQL target")
	cmd.Flags().StringVar(&machine, "machine", "", "Original source machine to reconcile (required)")
	cmd.Flags().IntVar(&sources, "max-sources", 25, "Maximum source sessions examined")
	cmd.Flags().IntVar(&sourceBytes, "max-source-bytes", 16<<20, "Maximum source transcript bytes held in memory")
	cmd.Flags().IntVar(&chunks, "max-chunks", 256, "Chunk budget between complete sources; one source may exceed it")
	cmd.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "Normal pass budget; finish the current source before yielding")
	cmd.Flags().DurationVar(&hardTimeout, "hard-timeout", 30*time.Minute, "Hard deadline including source completion")
	_ = cmd.MarkFlagRequired("machine")
	return cmd
}
