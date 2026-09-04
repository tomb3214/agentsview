// ABOUTME: `session source-retire` records exact content-addressed proof that
// ABOUTME: a managed archival lifecycle may intentionally remove a local file.
package main

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"

	"github.com/spf13/cobra"
	"go.kenn.io/agentsview/internal/service"
)

var sourceRetirementSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)

func newSessionSourceRetireCommand() *cobra.Command {
	var machine, agent, path, hash string
	cmd := &cobra.Command{
		Use:          "source-retire <session-id>",
		Short:        "Retire an exact local source without deleting its session",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if machine == "" || agent == "" || path == "" || hash == "" {
				return errors.New("--machine, --agent, --path, and --sha256 are required")
			}
			if !filepath.IsAbs(path) {
				return errors.New("--path must be absolute")
			}
			if !sourceRetirementSHA256.MatchString(hash) {
				return errors.New("--sha256 must be 64 lowercase hexadecimal characters")
			}
			svc, cleanup, err := resolveWritableService(cmd)
			if err != nil {
				return err
			}
			defer cleanup()
			offloader, ok := svc.(service.SessionSourceOffloader)
			if !ok {
				return errors.New("session source retirement is unavailable")
			}
			receipt, err := offloader.RetireSessionSource(
				cmd.Context(), service.SessionSourceRetirementInput{
					SessionID: args[0], Machine: machine, Agent: agent,
					FilePath: path, FileHash: hash,
				},
			)
			if err != nil {
				return err
			}
			if outputFormat(cmd) == "json" {
				return json.MarshalEncode(jsontext.NewEncoder(cmd.OutOrStdout()), receipt)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "retired source: %s\n", sanitizeTerminal(receipt.SessionID))
			return nil
		},
	}
	cmd.Flags().StringVar(&machine, "machine", "", "Exact stored machine identity")
	cmd.Flags().StringVar(&agent, "agent", "", "Exact stored provider identity")
	cmd.Flags().StringVar(&path, "path", "", "Exact stored native source path")
	cmd.Flags().StringVar(&hash, "sha256", "", "Exact current native source SHA-256")
	return cmd
}
