package cmd

import (
	"fmt"

	"github.com/intuware/intu/internal/bootstrap"
	"github.com/intuware/intu/pkg/logging"
	"github.com/spf13/cobra"
)

func newCCmd() *cobra.Command {
	var (
		force bool
		dir   string
	)

	cmd := &cobra.Command{
		Use:   "c [channel-name]",
		Short: "Bootstrap a new channel in an existing project",
		Long:  "Creates a new channel in channels/<channel-name>/ within the project at --dir.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			channelName := args[0]
			logger := logging.New(rootOpts.logLevel, nil)
			scaffolder := bootstrap.NewScaffolder(logger)

			result, err := scaffolder.BootstrapChannel(dir, channelName, force)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(),
				"intu c completed in %s (created: %d, overwritten: %d, skipped: %d)\n",
				result.Root, result.Created, result.Overwritten, result.Skipped)
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing files")
	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")

	return cmd
}
