package channel

import (
	"fmt"

	"github.com/intuware/intu/internal/bootstrap"
	"github.com/intuware/intu/pkg/logging"
	"github.com/spf13/cobra"
)

type addOpts struct {
	dir   string
	force bool
}

func newAddCmd(logLevel *string) *cobra.Command {
	opts := addOpts{}

	cmd := &cobra.Command{
		Use:   "add [channel-name]",
		Short: "Add a new channel to the project",
		Long:  "Creates a new channel in channels/<channel-name>/ within the project at --dir.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			channelName := args[0]
			logger := logging.New(*logLevel, nil)
			scaffolder := bootstrap.NewScaffolder(logger)

			result, err := scaffolder.BootstrapChannel(opts.dir, channelName, opts.force)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(),
				"Channel %s added in %s (created: %d, overwritten: %d, skipped: %d)\n",
				channelName, result.Root, result.Created, result.Overwritten, result.Skipped)
			return nil
		},
	}

	cmd.Flags().BoolVar(&opts.force, "force", false, "Overwrite existing files")
	cmd.Flags().StringVar(&opts.dir, "dir", ".", "Project root directory")

	return cmd
}
