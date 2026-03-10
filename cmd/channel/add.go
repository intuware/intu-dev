package channel

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/intuware/intu-dev/internal/bootstrap"
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
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			scaffolder := bootstrap.NewScaffolder(logger)

			_, err := scaffolder.BootstrapChannel(opts.dir, channelName, opts.force)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Channel created: %s\n", channelName)
			return nil
		},
	}

	cmd.Flags().BoolVar(&opts.force, "force", false, "Overwrite existing files")
	cmd.Flags().StringVar(&opts.dir, "dir", ".", "Project root directory")

	return cmd
}
