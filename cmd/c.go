package cmd

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/intuware/intu-dev/internal/bootstrap"
	"github.com/intuware/intu-dev/pkg/config"
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
		Long:  "Creates a new channel in src/channels/<channel-name>/ within the project at --dir.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			channelName := args[0]
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			scaffolder := bootstrap.NewScaffolder(logger)

			channelsDir := bootstrap.DefaultChannelsDir
			loader := config.NewLoader(dir)
			if cfg, err := loader.Load("dev"); err == nil && cfg.ChannelsDir != "" {
				channelsDir = cfg.ChannelsDir
			}

			_, err := scaffolder.BootstrapChannel(dir, channelName, channelsDir, force)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Channel created: %s\n", channelName)
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing files")
	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")

	return cmd
}
