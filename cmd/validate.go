package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/intuware/intu/pkg/config"
	"github.com/intuware/intu/pkg/logging"
	"github.com/spf13/cobra"
)

func newValidateCmd() *cobra.Command {
	var dir, profile string

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate project configuration and channels",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.New(rootOpts.logLevel, nil)
			var errs []error

			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			logger.Info("config loaded", "profile", profile)

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)
			entries, err := os.ReadDir(channelsDir)
			if err != nil {
				if os.IsNotExist(err) {
					logger.Warn("channels directory not found", "path", channelsDir)
					fmt.Fprintln(cmd.OutOrStdout(), "Validation complete (no channels).")
					return nil
				}
				return fmt.Errorf("read channels dir: %w", err)
			}

			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				channelPath := filepath.Join(channelsDir, e.Name(), "channel.yaml")
				if _, err := os.Stat(channelPath); err != nil {
					if os.IsNotExist(err) {
						continue
					}
					errs = append(errs, fmt.Errorf("channel %s: %w", e.Name(), err))
					continue
				}
				logger.Info("channel valid", "id", e.Name())
			}

			if len(errs) > 0 {
				for _, e := range errs {
					fmt.Fprintln(cmd.ErrOrStderr(), e.Error())
				}
				return fmt.Errorf("validation failed: %d error(s)", len(errs))
			}

			fmt.Fprintln(cmd.OutOrStdout(), "Validation complete.")
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	return cmd
}
