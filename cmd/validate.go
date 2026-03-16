package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/intuware/intu-dev/pkg/config"
	"github.com/spf13/cobra"
)

func newValidateCmd() *cobra.Command {
	var dir, profile string

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate project configuration and channels",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			errs, err := validateProject(cmd, dir, profile)
			if err != nil {
				return err
			}
			if len(errs) > 0 {
				for _, e := range errs {
					fmt.Fprintln(cmd.ErrOrStderr(), "  error:", e)
				}
				return fmt.Errorf("validation failed: %d error(s)", len(errs))
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	return cmd
}

// validateProject loads all channel configs and runs structural checks.
// It returns a list of error strings (empty on success) and an error only
// if the project itself cannot be loaded.
func validateProject(cmd *cobra.Command, dir, profile string) ([]string, error) {
	loader := config.NewLoader(dir)
	cfg, err := loader.Load(profile)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	if cfg.Runtime.Profile == "" {
		cfg.Runtime.Profile = profile
	}

	channelsDir := filepath.Join(dir, cfg.ChannelsDir)
	channelDirs, err := config.DiscoverChannelDirs(channelsDir)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintln(cmd.OutOrStdout(), "Validation passed (no channels directory).")
			return nil, nil
		}
		return nil, fmt.Errorf("discover channels: %w", err)
	}
	if len(channelDirs) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "Validation passed (no channels directory).")
		return nil, nil
	}

	var errs []string
	var channels []*config.ChannelConfig

	for _, channelDir := range channelDirs {
		chCfg, loadErr := config.LoadChannelConfig(channelDir)
		if loadErr != nil {
			relDir, _ := filepath.Rel(channelsDir, channelDir)
			errs = append(errs, fmt.Sprintf("channel %s: %v", relDir, loadErr))
			continue
		}
		if !chCfg.MatchesProfile(cfg.Runtime.Profile) {
			continue
		}
		channels = append(channels, chCfg)
	}

	errs = append(errs, config.ValidateListenerEndpoints(channels)...)

	if len(errs) == 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "Validation passed: %d channel(s), profile=%s\n", len(channels), profile)
	}
	return errs, nil
}
