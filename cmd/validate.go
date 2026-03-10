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

	channelsDir := filepath.Join(dir, cfg.ChannelsDir)
	entries, err := os.ReadDir(channelsDir)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintln(cmd.OutOrStdout(), "Validation passed (no channels directory).")
			return nil, nil
		}
		return nil, fmt.Errorf("read channels dir: %w", err)
	}

	var errs []string
	var channels []*config.ChannelConfig

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		channelDir := filepath.Join(channelsDir, e.Name())
		channelPath := filepath.Join(channelDir, "channel.yaml")
		if _, statErr := os.Stat(channelPath); statErr != nil {
			if os.IsNotExist(statErr) {
				continue
			}
			errs = append(errs, fmt.Sprintf("channel %s: %v", e.Name(), statErr))
			continue
		}

		chCfg, loadErr := config.LoadChannelConfig(channelDir)
		if loadErr != nil {
			errs = append(errs, fmt.Sprintf("channel %s: %v", e.Name(), loadErr))
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
