package channel

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/intuware/intu/pkg/config"
	"github.com/intuware/intu/pkg/logging"
	"github.com/spf13/cobra"
)

func newListCmd(logLevel *string) *cobra.Command {
	var dir, profile, tag, group string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all channels in the project",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.New(*logLevel, nil)
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)
			entries, err := os.ReadDir(channelsDir)
			if err != nil {
				if os.IsNotExist(err) {
					fmt.Fprintln(cmd.OutOrStdout(), "No channels found.")
					return nil
				}
				return fmt.Errorf("read channels dir: %w", err)
			}

			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				channelDir := filepath.Join(channelsDir, e.Name())
				chCfg, err := config.LoadChannelConfig(channelDir)
				if err != nil {
					logger.Warn("skip channel", "dir", e.Name(), "error", err)
					continue
				}

				if tag != "" && !containsTag(chCfg.Tags, tag) {
					continue
				}
				if group != "" && chCfg.Group != group {
					continue
				}

				status := "enabled"
				if !chCfg.Enabled {
					status = "disabled"
				}

				line := fmt.Sprintf("%-30s %-10s", chCfg.ID, status)
				if len(chCfg.Tags) > 0 {
					line += fmt.Sprintf("  tags=%v", chCfg.Tags)
				}
				if chCfg.Group != "" {
					line += fmt.Sprintf("  group=%s", chCfg.Group)
				}
				if chCfg.Priority != "" {
					line += fmt.Sprintf("  priority=%s", chCfg.Priority)
				}
				fmt.Fprintln(cmd.OutOrStdout(), line)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().StringVar(&tag, "tag", "", "Filter by tag")
	cmd.Flags().StringVar(&group, "group", "", "Filter by group")
	return cmd
}

func containsTag(tags []string, tag string) bool {
	for _, t := range tags {
		if t == tag {
			return true
		}
	}
	return false
}
