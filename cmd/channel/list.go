package channel

import (
	"fmt"
	"path/filepath"

	"github.com/intuware/intu-dev/pkg/config"
	"github.com/intuware/intu-dev/pkg/logging"
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
			channelDirs, err := config.DiscoverChannelDirs(channelsDir)
			if err != nil || len(channelDirs) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No channels found.")
				return nil
			}

			for _, channelDir := range channelDirs {
				chCfg, err := config.LoadChannelConfig(channelDir)
				if err != nil {
					logger.Warn("skip channel", "dir", channelDir, "error", err)
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

				relPath, _ := filepath.Rel(channelsDir, channelDir)
				line := fmt.Sprintf("%-30s %-10s  path=%s", chCfg.ID, status, relPath)
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
