package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/intuware/intu/pkg/config"
	"github.com/spf13/cobra"
)

func newStatsCmd() *cobra.Command {
	var dir, profile string
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "stats [channel-id]",
		Short: "Show channel statistics",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)

			if len(args) == 1 {
				return printChannelStats(cmd, channelsDir, args[0], jsonOutput)
			}

			entries, err := os.ReadDir(channelsDir)
			if err != nil {
				return fmt.Errorf("read channels dir: %w", err)
			}

			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				printChannelStats(cmd, channelsDir, e.Name(), jsonOutput)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	return cmd
}

func printChannelStats(cmd *cobra.Command, channelsDir, channelID string, jsonOutput bool) error {
	channelDir := filepath.Join(channelsDir, channelID)
	chCfg, err := config.LoadChannelConfig(channelDir)
	if err != nil {
		return fmt.Errorf("load channel %s: %w", channelID, err)
	}

	stats := map[string]any{
		"channel":  chCfg.ID,
		"enabled":  chCfg.Enabled,
		"listener": chCfg.Listener.Type,
	}
	if len(chCfg.Tags) > 0 {
		stats["tags"] = chCfg.Tags
	}
	if chCfg.Group != "" {
		stats["group"] = chCfg.Group
	}

	destNames := []string{}
	for _, d := range chCfg.Destinations {
		name := d.Name
		if name == "" {
			name = d.Ref
		}
		destNames = append(destNames, name)
	}
	stats["destinations"] = destNames

	if jsonOutput {
		data, _ := json.MarshalIndent(stats, "", "  ")
		fmt.Fprintln(cmd.OutOrStdout(), string(data))
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "Channel: %s\n", chCfg.ID)
		fmt.Fprintf(cmd.OutOrStdout(), "  Enabled:      %v\n", chCfg.Enabled)
		fmt.Fprintf(cmd.OutOrStdout(), "  Listener:     %s\n", chCfg.Listener.Type)
		if len(chCfg.Tags) > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), "  Tags:         %v\n", chCfg.Tags)
		}
		if chCfg.Group != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "  Group:        %s\n", chCfg.Group)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "  Destinations: %v\n", destNames)
		fmt.Fprintln(cmd.OutOrStdout())
	}

	return nil
}
