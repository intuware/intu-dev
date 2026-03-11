package cmd

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/intuware/intu-dev/internal/observability"
	"github.com/intuware/intu-dev/pkg/config"
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
				channelDir, err := config.FindChannelDir(channelsDir, args[0])
				if err != nil {
					return err
				}
				return printChannelStatsFromDir(cmd, channelDir, jsonOutput)
			}

			channelDirs, err := config.DiscoverChannelDirs(channelsDir)
			if err != nil {
				return fmt.Errorf("discover channels: %w", err)
			}

			for _, channelDir := range channelDirs {
				printChannelStatsFromDir(cmd, channelDir, jsonOutput)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	return cmd
}

func printChannelStatsFromDir(cmd *cobra.Command, channelDir string, jsonOutput bool) error {
	chCfg, err := config.LoadChannelConfig(channelDir)
	if err != nil {
		return fmt.Errorf("load channel %s: %w", channelDir, err)
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

	metrics := observability.Global()
	snap := metrics.Snapshot()

	channelMetrics := map[string]any{}
	if counters, ok := snap["counters"].(map[string]int64); ok {
		prefix := "messages_received_total." + chCfg.ID
		if v, ok := counters[prefix]; ok {
			channelMetrics["received"] = v
		}
		prefix = "messages_processed_total." + chCfg.ID
		if v, ok := counters[prefix]; ok {
			channelMetrics["processed"] = v
		}
		prefix = "messages_filtered_total." + chCfg.ID
		if v, ok := counters[prefix]; ok {
			channelMetrics["filtered"] = v
		}
		prefix = "messages_errored_total." + chCfg.ID
		for k, v := range counters {
			if len(k) > len(prefix)+1 && k[:len(prefix)] == prefix {
				if channelMetrics["errored"] == nil {
					channelMetrics["errored"] = v
				} else {
					channelMetrics["errored"] = channelMetrics["errored"].(int64) + v
				}
			}
		}
	}

	if timings, ok := snap["timings"].(map[string]map[string]any); ok {
		latencyKey := "processing_duration." + chCfg.ID + ".total"
		if t, ok := timings[latencyKey]; ok {
			channelMetrics["avg_latency"] = t["avg"]
			channelMetrics["min_latency"] = t["min"]
			channelMetrics["max_latency"] = t["max"]
			channelMetrics["total_count"] = t["count"]
		}
	}

	if len(channelMetrics) > 0 {
		stats["metrics"] = channelMetrics
	}

	if len(chCfg.DependsOn) > 0 {
		stats["depends_on"] = chCfg.DependsOn
	}

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
		if len(chCfg.DependsOn) > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), "  Depends on:   %v\n", chCfg.DependsOn)
		}
		if len(channelMetrics) > 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "  Metrics:")
			for k, v := range channelMetrics {
				fmt.Fprintf(cmd.OutOrStdout(), "    %-14s %v\n", k+":", v)
			}
		}
		fmt.Fprintln(cmd.OutOrStdout())
	}

	return nil
}
