package cmd

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/intuware/intu-dev/internal/observability"
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
	"github.com/spf13/cobra"
)

func newStatsCmd() *cobra.Command {
	var dir, profile string
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "stats [channel-id]",
		Short: "Show channel and message store statistics",
		Long:  "Print channel config and in-memory metrics (when engine has been running), plus message store counts when storage is enabled. Use --profile to select config profile (e.g. prod).",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			var store storage.MessageStore
			if cfg.MessageStorage != nil {
				store, err = storage.NewMessageStore(cfg.MessageStorage)
				if err != nil {
					return fmt.Errorf("message store: %w", err)
				}
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)

			if len(args) == 1 {
				channelDir, err := config.FindChannelDir(channelsDir, args[0])
				if err != nil {
					return err
				}
				chCfg, _ := config.LoadChannelConfig(channelDir)
				return printChannelStatsFromDir(cmd, channelDir, jsonOutput, store, chCfg)
			}

			// Global message store stats when no channel filter
			if store != nil {
				total, err := store.Count(storage.QueryOpts{})
				if err == nil {
					if jsonOutput {
						fmt.Fprintf(cmd.OutOrStdout(), "{\"message_store_total\": %d}\n", total)
					} else {
						fmt.Fprintf(cmd.OutOrStdout(), "Message store (profile %s): %d records\n\n", profile, total)
					}
				}
			}

			channelDirs, err := config.DiscoverChannelDirs(channelsDir)
			if err != nil {
				return fmt.Errorf("discover channels: %w", err)
			}

			for _, channelDir := range channelDirs {
				chCfg, _ := config.LoadChannelConfig(channelDir)
				printChannelStatsFromDir(cmd, channelDir, jsonOutput, store, chCfg)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile (e.g. dev, prod)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	return cmd
}

func printChannelStatsFromDir(cmd *cobra.Command, channelDir string, jsonOutput bool, store storage.MessageStore, chCfg *config.ChannelConfig) error {
	if chCfg == nil {
		var loadErr error
		chCfg, loadErr = config.LoadChannelConfig(channelDir)
		if loadErr != nil {
			return fmt.Errorf("load channel %s: %w", channelDir, loadErr)
		}
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

	if store != nil {
		n, err := store.Count(storage.QueryOpts{ChannelID: chCfg.ID})
		if err == nil {
			channelMetrics["stored"] = n
			stats["message_store_count"] = n
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
