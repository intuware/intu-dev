package cmd

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/intuware/intu/internal/storage"
	"github.com/intuware/intu/pkg/config"
	"github.com/spf13/cobra"
)

func newMessageCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "message",
		Short: "Browse and search processed messages",
		Long:  "Query the message store for processed messages by channel, status, and time range.",
	}

	cmd.AddCommand(newMessageListCmd())
	cmd.AddCommand(newMessageGetCmd())
	cmd.AddCommand(newMessageCountCmd())
	return cmd
}

func newMessageListCmd() *cobra.Command {
	var dir, profile, channelID, status, since, before string
	var limit, offset int
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List messages from the store",
		RunE: func(cmd *cobra.Command, args []string) error {
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			store, err := storage.NewMessageStore(cfg.MessageStorage)
			if err != nil {
				return fmt.Errorf("init message store: %w", err)
			}

			opts := storage.QueryOpts{
				ChannelID: channelID,
				Status:    status,
				Limit:     limit,
				Offset:    offset,
			}

			if since != "" {
				t, err := time.Parse(time.RFC3339, since)
				if err != nil {
					t, err = time.Parse("2006-01-02", since)
					if err != nil {
						return fmt.Errorf("invalid --since (use RFC3339 or YYYY-MM-DD): %w", err)
					}
				}
				opts.Since = t
			}

			if before != "" {
				t, err := time.Parse(time.RFC3339, before)
				if err != nil {
					t, err = time.Parse("2006-01-02", before)
					if err != nil {
						return fmt.Errorf("invalid --before (use RFC3339 or YYYY-MM-DD): %w", err)
					}
				}
				opts.Before = t
			}

			records, err := store.Query(opts)
			if err != nil {
				return fmt.Errorf("query messages: %w", err)
			}

			if jsonOutput {
				data, _ := json.MarshalIndent(records, "", "  ")
				fmt.Fprintln(cmd.OutOrStdout(), string(data))
			} else {
				if len(records) == 0 {
					fmt.Fprintln(cmd.OutOrStdout(), "No messages found.")
					return nil
				}
				for _, r := range records {
					fmt.Fprintf(cmd.OutOrStdout(), "ID: %s  Channel: %s  Stage: %s  Status: %s  Time: %s\n",
						r.ID, r.ChannelID, r.Stage, r.Status,
						r.Timestamp.Format(time.RFC3339))
					if len(r.Content) > 0 {
						content := string(r.Content)
						if len(content) > 200 {
							content = content[:200] + "...(truncated)"
						}
						fmt.Fprintf(cmd.OutOrStdout(), "  Content: %s\n", content)
					}
					fmt.Fprintln(cmd.OutOrStdout())
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Total: %d messages\n", len(records))
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().StringVar(&channelID, "channel", "", "Filter by channel ID")
	cmd.Flags().StringVar(&status, "status", "", "Filter by status (RECEIVED, TRANSFORMED, SENT, ERROR, FILTERED)")
	cmd.Flags().StringVar(&since, "since", "", "Messages since (RFC3339 or YYYY-MM-DD)")
	cmd.Flags().StringVar(&before, "before", "", "Messages before (RFC3339 or YYYY-MM-DD)")
	cmd.Flags().IntVar(&limit, "limit", 50, "Maximum number of messages to return")
	cmd.Flags().IntVar(&offset, "offset", 0, "Offset for pagination")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	return cmd
}

func newMessageGetCmd() *cobra.Command {
	var dir, profile string
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "get <message-id>",
		Short: "Get a specific message by ID",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			store, err := storage.NewMessageStore(cfg.MessageStorage)
			if err != nil {
				return fmt.Errorf("init message store: %w", err)
			}

			record, err := store.Get(args[0])
			if err != nil {
				return fmt.Errorf("message not found: %w", err)
			}

			if jsonOutput {
				data, _ := json.MarshalIndent(record, "", "  ")
				fmt.Fprintln(cmd.OutOrStdout(), string(data))
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "ID:            %s\n", record.ID)
				fmt.Fprintf(cmd.OutOrStdout(), "Correlation:   %s\n", record.CorrelationID)
				fmt.Fprintf(cmd.OutOrStdout(), "Channel:       %s\n", record.ChannelID)
				fmt.Fprintf(cmd.OutOrStdout(), "Stage:         %s\n", record.Stage)
				fmt.Fprintf(cmd.OutOrStdout(), "Status:        %s\n", record.Status)
				fmt.Fprintf(cmd.OutOrStdout(), "Timestamp:     %s\n", record.Timestamp.Format(time.RFC3339))
				if len(record.Content) > 0 {
					fmt.Fprintf(cmd.OutOrStdout(), "Content:\n%s\n", string(record.Content))
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	return cmd
}

func newMessageCountCmd() *cobra.Command {
	var dir, profile, channelID, status string

	cmd := &cobra.Command{
		Use:   "count",
		Short: "Count messages in the store",
		RunE: func(cmd *cobra.Command, args []string) error {
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			store, err := storage.NewMessageStore(cfg.MessageStorage)
			if err != nil {
				return fmt.Errorf("init message store: %w", err)
			}

			records, err := store.Query(storage.QueryOpts{
				ChannelID: channelID,
				Status:    status,
			})
			if err != nil {
				return fmt.Errorf("query messages: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "%d\n", len(records))
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().StringVar(&channelID, "channel", "", "Filter by channel ID")
	cmd.Flags().StringVar(&status, "status", "", "Filter by status")
	return cmd
}
