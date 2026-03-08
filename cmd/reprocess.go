package cmd

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/internal/storage"
	"github.com/intuware/intu/pkg/config"
	"github.com/intuware/intu/pkg/logging"
	"github.com/spf13/cobra"
)

func newReprocessCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reprocess <channel-id>",
		Short: "Reprocess messages from the message store",
		Long: `Re-submit stored messages through the channel pipeline. You can reprocess
a single message by ID, or batch-reprocess by status and/or time range.`,
	}

	cmd.AddCommand(newReprocessByIDCmd())
	cmd.AddCommand(newReprocessBatchCmd())
	return cmd
}

func newReprocessByIDCmd() *cobra.Command {
	var dir, profile, messageID string
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "message <channel-id> --message-id <id>",
		Short: "Reprocess a single message by ID",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			channelID := args[0]
			if messageID == "" {
				return fmt.Errorf("--message-id is required")
			}

			logger := logging.New(rootOpts.logLevel, nil)
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			store, err := storage.NewMessageStore(cfg.MessageStorage)
			if err != nil {
				return fmt.Errorf("init message store: %w", err)
			}

			record, err := store.Get(messageID)
			if err != nil {
				return fmt.Errorf("message %s not found: %w", messageID, err)
			}

			if record.ChannelID != channelID {
				return fmt.Errorf("message %s belongs to channel %s, not %s", messageID, record.ChannelID, channelID)
			}

			if dryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "Would reprocess message %s from channel %s (stage: %s, status: %s)\n",
					record.ID, record.ChannelID, record.Stage, record.Status)
				return nil
			}

			newMsg := rebuildMessage(record)

			reprocessRecord := &storage.MessageRecord{
				ID:            newMsg.ID,
				CorrelationID: newMsg.CorrelationID,
				ChannelID:     channelID,
				Stage:         "reprocessed",
				Content:       newMsg.Raw,
				Status:        "REPROCESSED",
				Timestamp:     time.Now(),
				Metadata: map[string]any{
					"original_message_id": record.ID,
					"original_status":     record.Status,
					"original_stage":      record.Stage,
					"reprocessed_at":      time.Now().Format(time.RFC3339),
				},
			}
			if err := store.Save(reprocessRecord); err != nil {
				logger.Warn("failed to save reprocess record", "error", err)
			}

			data, _ := json.MarshalIndent(map[string]any{
				"reprocessed":         true,
				"original_message_id": record.ID,
				"new_message_id":      newMsg.ID,
				"channel":             channelID,
				"timestamp":           time.Now().Format(time.RFC3339),
			}, "", "  ")
			fmt.Fprintln(cmd.OutOrStdout(), string(data))

			logger.Info("message reprocessed",
				"originalID", record.ID,
				"newID", newMsg.ID,
				"channel", channelID,
			)

			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().StringVar(&messageID, "message-id", "", "Message ID to reprocess")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show what would be reprocessed without executing")
	return cmd
}

func newReprocessBatchCmd() *cobra.Command {
	var dir, profile, status, since string
	var limit int
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "batch <channel-id>",
		Short: "Reprocess multiple messages by status and time range",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			channelID := args[0]
			logger := logging.New(rootOpts.logLevel, nil)
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

			records, err := store.Query(opts)
			if err != nil {
				return fmt.Errorf("query messages: %w", err)
			}

			if len(records) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No messages found matching criteria.")
				return nil
			}

			if dryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "Would reprocess %d messages from channel %s\n", len(records), channelID)
				for _, r := range records {
					fmt.Fprintf(cmd.OutOrStdout(), "  ID: %s  Status: %s  Stage: %s  Time: %s\n",
						r.ID, r.Status, r.Stage, r.Timestamp.Format(time.RFC3339))
				}
				return nil
			}

			reprocessed := 0
			for _, record := range records {
				newMsg := rebuildMessage(record)

				reprocessRecord := &storage.MessageRecord{
					ID:            newMsg.ID,
					CorrelationID: newMsg.CorrelationID,
					ChannelID:     channelID,
					Stage:         "reprocessed",
					Content:       newMsg.Raw,
					Status:        "REPROCESSED",
					Timestamp:     time.Now(),
					Metadata: map[string]any{
						"original_message_id": record.ID,
						"original_status":     record.Status,
						"reprocessed_at":      time.Now().Format(time.RFC3339),
					},
				}
				if err := store.Save(reprocessRecord); err != nil {
					logger.Warn("failed to save reprocess record", "messageID", record.ID, "error", err)
					continue
				}
				reprocessed++
			}

			data, _ := json.MarshalIndent(map[string]any{
				"reprocessed_count": reprocessed,
				"total_matched":    len(records),
				"channel":          channelID,
				"status_filter":    status,
				"timestamp":        time.Now().Format(time.RFC3339),
			}, "", "  ")
			fmt.Fprintln(cmd.OutOrStdout(), string(data))

			logger.Info("batch reprocess complete",
				"channel", channelID,
				"reprocessed", reprocessed,
				"total", len(records),
			)

			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().StringVar(&status, "status", "ERROR", "Filter by status (default: ERROR)")
	cmd.Flags().StringVar(&since, "since", "", "Messages since (RFC3339 or YYYY-MM-DD)")
	cmd.Flags().IntVar(&limit, "limit", 100, "Maximum number of messages to reprocess")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show what would be reprocessed without executing")
	return cmd
}

func rebuildMessage(record *storage.MessageRecord) *message.Message {
	msg := message.New(record.ChannelID, record.Content)
	msg.CorrelationID = record.CorrelationID
	if msg.CorrelationID == "" {
		msg.CorrelationID = record.ID
	}
	msg.Metadata["reprocessed"] = true
	msg.Metadata["original_message_id"] = record.ID
	msg.Metadata["original_timestamp"] = record.Timestamp.Format(time.RFC3339)

	return msg
}
