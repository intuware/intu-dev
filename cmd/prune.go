package cmd

import (
	"fmt"
	"time"

	"github.com/intuware/intu/internal/storage"
	"github.com/intuware/intu/pkg/config"
	"github.com/spf13/cobra"
)

func newPruneCmd() *cobra.Command {
	var dir, channel, before, profile string
	var all, dryRun, confirm bool

	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Prune old message data",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !all && channel == "" {
				return fmt.Errorf("specify --channel or --all")
			}

			var beforeTime time.Time
			if before != "" {
				t, err := time.Parse("2006-01-02", before)
				if err != nil {
					return fmt.Errorf("invalid --before date (use YYYY-MM-DD): %w", err)
				}
				beforeTime = t
			} else {
				beforeTime = time.Now().AddDate(0, 0, -30)
			}

			target := channel
			if all {
				target = ""
			}

			if dryRun {
				displayTarget := channel
				if all {
					displayTarget = "all channels"
				}
				fmt.Fprintf(cmd.OutOrStdout(), "DRY RUN: Would prune messages for %s before %s\n",
					displayTarget, beforeTime.Format("2006-01-02"))
				return nil
			}

			if !confirm {
				return fmt.Errorf("add --confirm to actually prune data")
			}

			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			store, err := storage.NewMessageStore(cfg.MessageStorage)
			if err != nil {
				return fmt.Errorf("init message store: %w", err)
			}

			pruned, err := store.Prune(beforeTime, target)
			if err != nil {
				return fmt.Errorf("prune failed: %w", err)
			}

			displayTarget := channel
			if all {
				displayTarget = "all channels"
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Pruned %d messages for %s before %s\n",
				pruned, displayTarget, beforeTime.Format("2006-01-02"))
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().StringVar(&channel, "channel", "", "Channel to prune")
	cmd.Flags().BoolVar(&all, "all", false, "Prune all channels")
	cmd.Flags().StringVar(&before, "before", "", "Prune messages before date (YYYY-MM-DD)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview what would be pruned")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Confirm pruning")
	return cmd
}
