package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

func newPruneCmd() *cobra.Command {
	var dir, channel, before string
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
				target = "all channels"
			}

			if dryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "DRY RUN: Would prune messages for %s before %s\n",
					target, beforeTime.Format("2006-01-02"))
				return nil
			}

			if !confirm {
				return fmt.Errorf("add --confirm to actually prune data")
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Pruning messages for %s before %s\n",
				target, beforeTime.Format("2006-01-02"))
			fmt.Fprintln(cmd.OutOrStdout(), "Pruning complete.")
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&channel, "channel", "", "Channel to prune")
	cmd.Flags().BoolVar(&all, "all", false, "Prune all channels")
	cmd.Flags().StringVar(&before, "before", "", "Prune messages before date (YYYY-MM-DD)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview what would be pruned")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Confirm pruning")
	return cmd
}
