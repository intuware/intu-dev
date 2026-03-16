package channel

import (
	"github.com/spf13/cobra"
)

// NewChannelCmd creates the intu channel subcommand.
func NewChannelCmd(logLevel *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "channel",
		Short: "Manage channels",
		Long:  "List, add, describe, clone, export, and import channels in an intu project.",
	}

	cmd.AddCommand(newAddCmd(logLevel))
	cmd.AddCommand(newListCmd(logLevel))
	cmd.AddCommand(newDescribeCmd(logLevel))
	cmd.AddCommand(newCloneCmd(logLevel))
	cmd.AddCommand(newExportCmd(logLevel))
	cmd.AddCommand(newImportCmd(logLevel))

	return cmd
}
