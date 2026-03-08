package cmd

import (
	"fmt"

	"github.com/intuware/intu/internal/bootstrap"
	"github.com/intuware/intu/pkg/logging"
	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	var (
		force bool
		dir   string
	)

	cmd := &cobra.Command{
		Use:   "init [project-name]",
		Short: "Bootstrap a new intu project",
		Long:  "Creates a new intu project in <dir>/<project-name>. Default dir is current directory.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			projectName := args[0]
			logger := logging.New(rootOpts.logLevel, nil)
			scaffolder := bootstrap.NewScaffolder(logger)

			result, err := scaffolder.BootstrapProject(dir, projectName, force)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(),
				"intu init completed in %s (created: %d, overwritten: %d, skipped: %d)\n",
				result.Root, result.Created, result.Overwritten, result.Skipped)
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing files")
	cmd.Flags().StringVar(&dir, "dir", ".", "Target directory for project output")

	return cmd
}
