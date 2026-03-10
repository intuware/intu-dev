package cmd

import (
	"fmt"
	"io"
	"log/slog"
	"os/exec"

	"github.com/intuware/intu/internal/bootstrap"
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
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			scaffolder := bootstrap.NewScaffolder(logger)

			result, err := scaffolder.BootstrapProject(dir, projectName, force)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Installing dependencies...\n")
			npm := exec.Command("npm", "install")
			npm.Dir = result.Root
			npm.Stdout = cmd.OutOrStdout()
			npm.Stderr = cmd.ErrOrStderr()
			if err := npm.Run(); err != nil {
				return fmt.Errorf("npm install: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "\nProject created: %s (2 channels)\n", projectName)
			fmt.Fprintf(cmd.OutOrStdout(), "Next steps:\n")
			fmt.Fprintf(cmd.OutOrStdout(), "  cd %s\n", result.Root)
			fmt.Fprintf(cmd.OutOrStdout(), "  npm run dev\n")
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing files")
	cmd.Flags().StringVar(&dir, "dir", ".", "Target directory for project output")

	return cmd
}
