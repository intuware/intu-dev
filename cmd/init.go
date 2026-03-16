package cmd

import (
	"fmt"
	"io"
	"log/slog"
	"os/exec"

	"github.com/intuware/intu-dev/internal/bootstrap"
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
		Long:  "Creates a new intu project with config, sample channels, and npm install.\n\nBy default the project is created in <dir>/<project-name>. Use --dir . to create in the current directory (no subfolder).",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			projectName := args[0]
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			scaffolder := bootstrap.NewScaffolder(logger)
			// When user explicitly passes --dir ., create project in that dir (no project-name subfolder)
			inPlace := cmd.Flags().Lookup("dir").Changed && (dir == "." || dir == "")
			result, err := scaffolder.BootstrapProject(dir, projectName, force, inPlace)
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
			if result.Root == "." || result.Root == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "  npm run dev\n")
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "  cd %s\n", result.Root)
				fmt.Fprintf(cmd.OutOrStdout(), "  npm run dev\n")
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing files")
	cmd.Flags().StringVar(&dir, "dir", ".", "Target directory; use . to create project in current directory (no subfolder)")

	return cmd
}
