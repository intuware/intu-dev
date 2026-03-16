package cmd

import (
	"os"

	"github.com/intuware/intu-dev/cmd/channel"
	"github.com/spf13/cobra"
)

var (
	// Set via -ldflags at build time: go build -ldflags "-X github.com/intuware/intu-dev/cmd.Version=1.0.0"
	Version = "dev"

	rootOpts struct {
		logLevel string
	}

	rootCmd = &cobra.Command{
		Use:   "intu",
		Short: "intu is a Git-native healthcare interoperability framework",
		Long: "A Git-native, AI-friendly healthcare interoperability framework that lets teams " +
			"build, version, and deploy integration pipelines using YAML configuration and TypeScript transformers.",
		Version:       Version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
)

func init() {
	rootCmd.PersistentFlags().StringVar(&rootOpts.logLevel, "log-level", "info", "Structured log level (debug|info|warn|error)")
	rootCmd.AddCommand(newInitCmd())
	rootCmd.AddCommand(newCCmd())
	rootCmd.AddCommand(newChannelCmd())
	rootCmd.AddCommand(newServeCmd())
	rootCmd.AddCommand(newValidateCmd())
	rootCmd.AddCommand(newBuildCmd())
	rootCmd.AddCommand(newStatsCmd())
	rootCmd.AddCommand(newDeployCmd())
	rootCmd.AddCommand(newUndeployCmd())
	rootCmd.AddCommand(newEnableCmd())
	rootCmd.AddCommand(newDisableCmd())
	rootCmd.AddCommand(newPruneCmd())
	rootCmd.AddCommand(newDashboardCmd())
	rootCmd.AddCommand(newMessageCmd())
	rootCmd.AddCommand(newReprocessCmd())
	rootCmd.AddCommand(newImportCmd())
}

func newChannelCmd() *cobra.Command {
	return channel.NewChannelCmd(&rootOpts.logLevel)
}

func Execute() error {
	if err := rootCmd.Execute(); err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		return err
	}
	return nil
}
