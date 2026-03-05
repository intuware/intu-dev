package cmd

import (
	"os"

	"github.com/intuware/intu/cmd/channel"
	"github.com/spf13/cobra"
)

var rootOpts struct {
	logLevel string
}

var rootCmd = &cobra.Command{
	Use:   "intu",
	Short: "intu is a Git-native healthcare interoperability framework",
	Long: "intu is a production-grade distributed integration framework for health-tech teams " +
		"to run within their own infrastructure.",
	SilenceUsage:  true,
	SilenceErrors: true,
}

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
