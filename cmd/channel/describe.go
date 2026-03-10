package channel

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/intuware/intu-dev/pkg/config"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newDescribeCmd(logLevel *string) *cobra.Command {
	var dir, profile string

	cmd := &cobra.Command{
		Use:   "describe [channel-id]",
		Short: "Show channel configuration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			channelID := args[0]
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelPath := filepath.Join(dir, cfg.ChannelsDir, channelID, "channel.yaml")
			data, err := os.ReadFile(channelPath)
			if err != nil {
				if os.IsNotExist(err) {
					return fmt.Errorf("channel %s not found", channelID)
				}
				return fmt.Errorf("read channel config: %w", err)
			}

			var raw map[string]interface{}
			if err := yaml.Unmarshal(data, &raw); err != nil {
				return fmt.Errorf("parse channel config: %w", err)
			}

			out, err := yaml.Marshal(raw)
			if err != nil {
				return fmt.Errorf("marshal config: %w", err)
			}

			fmt.Fprint(cmd.OutOrStdout(), string(out))
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	return cmd
}
