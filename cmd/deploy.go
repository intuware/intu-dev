package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/intuware/intu/pkg/config"
	"github.com/intuware/intu/pkg/logging"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newDeployCmd() *cobra.Command {
	var dir, profile, tag string
	var all bool

	cmd := &cobra.Command{
		Use:   "deploy [channel-id]",
		Short: "Deploy a channel (mark as enabled)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.New(rootOpts.logLevel, nil)
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)

			if len(args) == 1 {
				return setChannelEnabled(channelsDir, args[0], true)
			}

			if !all && tag == "" {
				return fmt.Errorf("specify a channel ID, --all, or --tag")
			}

			entries, err := os.ReadDir(channelsDir)
			if err != nil {
				return fmt.Errorf("read channels dir: %w", err)
			}

			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				if tag != "" {
					chCfg, err := config.LoadChannelConfig(filepath.Join(channelsDir, e.Name()))
					if err != nil {
						logger.Warn("skip channel", "name", e.Name(), "error", err)
						continue
					}
					found := false
					for _, t := range chCfg.Tags {
						if t == tag {
							found = true
							break
						}
					}
					if !found {
						continue
					}
				}
				if err := setChannelEnabled(channelsDir, e.Name(), true); err != nil {
					logger.Error("deploy failed", "channel", e.Name(), "error", err)
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "Deployed: %s\n", e.Name())
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().BoolVar(&all, "all", false, "Deploy all channels")
	cmd.Flags().StringVar(&tag, "tag", "", "Deploy channels with this tag")
	return cmd
}

func newUndeployCmd() *cobra.Command {
	var dir, profile string

	cmd := &cobra.Command{
		Use:   "undeploy [channel-id]",
		Short: "Undeploy a channel (mark as disabled)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)
			if err := setChannelEnabled(channelsDir, args[0], false); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Undeployed: %s\n", args[0])
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	return cmd
}

func newEnableCmd() *cobra.Command {
	var dir, profile string

	cmd := &cobra.Command{
		Use:   "enable [channel-id]",
		Short: "Enable a channel",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)
			if err := setChannelEnabled(channelsDir, args[0], true); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Enabled: %s\n", args[0])
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	return cmd
}

func newDisableCmd() *cobra.Command {
	var dir, profile string

	cmd := &cobra.Command{
		Use:   "disable [channel-id]",
		Short: "Disable a channel",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)
			if err := setChannelEnabled(channelsDir, args[0], false); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Disabled: %s\n", args[0])
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	return cmd
}

func setChannelEnabled(channelsDir, channelID string, enabled bool) error {
	path := filepath.Join(channelsDir, channelID, "channel.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read channel %s: %w", channelID, err)
	}

	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("parse channel %s: %w", channelID, err)
	}

	raw["enabled"] = enabled
	out, err := yaml.Marshal(raw)
	if err != nil {
		return fmt.Errorf("marshal channel %s: %w", channelID, err)
	}

	return os.WriteFile(path, out, 0o644)
}
