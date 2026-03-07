package channel

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/intuware/intu/pkg/config"
	"github.com/spf13/cobra"
)

func newCloneCmd(logLevel *string) *cobra.Command {
	var dir, profile string

	cmd := &cobra.Command{
		Use:   "clone <source-channel> <new-channel>",
		Short: "Clone a channel to create a new one",
		Long:  "Copies all files from an existing channel directory to a new channel, updating the channel ID.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sourceID := args[0]
			newID := args[1]

			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)
			sourceDir := filepath.Join(channelsDir, sourceID)
			destDir := filepath.Join(channelsDir, newID)

			if _, err := os.Stat(sourceDir); os.IsNotExist(err) {
				return fmt.Errorf("source channel %q not found at %s", sourceID, sourceDir)
			}

			if _, err := os.Stat(destDir); err == nil {
				return fmt.Errorf("target channel %q already exists at %s", newID, destDir)
			}

			if err := os.MkdirAll(destDir, 0o755); err != nil {
				return fmt.Errorf("create target directory: %w", err)
			}

			entries, err := os.ReadDir(sourceDir)
			if err != nil {
				return fmt.Errorf("read source channel: %w", err)
			}

			copied := 0
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				srcPath := filepath.Join(sourceDir, entry.Name())
				dstPath := filepath.Join(destDir, entry.Name())

				data, err := os.ReadFile(srcPath)
				if err != nil {
					return fmt.Errorf("read %s: %w", srcPath, err)
				}

				content := string(data)
				if entry.Name() == "channel.yaml" {
					content = replaceChannelID(content, sourceID, newID)
				}

				if err := os.WriteFile(dstPath, []byte(content), 0o644); err != nil {
					return fmt.Errorf("write %s: %w", dstPath, err)
				}
				copied++
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Cloned channel %q -> %q (%d files)\n", sourceID, newID, copied)
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	return cmd
}

func replaceChannelID(content, oldID, newID string) string {
	// Replace the id field in channel.yaml
	lines := []byte(content)
	result := make([]byte, 0, len(lines))

	for _, line := range splitLines(string(lines)) {
		if len(line) > 4 && line[:4] == "id: " {
			result = append(result, []byte("id: "+newID+"\n")...)
		} else {
			result = append(result, []byte(line+"\n")...)
		}
	}

	return string(result)
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
