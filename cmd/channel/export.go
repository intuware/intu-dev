package channel

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/intuware/intu-dev/pkg/config"
	"github.com/intuware/intu-dev/pkg/logging"
	"github.com/spf13/cobra"
)

func newExportCmd(logLevel *string) *cobra.Command {
	var dir, profile, output, tag, group string

	cmd := &cobra.Command{
		Use:   "export [channel-id]",
		Short: "Export channel(s) as portable archives",
		Long:  "Export one channel by ID, or all channels (optionally filtered by --group or --tag). With no arguments, exports all channels; each is written to <channel-id>.tar.gz. With a channel-id, exports that channel to the given -o path or <channel-id>.tar.gz. Filtering works like `intu channel list`.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)

			if len(args) == 1 {
				// Single channel export
				channelID := args[0]
				channelDir, err := config.FindChannelDir(channelsDir, channelID)
				if err != nil {
					return fmt.Errorf("channel %q not found", channelID)
				}
				outPath := output
				if outPath == "" {
					outPath = channelID + ".tar.gz"
				}
				fileCount, err := writeChannelArchive(channelsDir, channelDir, channelID, outPath)
				if err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Exported channel %q to %s (%d files)\n", channelID, outPath, fileCount)
				return nil
			}

			// Export all (filtered by --group / --tag like list)
			channelDirs, err := config.DiscoverChannelDirs(channelsDir)
			if err != nil || len(channelDirs) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No channels found.")
				return nil
			}

			logger := logging.New(*logLevel, nil)
			outputDir := output
			if outputDir == "" {
				outputDir = "."
			}
			if outputDir != "." {
				if err := os.MkdirAll(outputDir, 0o755); err != nil {
					return fmt.Errorf("create output directory: %w", err)
				}
			}

			exported := 0
			for _, channelDir := range channelDirs {
				chCfg, err := config.LoadChannelConfig(channelDir)
				if err != nil {
					logger.Warn("skip channel", "dir", channelDir, "error", err)
					continue
				}
				if tag != "" && !containsTag(chCfg.Tags, tag) {
					continue
				}
				if group != "" && chCfg.Group != group {
					continue
				}

				outPath := filepath.Join(outputDir, chCfg.ID+".tar.gz")
				fileCount, err := writeChannelArchive(channelsDir, channelDir, chCfg.ID, outPath)
				if err != nil {
					return fmt.Errorf("export %q: %w", chCfg.ID, err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Exported channel %q to %s (%d files)\n", chCfg.ID, outPath, fileCount)
				exported++
			}

			if exported == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "No channels matched the given filters.")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output file (single channel) or output directory (all/filtered); default <channel-id>.tar.gz or .")
	cmd.Flags().StringVar(&tag, "tag", "", "Filter by tag (same as list)")
	cmd.Flags().StringVar(&group, "group", "", "Filter by group (same as list)")
	return cmd
}

// writeChannelArchive writes the channel at channelDir to outPath as a .tar.gz.
// channelsDir is the project's channels root (for relative paths in the archive).
func writeChannelArchive(channelsDir, channelDir, channelID, outPath string) (fileCount int, err error) {
	outFile, err := os.Create(outPath)
	if err != nil {
		return 0, fmt.Errorf("create output file: %w", err)
	}
	defer outFile.Close()

	gzw := gzip.NewWriter(outFile)
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	fileCount = 0
	err = filepath.Walk(channelDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(channelsDir, path)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = relPath

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		if _, err := io.Copy(tw, file); err != nil {
			return err
		}

		fileCount++
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("archive channel: %w", err)
	}
	return fileCount, nil
}
