package channel

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/intuware/intu-dev/pkg/config"
	"github.com/spf13/cobra"
)

func newExportCmd(logLevel *string) *cobra.Command {
	var dir, profile, output string

	cmd := &cobra.Command{
		Use:   "export <channel-id>",
		Short: "Export a channel as a portable archive",
		Long:  "Packages a channel directory into a .tar.gz archive for sharing or backup.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			channelID := args[0]

			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)
			channelDir := filepath.Join(channelsDir, channelID)

			if _, err := os.Stat(channelDir); os.IsNotExist(err) {
				return fmt.Errorf("channel %q not found at %s", channelID, channelDir)
			}

			if output == "" {
				output = channelID + ".tar.gz"
			}

			outFile, err := os.Create(output)
			if err != nil {
				return fmt.Errorf("create output file: %w", err)
			}
			defer outFile.Close()

			gzw := gzip.NewWriter(outFile)
			defer gzw.Close()

			tw := tar.NewWriter(gzw)
			defer tw.Close()

			fileCount := 0
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
				return fmt.Errorf("archive channel: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Exported channel %q to %s (%d files)\n", channelID, output, fileCount)
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output file path (default: <channel-id>.tar.gz)")
	return cmd
}
