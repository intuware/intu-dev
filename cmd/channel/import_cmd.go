package channel

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/intuware/intu/pkg/config"
	"github.com/spf13/cobra"
)

func newImportCmd(logLevel *string) *cobra.Command {
	var dir, profile string
	var force bool

	cmd := &cobra.Command{
		Use:   "import <archive-path>",
		Short: "Import a channel from a portable archive",
		Long:  "Extracts a channel from a .tar.gz archive into the channels directory.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			archivePath := args[0]

			loader := config.NewLoader(dir)
			cfg, err := loader.Load(profile)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			channelsDir := filepath.Join(dir, cfg.ChannelsDir)

			file, err := os.Open(archivePath)
			if err != nil {
				return fmt.Errorf("open archive: %w", err)
			}
			defer file.Close()

			gzr, err := gzip.NewReader(file)
			if err != nil {
				return fmt.Errorf("decompress archive: %w", err)
			}
			defer gzr.Close()

			tr := tar.NewReader(gzr)
			fileCount := 0
			var channelID string

			for {
				header, err := tr.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("read archive: %w", err)
				}

				if header.Typeflag != tar.TypeReg {
					continue
				}

				cleanName := filepath.Clean(header.Name)
				if strings.Contains(cleanName, "..") {
					return fmt.Errorf("archive contains path traversal: %s", header.Name)
				}

				destPath := filepath.Join(channelsDir, cleanName)

				if channelID == "" {
					parts := strings.SplitN(cleanName, string(filepath.Separator), 2)
					if len(parts) > 0 {
						channelID = parts[0]
					}
				}

				if !force {
					if _, err := os.Stat(destPath); err == nil {
						return fmt.Errorf("file already exists: %s (use --force to overwrite)", destPath)
					}
				}

				if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
					return fmt.Errorf("create directory: %w", err)
				}

				outFile, err := os.Create(destPath)
				if err != nil {
					return fmt.Errorf("create file: %w", err)
				}

				if _, err := io.Copy(outFile, tr); err != nil {
					outFile.Close()
					return fmt.Errorf("write file: %w", err)
				}
				outFile.Close()
				fileCount++
			}

			if channelID == "" {
				channelID = "(unknown)"
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Imported channel %q from %s (%d files)\n", channelID, archivePath, fileCount)
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", ".", "Project root directory")
	cmd.Flags().StringVar(&profile, "profile", "dev", "Config profile")
	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing files")
	return cmd
}
