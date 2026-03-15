package bootstrap

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
)

type Result struct {
	Root        string
	Created     int
	Overwritten int
	Skipped     int
}

type Scaffolder struct {
	logger *slog.Logger
}

func NewScaffolder(logger *slog.Logger) *Scaffolder {
	return &Scaffolder{logger: logger}
}

// BootstrapProject creates a full intu project in dir/projectName.
func (s *Scaffolder) BootstrapProject(dir, projectName string, force bool) (*Result, error) {
	root := filepath.Join(dir, projectName)
	cleanRoot := filepath.Clean(root)
	result := &Result{Root: cleanRoot}

	if stat, err := os.Stat(cleanRoot); err == nil {
		if stat.IsDir() && !force {
			return nil, fmt.Errorf("project directory already exists: %s (use --force to overwrite)", cleanRoot)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat %s: %w", cleanRoot, err)
	}

	if err := os.MkdirAll(cleanRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create root directory: %w", err)
	}

	for _, d := range projectDirectories {
		absDir := filepath.Join(cleanRoot, d)
		if err := os.MkdirAll(absDir, 0o755); err != nil {
			return nil, fmt.Errorf("create directory %s: %w", absDir, err)
		}
		s.logger.Debug("ensured directory", "path", absDir)
	}

	for relPath, content := range projectFiles(projectName) {
		absPath := filepath.Join(cleanRoot, relPath)
		status, err := writeFile(absPath, content, force)
		if err != nil {
			return nil, err
		}

		switch status {
		case "created":
			result.Created++
		case "overwritten":
			result.Overwritten++
		case "skipped":
			result.Skipped++
		}

		s.logger.Debug("processed file", "path", absPath, "status", status)
	}

	return result, nil
}

// DefaultChannelsDir is the default channels directory for new projects.
const DefaultChannelsDir = "src/channels"

// BootstrapChannel creates a new channel in an existing project root.
// channelsDir is the relative path from root (e.g. "src/channels").
func (s *Scaffolder) BootstrapChannel(root, channelName, channelsDir string, force bool) (*Result, error) {
	cleanRoot := filepath.Clean(root)
	result := &Result{Root: cleanRoot}

	if channelsDir == "" {
		channelsDir = DefaultChannelsDir
	}

	channelDir := filepath.Join(cleanRoot, channelsDir, channelName)
	if err := os.MkdirAll(channelDir, 0o755); err != nil {
		return nil, fmt.Errorf("create channel directory: %w", err)
	}

	files := channelFiles(channelsDir, channelName)
	for relPath, content := range files {
		absPath := filepath.Join(cleanRoot, relPath)
		status, err := writeFile(absPath, content, force)
		if err != nil {
			return nil, err
		}

		switch status {
		case "created":
			result.Created++
		case "overwritten":
			result.Overwritten++
		case "skipped":
			result.Skipped++
		}

		s.logger.Debug("processed file", "path", absPath, "status", status)
	}

	return result, nil
}

func writeFile(path, content string, force bool) (string, error) {
	if stat, err := os.Stat(path); err == nil {
		if stat.IsDir() {
			return "", fmt.Errorf("path %s exists as a directory", path)
		}
		if !force {
			return "skipped", nil
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return "", fmt.Errorf("overwrite %s: %w", path, err)
		}
		return "overwritten", nil
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("stat %s: %w", path, err)
	}

	if err := os.WriteFile(path, []byte(content), fs.FileMode(0o644)); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return "created", nil
}
