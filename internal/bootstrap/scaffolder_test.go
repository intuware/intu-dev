package bootstrap

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestBootstrapProjectCreatesProject(t *testing.T) {
	dir := t.TempDir()
	scaffolder := NewScaffolder(slog.Default())

	result, err := scaffolder.BootstrapProject(dir, "test-project", false)
	if err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}

	root := filepath.Join(dir, "test-project")
	if result.Root != root {
		t.Fatalf("expected root %s, got %s", root, result.Root)
	}

	files := projectFiles("test-project")
	if result.Created != len(files) {
		t.Fatalf("expected %d created files, got %d", len(files), result.Created)
	}

	for relPath := range files {
		absPath := filepath.Join(root, relPath)
		if _, err := os.Stat(absPath); err != nil {
			t.Fatalf("expected file %s to exist: %v", absPath, err)
		}
	}
}

func TestBootstrapProjectIsIdempotentWithoutForce(t *testing.T) {
	dir := t.TempDir()
	scaffolder := NewScaffolder(slog.Default())

	if _, err := scaffolder.BootstrapProject(dir, "test", false); err != nil {
		t.Fatalf("first bootstrap failed: %v", err)
	}

	result, err := scaffolder.BootstrapProject(dir, "test", false)
	if err != nil {
		t.Fatalf("second bootstrap failed: %v", err)
	}

	if result.Skipped != len(projectFiles("test")) {
		t.Fatalf("expected %d skipped files, got %d", len(projectFiles("test")), result.Skipped)
	}
}

func TestBootstrapProjectForceOverwritesFiles(t *testing.T) {
	dir := t.TempDir()
	scaffolder := NewScaffolder(slog.Default())

	if _, err := scaffolder.BootstrapProject(dir, "test", false); err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}

	samplePath := filepath.Join(dir, "test", "intu.yaml")
	if err := os.WriteFile(samplePath, []byte("mutated: true\n"), 0o644); err != nil {
		t.Fatalf("mutate file: %v", err)
	}

	result, err := scaffolder.BootstrapProject(dir, "test", true)
	if err != nil {
		t.Fatalf("bootstrap with force failed: %v", err)
	}

	if result.Overwritten != len(projectFiles("test")) {
		t.Fatalf("expected %d overwritten files, got %d", len(projectFiles("test")), result.Overwritten)
	}

	content, err := os.ReadFile(samplePath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(content) != intuYAML {
		t.Fatalf("expected intu.yaml template to be restored")
	}
}

func TestBootstrapChannelCreatesChannel(t *testing.T) {
	dir := t.TempDir()
	scaffolder := NewScaffolder(slog.Default())

	// Bootstrap project first
	if _, err := scaffolder.BootstrapProject(dir, "test", false); err != nil {
		t.Fatalf("bootstrap project failed: %v", err)
	}

	root := filepath.Join(dir, "test")
	result, err := scaffolder.BootstrapChannel(root, "my-channel", "", false)
	if err != nil {
		t.Fatalf("bootstrap channel failed: %v", err)
	}

	files := channelFiles(DefaultChannelsDir, "my-channel")
	if result.Created != len(files) {
		t.Fatalf("expected %d created files, got %d", len(files), result.Created)
	}

	for relPath := range files {
		absPath := filepath.Join(root, relPath)
		if _, err := os.Stat(absPath); err != nil {
			t.Fatalf("expected file %s to exist: %v", absPath, err)
		}
	}
}
