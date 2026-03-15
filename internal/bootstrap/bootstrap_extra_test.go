package bootstrap

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// NewScaffolder
// ---------------------------------------------------------------------------

func TestNewScaffolder(t *testing.T) {
	s := NewScaffolder(slog.Default())
	if s == nil {
		t.Fatal("expected non-nil Scaffolder")
	}
}

// ---------------------------------------------------------------------------
// BootstrapProject: validates file contents
// ---------------------------------------------------------------------------

func TestBootstrapProject_FileContents(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "my-project", false)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	root := result.Root

	// intu.yaml should contain the runtime block
	data, err := os.ReadFile(filepath.Join(root, "intu.yaml"))
	if err != nil {
		t.Fatalf("read intu.yaml: %v", err)
	}
	if !strings.Contains(string(data), "runtime:") {
		t.Fatal("intu.yaml should contain runtime: block")
	}
	if !strings.Contains(string(data), "channels_dir: src/channels") {
		t.Fatal("intu.yaml should contain channels_dir")
	}

	// package.json should exist and contain intu-related content
	data, err = os.ReadFile(filepath.Join(root, "package.json"))
	if err != nil {
		t.Fatalf("read package.json: %v", err)
	}
	if !strings.Contains(string(data), "intu-channel-runtime") {
		t.Fatal("package.json should contain intu-channel-runtime")
	}

	// tsconfig.json should exist
	if _, err := os.Stat(filepath.Join(root, "tsconfig.json")); err != nil {
		t.Fatalf("expected tsconfig.json: %v", err)
	}

	// Dockerfile should exist
	if _, err := os.Stat(filepath.Join(root, "Dockerfile")); err != nil {
		t.Fatalf("expected Dockerfile: %v", err)
	}

	// docker-compose.yml should reference the project name
	data, err = os.ReadFile(filepath.Join(root, "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read docker-compose.yml: %v", err)
	}
	if !strings.Contains(string(data), "my-project") {
		t.Fatal("docker-compose.yml should contain project name")
	}

	// .gitignore should exist
	if _, err := os.Stat(filepath.Join(root, ".gitignore")); err != nil {
		t.Fatalf("expected .gitignore: %v", err)
	}

	// Type declarations should exist
	if _, err := os.Stat(filepath.Join(root, "src", "types", "intu.d.ts")); err != nil {
		t.Fatalf("expected intu.d.ts: %v", err)
	}
}

// ---------------------------------------------------------------------------
// BootstrapProject: creates expected directories
// ---------------------------------------------------------------------------

func TestBootstrapProject_Directories(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "dir-test", false)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	for _, d := range projectDirectories {
		absDir := filepath.Join(result.Root, d)
		info, err := os.Stat(absDir)
		if err != nil {
			t.Fatalf("expected directory %s: %v", d, err)
		}
		if !info.IsDir() {
			t.Fatalf("expected %s to be a directory", d)
		}
	}
}

// ---------------------------------------------------------------------------
// BootstrapProject: Result counts
// ---------------------------------------------------------------------------

func TestBootstrapProject_ResultCreatedCount(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "count-test", false)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	expectedFiles := len(projectFiles("count-test"))
	if result.Created != expectedFiles {
		t.Fatalf("expected %d created, got %d", expectedFiles, result.Created)
	}
	if result.Skipped != 0 {
		t.Fatalf("expected 0 skipped, got %d", result.Skipped)
	}
	if result.Overwritten != 0 {
		t.Fatalf("expected 0 overwritten, got %d", result.Overwritten)
	}
}

// ---------------------------------------------------------------------------
// BootstrapChannel: custom channels dir
// ---------------------------------------------------------------------------

func TestBootstrapChannel_CustomChannelsDir(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	if _, err := s.BootstrapProject(dir, "ch-dir-test", false); err != nil {
		t.Fatalf("bootstrap project: %v", err)
	}

	root := filepath.Join(dir, "ch-dir-test")
	result, err := s.BootstrapChannel(root, "custom-ch", "custom/channels", false)
	if err != nil {
		t.Fatalf("bootstrap channel: %v", err)
	}

	channelYAML := filepath.Join(root, "custom", "channels", "custom-ch", "channel.yaml")
	if _, err := os.Stat(channelYAML); err != nil {
		t.Fatalf("expected channel.yaml at custom path: %v", err)
	}

	if result.Created != 1 {
		t.Fatalf("expected 1 created, got %d", result.Created)
	}
}

func TestBootstrapChannel_DefaultChannelsDir(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	if _, err := s.BootstrapProject(dir, "default-dir-test", false); err != nil {
		t.Fatalf("bootstrap project: %v", err)
	}

	root := filepath.Join(dir, "default-dir-test")
	_, err := s.BootstrapChannel(root, "new-ch", "", false)
	if err != nil {
		t.Fatalf("bootstrap channel: %v", err)
	}

	channelYAML := filepath.Join(root, "src", "channels", "new-ch", "channel.yaml")
	if _, err := os.Stat(channelYAML); err != nil {
		t.Fatalf("expected channel.yaml at default path: %v", err)
	}
}

// ---------------------------------------------------------------------------
// BootstrapChannel: idempotent without force
// ---------------------------------------------------------------------------

func TestBootstrapChannel_IdempotentWithoutForce(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	if _, err := s.BootstrapProject(dir, "idem-test", false); err != nil {
		t.Fatalf("bootstrap project: %v", err)
	}

	root := filepath.Join(dir, "idem-test")
	if _, err := s.BootstrapChannel(root, "idem-ch", "", false); err != nil {
		t.Fatalf("first bootstrap channel: %v", err)
	}

	result, err := s.BootstrapChannel(root, "idem-ch", "", false)
	if err != nil {
		t.Fatalf("second bootstrap channel: %v", err)
	}

	if result.Created != 0 {
		t.Fatalf("expected 0 created on second run, got %d", result.Created)
	}
	if result.Skipped != 1 {
		t.Fatalf("expected 1 skipped on second run, got %d", result.Skipped)
	}
}

// ---------------------------------------------------------------------------
// BootstrapChannel: force overwrites
// ---------------------------------------------------------------------------

func TestBootstrapChannel_ForceOverwrites(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	if _, err := s.BootstrapProject(dir, "force-ch-test", false); err != nil {
		t.Fatalf("bootstrap project: %v", err)
	}

	root := filepath.Join(dir, "force-ch-test")
	if _, err := s.BootstrapChannel(root, "force-ch", "", false); err != nil {
		t.Fatalf("first bootstrap channel: %v", err)
	}

	channelYAML := filepath.Join(root, "src", "channels", "force-ch", "channel.yaml")
	if err := os.WriteFile(channelYAML, []byte("modified: true"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	result, err := s.BootstrapChannel(root, "force-ch", "", true)
	if err != nil {
		t.Fatalf("force bootstrap channel: %v", err)
	}

	if result.Overwritten != 1 {
		t.Fatalf("expected 1 overwritten, got %d", result.Overwritten)
	}

	data, err := os.ReadFile(channelYAML)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.Contains(string(data), "modified") {
		t.Fatal("expected overwritten content, still has 'modified'")
	}
}

// ---------------------------------------------------------------------------
// BootstrapChannel: channel.yaml contains channel id
// ---------------------------------------------------------------------------

func TestBootstrapChannel_ContainsChannelID(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	if _, err := s.BootstrapProject(dir, "id-test", false); err != nil {
		t.Fatalf("bootstrap project: %v", err)
	}

	root := filepath.Join(dir, "id-test")
	if _, err := s.BootstrapChannel(root, "my-special-channel", "", false); err != nil {
		t.Fatalf("bootstrap channel: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(root, "src", "channels", "my-special-channel", "channel.yaml"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(data), "id: my-special-channel") {
		t.Fatalf("channel.yaml should contain channel id, got: %s", string(data))
	}
}

// ---------------------------------------------------------------------------
// writeFile helper: error when path is a directory
// ---------------------------------------------------------------------------

func TestWriteFile_PathIsDirectory(t *testing.T) {
	dir := t.TempDir()
	subDir := filepath.Join(dir, "existing-dir")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatalf("create dir: %v", err)
	}

	_, err := writeFile(subDir, "content", false)
	if err == nil {
		t.Fatal("expected error when path is a directory")
	}
	if !strings.Contains(err.Error(), "exists as a directory") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// projectFiles: keys are consistent
// ---------------------------------------------------------------------------

func TestProjectFiles_ContainsExpectedKeys(t *testing.T) {
	files := projectFiles("test-proj")

	expectedKeys := []string{
		"intu.yaml",
		"package.json",
		"tsconfig.json",
		"Dockerfile",
		"docker-compose.yml",
		".gitignore",
		".dockerignore",
		".env",
		"README.md",
	}

	for _, key := range expectedKeys {
		if _, ok := files[key]; !ok {
			t.Fatalf("expected key %q in projectFiles", key)
		}
	}
}

// ---------------------------------------------------------------------------
// channelFiles
// ---------------------------------------------------------------------------

func TestChannelFiles_Default(t *testing.T) {
	files := channelFiles("src/channels", "test-ch")
	expected := "src/channels/test-ch/channel.yaml"
	if _, ok := files[expected]; !ok {
		t.Fatalf("expected key %q, got %v", expected, files)
	}
}

func TestChannelFiles_NestedName(t *testing.T) {
	files := channelFiles("src/channels", "vendor/custom-ch")
	expected := "src/channels/vendor/custom-ch/channel.yaml"
	if _, ok := files[expected]; !ok {
		t.Fatalf("expected key %q, got %v", expected, files)
	}
	// The id in the YAML should be just the base name
	content := files[expected]
	if !strings.Contains(content, "id: custom-ch") {
		t.Fatalf("expected id: custom-ch in content, got: %s", content)
	}
}

// ---------------------------------------------------------------------------
// BootstrapProject: dev and prod profiles
// ---------------------------------------------------------------------------

func TestBootstrapProject_DevProfile(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "profile-test", false)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(result.Root, "intu.dev.yaml"))
	if err != nil {
		t.Fatalf("read dev profile: %v", err)
	}
	if !strings.Contains(string(data), "profile: dev") {
		t.Fatal("intu.dev.yaml should contain 'profile: dev'")
	}
}

func TestBootstrapProject_ProdProfile(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "profile-test-2", false)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(result.Root, "intu.prod.yaml"))
	if err != nil {
		t.Fatalf("read prod profile: %v", err)
	}
	if !strings.Contains(string(data), "profile: prod") {
		t.Fatal("intu.prod.yaml should contain 'profile: prod'")
	}
}

// ---------------------------------------------------------------------------
// BootstrapProject: sample channels exist
// ---------------------------------------------------------------------------

func TestBootstrapProject_SampleChannels(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "sample-ch-test", false)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	for _, ch := range []string{"http-to-file", "fhir-to-adt"} {
		chDir := filepath.Join(result.Root, "src", "channels", ch)
		for _, f := range []string{"channel.yaml", "transformer.ts", "validator.ts"} {
			path := filepath.Join(chDir, f)
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("expected %s/%s: %v", ch, f, err)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// BootstrapProject: .vscode directory
// ---------------------------------------------------------------------------

func TestBootstrapProject_VSCode(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "vscode-test", false)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	for _, f := range []string{".vscode/settings.json", ".vscode/extensions.json"} {
		path := filepath.Join(result.Root, f)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s: %v", f, err)
		}
	}
}
