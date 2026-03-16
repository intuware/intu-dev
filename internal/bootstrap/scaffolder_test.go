package bootstrap

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBootstrapProjectCreatesProject(t *testing.T) {
	dir := t.TempDir()
	scaffolder := NewScaffolder(slog.Default())

	result, err := scaffolder.BootstrapProject(dir, "test-project", false, false)
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

	// AGENTS.md and Cursor rule are created for AI guidance
	agentsPath := filepath.Join(root, "AGENTS.md")
	if _, err := os.Stat(agentsPath); err != nil {
		t.Fatalf("expected AGENTS.md to exist: %v", err)
	}
	agentsContent, err := os.ReadFile(agentsPath)
	if err != nil {
		t.Fatalf("read AGENTS.md: %v", err)
	}
	for _, key := range []string{"listener", "intu validate", "destinations"} {
		if !strings.Contains(string(agentsContent), key) {
			t.Errorf("AGENTS.md should contain %q", key)
		}
	}

	rulePath := filepath.Join(root, ".cursor", "rules", "intu-yaml.mdc")
	if _, err := os.Stat(rulePath); err != nil {
		t.Fatalf("expected .cursor/rules/intu-yaml.mdc to exist: %v", err)
	}
}

func TestBootstrapProjectErrorsWhenDirExists(t *testing.T) {
	dir := t.TempDir()
	scaffolder := NewScaffolder(slog.Default())

	if _, err := scaffolder.BootstrapProject(dir, "test", false, false); err != nil {
		t.Fatalf("first bootstrap failed: %v", err)
	}

	_, err := scaffolder.BootstrapProject(dir, "test", false, false)
	if err == nil {
		t.Fatal("second bootstrap without force should fail when project dir exists")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected error to mention directory already exists, got: %v", err)
	}
}

func TestBootstrapProjectForceOverwritesFiles(t *testing.T) {
	dir := t.TempDir()
	scaffolder := NewScaffolder(slog.Default())

	if _, err := scaffolder.BootstrapProject(dir, "test", false, false); err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}

	samplePath := filepath.Join(dir, "test", "intu.yaml")
	if err := os.WriteFile(samplePath, []byte("mutated: true\n"), 0o644); err != nil {
		t.Fatalf("mutate file: %v", err)
	}

	result, err := scaffolder.BootstrapProject(dir, "test", true, false)
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
	if _, err := scaffolder.BootstrapProject(dir, "test", false, false); err != nil {
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

func TestScaffoldedChannelsContainDescription(t *testing.T) {
	dir := t.TempDir()
	scaffolder := NewScaffolder(slog.Default())

	if _, err := scaffolder.BootstrapProject(dir, "test", false, false); err != nil {
		t.Fatalf("bootstrap project failed: %v", err)
	}

	root := filepath.Join(dir, "test")
	for _, ch := range []string{"http-to-file", "fhir-to-adt"} {
		data, err := os.ReadFile(filepath.Join(root, "src", "channels", ch, "channel.yaml"))
		if err != nil {
			t.Fatalf("read %s channel.yaml: %v", ch, err)
		}
		if !strings.Contains(string(data), "description:") {
			t.Fatalf("channel %s should contain description field", ch)
		}
	}
}
func TestNewScaffolder(t *testing.T) {
	s := NewScaffolder(slog.Default())
	if s == nil {
		t.Fatal("expected non-nil Scaffolder")
	}
}

func TestBootstrapProject_FileContents(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "my-project", false, false)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	root := result.Root

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

	data, err = os.ReadFile(filepath.Join(root, "package.json"))
	if err != nil {
		t.Fatalf("read package.json: %v", err)
	}
	if !strings.Contains(string(data), "intu-channel-runtime") {
		t.Fatal("package.json should contain intu-channel-runtime")
	}

	if _, err := os.Stat(filepath.Join(root, "tsconfig.json")); err != nil {
		t.Fatalf("expected tsconfig.json: %v", err)
	}

	if _, err := os.Stat(filepath.Join(root, "Dockerfile")); err != nil {
		t.Fatalf("expected Dockerfile: %v", err)
	}

	data, err = os.ReadFile(filepath.Join(root, "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read docker-compose.yml: %v", err)
	}
	if !strings.Contains(string(data), "my-project") {
		t.Fatal("docker-compose.yml should contain project name")
	}

	if _, err := os.Stat(filepath.Join(root, ".gitignore")); err != nil {
		t.Fatalf("expected .gitignore: %v", err)
	}

	if _, err := os.Stat(filepath.Join(root, "src", "types", "intu.d.ts")); err != nil {
		t.Fatalf("expected intu.d.ts: %v", err)
	}
}

func TestBootstrapProject_Directories(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "dir-test", false, false)
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

func TestBootstrapProject_ResultCreatedCount(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "count-test", false, false)
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

func TestBootstrapChannel_CustomChannelsDir(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	if _, err := s.BootstrapProject(dir, "ch-dir-test", false, false); err != nil {
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

	if _, err := s.BootstrapProject(dir, "default-dir-test", false, false); err != nil {
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

func TestBootstrapChannel_IdempotentWithoutForce(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	if _, err := s.BootstrapProject(dir, "idem-test", false, false); err != nil {
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

func TestBootstrapChannel_ForceOverwrites(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	if _, err := s.BootstrapProject(dir, "force-ch-test", false, false); err != nil {
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

func TestBootstrapChannel_ContainsChannelID(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	if _, err := s.BootstrapProject(dir, "id-test", false, false); err != nil {
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

	content := files[expected]
	if !strings.Contains(content, "id: custom-ch") {
		t.Fatalf("expected id: custom-ch in content, got: %s", content)
	}
}

func TestBootstrapProject_DevProfile(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "profile-test", false, false)
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

	result, err := s.BootstrapProject(dir, "profile-test-2", false, false)
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

func TestBootstrapProject_SampleChannels(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "sample-ch-test", false, false)
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

func TestBootstrapProject_VSCode(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "vscode-test", false, false)
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

func TestBootstrapProject_InPlace(t *testing.T) {
	dir := t.TempDir()
	s := NewScaffolder(slog.Default())

	result, err := s.BootstrapProject(dir, "sample", false, true)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	// In-place: root is dir, not dir/sample
	if result.Root != dir {
		t.Fatalf("expected root %q when in-place, got %q", dir, result.Root)
	}
	if _, err := os.Stat(filepath.Join(dir, "intu.yaml")); err != nil {
		t.Fatalf("expected intu.yaml in dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "sample")); err == nil {
		t.Fatal("expected no project-name subfolder when in-place")
	}
}
