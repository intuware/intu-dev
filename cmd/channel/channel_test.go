package channel

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/intuware/intu-dev/internal/bootstrap"
)

// scaffoldProject creates a full intu project in a temp dir and returns the project root.
func scaffoldProject(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	scaffolder := bootstrap.NewScaffolder(logger)
	result, err := scaffolder.BootstrapProject(dir, "test-proj", false)
	if err != nil {
		t.Fatalf("scaffold project: %v", err)
	}
	return result.Root
}

func TestNewChannelCmd(t *testing.T) {
	logLevel := "info"
	cmd := NewChannelCmd(&logLevel)
	if cmd == nil {
		t.Fatal("NewChannelCmd() returned nil")
	}
	if cmd.Use != "channel" {
		t.Fatalf("expected Use 'channel', got %q", cmd.Use)
	}
}

func TestChannelSubcommandsRegistered(t *testing.T) {
	logLevel := "info"
	cmd := NewChannelCmd(&logLevel)

	expected := map[string]bool{
		"add":      false,
		"list":     false,
		"describe": false,
		"clone":    false,
		"export":   false,
		"import":   false,
	}

	for _, sub := range cmd.Commands() {
		if _, ok := expected[sub.Name()]; ok {
			expected[sub.Name()] = true
		}
	}

	for name, found := range expected {
		if !found {
			t.Errorf("channel command missing subcommand %q", name)
		}
	}
}

func TestAddCmd(t *testing.T) {
	logLevel := "info"
	cmd := newAddCmd(&logLevel)
	if cmd == nil {
		t.Fatal("newAddCmd() returned nil")
	}
	if cmd.Use != "add [channel-name]" {
		t.Fatalf("expected Use 'add [channel-name]', got %q", cmd.Use)
	}
}

func TestAddCmdCreatesChannel(t *testing.T) {
	projectDir := scaffoldProject(t)

	logLevel := "info"
	cmd := NewChannelCmd(&logLevel)
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"add", "test-channel", "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("channel add failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Channel created: test-channel") {
		t.Fatalf("expected creation message, got %q", buf.String())
	}

	channelYAML := filepath.Join(projectDir, "src", "channels", "test-channel", "channel.yaml")
	if _, err := os.Stat(channelYAML); err != nil {
		t.Fatalf("expected channel.yaml at %s: %v", channelYAML, err)
	}
}

func TestListCmd(t *testing.T) {
	logLevel := "info"
	cmd := newListCmd(&logLevel)
	if cmd == nil {
		t.Fatal("newListCmd() returned nil")
	}
	if cmd.Use != "list" {
		t.Fatalf("expected Use 'list', got %q", cmd.Use)
	}
}

func TestListCmdWithProject(t *testing.T) {
	projectDir := scaffoldProject(t)

	logLevel := "info"
	cmd := NewChannelCmd(&logLevel)
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"list", "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("channel list failed: %v", err)
	}
	output := buf.String()
	if len(output) == 0 {
		t.Fatal("expected list output, got empty string")
	}
}

func TestDescribeCmd(t *testing.T) {
	logLevel := "info"
	cmd := newDescribeCmd(&logLevel)
	if cmd == nil {
		t.Fatal("newDescribeCmd() returned nil")
	}
	if cmd.Use != "describe [channel-id]" {
		t.Fatalf("expected Use 'describe [channel-id]', got %q", cmd.Use)
	}
}

func TestDescribeCmdWithProject(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, err := os.ReadDir(channelsDir)
	if err != nil {
		t.Fatalf("read channels dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("scaffolded project has no channels")
	}

	channelID := entries[0].Name()

	logLevel := "info"
	cmd := NewChannelCmd(&logLevel)
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"describe", channelID, "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("channel describe failed: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "id:") {
		t.Fatalf("expected 'id:' in describe output, got %q", output)
	}
}

func TestCloneCmd(t *testing.T) {
	logLevel := "info"
	cmd := newCloneCmd(&logLevel)
	if cmd == nil {
		t.Fatal("newCloneCmd() returned nil")
	}
	if cmd.Use != "clone <source-channel> <new-channel>" {
		t.Fatalf("expected Use 'clone <source-channel> <new-channel>', got %q", cmd.Use)
	}
}

func TestCloneCmdWithProject(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, err := os.ReadDir(channelsDir)
	if err != nil {
		t.Fatalf("read channels dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("scaffolded project has no channels")
	}

	sourceID := entries[0].Name()

	logLevel := "info"
	cmd := NewChannelCmd(&logLevel)
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"clone", sourceID, "cloned-channel", "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("channel clone failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Cloned channel") {
		t.Fatalf("expected clone message, got %q", buf.String())
	}

	clonedDir := filepath.Join(channelsDir, "cloned-channel")
	if _, err := os.Stat(filepath.Join(clonedDir, "channel.yaml")); err != nil {
		t.Fatalf("expected cloned channel.yaml: %v", err)
	}
}

func TestExportCmd(t *testing.T) {
	logLevel := "info"
	cmd := newExportCmd(&logLevel)
	if cmd == nil {
		t.Fatal("newExportCmd() returned nil")
	}
	if !strings.HasPrefix(cmd.Use, "export") {
		t.Fatalf("expected Use to start with 'export', got %q", cmd.Use)
	}
}

func TestExportCmdWithProject(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, err := os.ReadDir(channelsDir)
	if err != nil {
		t.Fatalf("read channels dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("scaffolded project has no channels")
	}

	channelID := entries[0].Name()
	outputFile := filepath.Join(t.TempDir(), channelID+".tar.gz")

	logLevel := "info"
	cmd := NewChannelCmd(&logLevel)
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"export", channelID, "--dir", projectDir, "-o", outputFile})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("channel export failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Exported channel") {
		t.Fatalf("expected export message, got %q", buf.String())
	}
	if _, err := os.Stat(outputFile); err != nil {
		t.Fatalf("expected archive at %s: %v", outputFile, err)
	}
}

func TestImportCmd(t *testing.T) {
	logLevel := "info"
	cmd := newImportCmd(&logLevel)
	if cmd == nil {
		t.Fatal("newImportCmd() returned nil")
	}
	if !strings.HasPrefix(cmd.Use, "import") {
		t.Fatalf("expected Use to start with 'import', got %q", cmd.Use)
	}
}

func TestExportCmdAllChannels(t *testing.T) {
	projectDir := scaffoldProject(t)
	outputDir := t.TempDir()

	logLevel := "info"
	cmd := NewChannelCmd(&logLevel)
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"export", "--dir", projectDir, "-o", outputDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("channel export (all) failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Exported channel") {
		t.Fatalf("expected export message, got %q", buf.String())
	}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("read output dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one .tar.gz in output dir")
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".tar.gz") {
			t.Errorf("expected only .tar.gz files, got %s", e.Name())
		}
	}
}

func TestExportThenImport(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, err := os.ReadDir(channelsDir)
	if err != nil {
		t.Fatalf("read channels dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("scaffolded project has no channels")
	}

	channelID := entries[0].Name()
	archiveFile := filepath.Join(t.TempDir(), channelID+".tar.gz")

	logLevel := "info"

	exportCmd := NewChannelCmd(&logLevel)
	exportCmd.SetOut(io.Discard)
	exportCmd.SetErr(io.Discard)
	exportCmd.SetArgs([]string{"export", channelID, "--dir", projectDir, "-o", archiveFile})
	if err := exportCmd.Execute(); err != nil {
		t.Fatalf("export failed: %v", err)
	}

	importProjectDir := scaffoldProject(t)
	importCmd := NewChannelCmd(&logLevel)
	var buf bytes.Buffer
	importCmd.SetOut(&buf)
	importCmd.SetErr(&buf)
	importCmd.SetArgs([]string{"import", archiveFile, "--dir", importProjectDir, "--force"})
	if err := importCmd.Execute(); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Imported channel") {
		t.Fatalf("expected import message, got %q", buf.String())
	}
}

func TestReplaceChannelID(t *testing.T) {
	content := "id: old-channel\nenabled: true\nlistener:\n  type: http\n"
	got := replaceChannelID(content, "old-channel", "new-channel")
	if !strings.Contains(got, "id: new-channel") {
		t.Errorf("expected 'id: new-channel', got:\n%s", got)
	}
	if strings.Contains(got, "id: old-channel") {
		t.Errorf("should not contain old ID, got:\n%s", got)
	}
}

func TestSplitLines(t *testing.T) {
	lines := splitLines("a\nb\nc")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}
	if lines[0] != "a" || lines[1] != "b" || lines[2] != "c" {
		t.Errorf("unexpected lines: %v", lines)
	}
}

func TestContainsTag(t *testing.T) {
	tags := []string{"hl7", "fhir", "lab"}
	if !containsTag(tags, "fhir") {
		t.Error("expected containsTag to find 'fhir'")
	}
	if containsTag(tags, "missing") {
		t.Error("expected containsTag to not find 'missing'")
	}
	if containsTag(nil, "any") {
		t.Error("expected containsTag on nil to return false")
	}
}
