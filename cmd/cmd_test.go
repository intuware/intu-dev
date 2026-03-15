package cmd

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/intuware/intu-dev/internal/bootstrap"
	"github.com/intuware/intu-dev/pkg/config"
	"github.com/spf13/cobra"
)

func TestExecute(t *testing.T) {
	old := rootCmd.Args
	rootCmd.SetArgs([]string{"--help"})
	rootCmd.SetOut(io.Discard)
	rootCmd.SetErr(io.Discard)
	defer func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.Args = old
	}()

	if err := Execute(); err != nil {
		t.Fatalf("Execute() with --help should not error: %v", err)
	}
}

func TestNewInitCmd(t *testing.T) {
	cmd := newInitCmd()
	if cmd == nil {
		t.Fatal("newInitCmd() returned nil")
	}
	if cmd.Use != "init [project-name]" {
		t.Fatalf("expected Use 'init [project-name]', got %q", cmd.Use)
	}
}

func TestNewCCmd(t *testing.T) {
	cmd := newCCmd()
	if cmd == nil {
		t.Fatal("newCCmd() returned nil")
	}
	if cmd.Use != "c [channel-name]" {
		t.Fatalf("expected Use 'c [channel-name]', got %q", cmd.Use)
	}
}

func TestNewValidateCmd(t *testing.T) {
	cmd := newValidateCmd()
	if cmd == nil {
		t.Fatal("newValidateCmd() returned nil")
	}
	if cmd.Use != "validate" {
		t.Fatalf("expected Use 'validate', got %q", cmd.Use)
	}
}

func TestNewBuildCmd(t *testing.T) {
	cmd := newBuildCmd()
	if cmd == nil {
		t.Fatal("newBuildCmd() returned nil")
	}
	if cmd.Use != "build" {
		t.Fatalf("expected Use 'build', got %q", cmd.Use)
	}
}

func TestNewServeCmd(t *testing.T) {
	cmd := newServeCmd()
	if cmd == nil {
		t.Fatal("newServeCmd() returned nil")
	}
	if cmd.Use != "serve" {
		t.Fatalf("expected Use 'serve', got %q", cmd.Use)
	}
}

func TestNewDeployCmd(t *testing.T) {
	cmd := newDeployCmd()
	if cmd == nil {
		t.Fatal("newDeployCmd() returned nil")
	}
	if cmd.Use != "deploy [channel-id]" {
		t.Fatalf("expected Use 'deploy [channel-id]', got %q", cmd.Use)
	}
}

func TestNewUndeployCmd(t *testing.T) {
	cmd := newUndeployCmd()
	if cmd == nil {
		t.Fatal("newUndeployCmd() returned nil")
	}
	if cmd.Use != "undeploy [channel-id]" {
		t.Fatalf("expected Use 'undeploy [channel-id]', got %q", cmd.Use)
	}
}

func TestNewEnableCmd(t *testing.T) {
	cmd := newEnableCmd()
	if cmd == nil {
		t.Fatal("newEnableCmd() returned nil")
	}
	if cmd.Use != "enable [channel-id]" {
		t.Fatalf("expected Use 'enable [channel-id]', got %q", cmd.Use)
	}
}

func TestNewDisableCmd(t *testing.T) {
	cmd := newDisableCmd()
	if cmd == nil {
		t.Fatal("newDisableCmd() returned nil")
	}
	if cmd.Use != "disable [channel-id]" {
		t.Fatalf("expected Use 'disable [channel-id]', got %q", cmd.Use)
	}
}

func TestNewStatsCmd(t *testing.T) {
	cmd := newStatsCmd()
	if cmd == nil {
		t.Fatal("newStatsCmd() returned nil")
	}
	if cmd.Use != "stats [channel-id]" {
		t.Fatalf("expected Use 'stats [channel-id]', got %q", cmd.Use)
	}
}

func TestNewPruneCmd(t *testing.T) {
	cmd := newPruneCmd()
	if cmd == nil {
		t.Fatal("newPruneCmd() returned nil")
	}
	if cmd.Use != "prune" {
		t.Fatalf("expected Use 'prune', got %q", cmd.Use)
	}
}

func TestNewDashboardCmd(t *testing.T) {
	cmd := newDashboardCmd()
	if cmd == nil {
		t.Fatal("newDashboardCmd() returned nil")
	}
	if cmd.Use != "dashboard" {
		t.Fatalf("expected Use 'dashboard', got %q", cmd.Use)
	}
}

func TestNewMessageCmd(t *testing.T) {
	cmd := newMessageCmd()
	if cmd == nil {
		t.Fatal("newMessageCmd() returned nil")
	}
	if cmd.Use != "message" {
		t.Fatalf("expected Use 'message', got %q", cmd.Use)
	}
	subs := cmd.Commands()
	names := make(map[string]bool)
	for _, s := range subs {
		names[s.Name()] = true
	}
	for _, expected := range []string{"list", "get", "count"} {
		if !names[expected] {
			t.Errorf("message cmd missing subcommand %q", expected)
		}
	}
}

func TestNewReprocessCmd(t *testing.T) {
	cmd := newReprocessCmd()
	if cmd == nil {
		t.Fatal("newReprocessCmd() returned nil")
	}
	if !strings.HasPrefix(cmd.Use, "reprocess") {
		t.Fatalf("expected Use to start with 'reprocess', got %q", cmd.Use)
	}
	subs := cmd.Commands()
	names := make(map[string]bool)
	for _, s := range subs {
		names[s.Name()] = true
	}
	for _, expected := range []string{"message", "batch"} {
		if !names[expected] {
			t.Errorf("reprocess cmd missing subcommand %q", expected)
		}
	}
}

func TestNewImportCmd(t *testing.T) {
	cmd := newImportCmd()
	if cmd == nil {
		t.Fatal("newImportCmd() returned nil")
	}
	if cmd.Use != "import" {
		t.Fatalf("expected Use 'import', got %q", cmd.Use)
	}
}

func TestNewChannelCmd(t *testing.T) {
	cmd := newChannelCmd()
	if cmd == nil {
		t.Fatal("newChannelCmd() returned nil")
	}
	if cmd.Use != "channel" {
		t.Fatalf("expected Use 'channel', got %q", cmd.Use)
	}
}

// scaffoldProject creates a minimal intu project in a temp dir and returns the project root.
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

func TestValidateProjectWithScaffoldedProject(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := &cobra.Command{}
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)

	errs, err := validateProject(cmd, projectDir, "dev")
	if err != nil {
		t.Fatalf("validateProject returned error: %v", err)
	}
	if len(errs) > 0 {
		t.Fatalf("validateProject returned validation errors: %v", errs)
	}
	if !strings.Contains(buf.String(), "Validation passed") {
		t.Fatalf("expected 'Validation passed' in output, got %q", buf.String())
	}
}

func TestValidateCmdWithDirFlag(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newValidateCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("validate command failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Validation passed") {
		t.Fatalf("expected 'Validation passed' in output, got %q", buf.String())
	}
}

func TestInitCmdCreatesProject(t *testing.T) {
	dir := t.TempDir()

	cmd := newInitCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"new-project", "--dir", dir})

	err := cmd.Execute()
	projectRoot := filepath.Join(dir, "new-project")
	if _, statErr := os.Stat(filepath.Join(projectRoot, "intu.yaml")); statErr != nil {
		if err != nil {
			t.Logf("init command returned error (possibly npm install): %v", err)
		}
		t.Fatalf("expected intu.yaml in %s: %v", projectRoot, statErr)
	}
}

func TestBuildCmdHasDirFlag(t *testing.T) {
	cmd := newBuildCmd()
	if cmd == nil {
		t.Fatal("newBuildCmd() returned nil")
	}
	f := cmd.Flags().Lookup("dir")
	if f == nil {
		t.Fatal("build cmd missing --dir flag")
	}
}

func TestCCmdCreatesChannel(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newCCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"my-new-channel", "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("c command failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Channel created: my-new-channel") {
		t.Fatalf("expected channel creation message, got %q", buf.String())
	}

	channelYAML := filepath.Join(projectDir, "src", "channels", "my-new-channel", "channel.yaml")
	if _, err := os.Stat(channelYAML); err != nil {
		t.Fatalf("expected channel.yaml at %s: %v", channelYAML, err)
	}
}

func TestPruneCmdDryRun(t *testing.T) {
	cmd := newPruneCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--all", "--dry-run"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("prune --all --dry-run failed: %v", err)
	}
	if !strings.Contains(buf.String(), "DRY RUN") {
		t.Fatalf("expected 'DRY RUN' in output, got %q", buf.String())
	}
}

func TestPruneCmdRequiresChannelOrAll(t *testing.T) {
	cmd := newPruneCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when neither --channel nor --all specified")
	}
	if !strings.Contains(err.Error(), "specify --channel or --all") {
		t.Fatalf("expected specific error message, got: %v", err)
	}
}

func TestSetChannelEnabled(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, err := os.ReadDir(channelsDir)
	if err != nil {
		t.Fatalf("read channels dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("scaffolded project has no channels")
	}

	channelDir := filepath.Join(channelsDir, entries[0].Name())
	channelID := entries[0].Name()

	if err := setChannelEnabled(channelDir, channelID, false); err != nil {
		t.Fatalf("setChannelEnabled(false) failed: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(channelDir, "channel.yaml"))
	if err != nil {
		t.Fatalf("read channel.yaml: %v", err)
	}
	if !strings.Contains(string(data), "enabled: false") {
		t.Fatalf("expected 'enabled: false' in channel.yaml, got:\n%s", string(data))
	}

	if err := setChannelEnabled(channelDir, channelID, true); err != nil {
		t.Fatalf("setChannelEnabled(true) failed: %v", err)
	}

	data, err = os.ReadFile(filepath.Join(channelDir, "channel.yaml"))
	if err != nil {
		t.Fatalf("read channel.yaml: %v", err)
	}
	if !strings.Contains(string(data), "enabled: true") {
		t.Fatalf("expected 'enabled: true' in channel.yaml, got:\n%s", string(data))
	}
}

func TestStatsCmdWithScaffoldedProject(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newStatsCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("stats command failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Channel:") {
		t.Fatalf("expected 'Channel:' in stats output, got %q", buf.String())
	}
}

func TestStatsCmdJSONOutput(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newStatsCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir, "--json"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("stats --json command failed: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, `"channel"`) {
		t.Fatalf("expected JSON output with 'channel' key, got %q", output)
	}
}

func TestBuildAuthMiddlewareNilAccessControl(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{}
	mw := buildAuthMiddleware(cfg, logger)
	if mw != nil {
		t.Fatal("expected nil middleware for nil access control")
	}
}

func TestSanitizeID(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"My Channel", "my-channel"},
		{"test_channel-01", "test_channel-01"},
		{"  spaces  ", "spaces"},
		{"Special!@#Chars", "specialchars"},
		{"", ""},
	}
	for _, tt := range tests {
		got := sanitizeID(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeID(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseMirthChannel(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>test-123</id>
  <name>Test Channel</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>HTTP Listener</transportName>
    <properties class="com.mirth.connect.connectors.http.HttpReceiverProperties">
      <listenerPort>8080</listenerPort>
    </properties>
    <transformer><steps></steps></transformer>
    <filter><rules></rules></filter>
  </sourceConnector>
  <destinationConnectors>
    <connector>
      <name>File Writer</name>
      <transportName>File Writer</transportName>
      <properties>
        <directory>/tmp/output</directory>
      </properties>
      <transformer><steps></steps></transformer>
      <filter><rules></rules></filter>
    </connector>
  </destinationConnectors>
</channel>`

	ch, warnings, err := parseMirthChannel([]byte(xml))
	if err != nil {
		t.Fatalf("parseMirthChannel failed: %v", err)
	}
	if ch.ID != "test-channel" {
		t.Errorf("expected ID 'test-channel', got %q", ch.ID)
	}
	if ch.ListenerType != "http" {
		t.Errorf("expected listener type 'http', got %q", ch.ListenerType)
	}
	if len(ch.Destinations) != 1 {
		t.Errorf("expected 1 destination, got %d", len(ch.Destinations))
	}
	if ch.Destinations[0].Type != "file" {
		t.Errorf("expected destination type 'file', got %q", ch.Destinations[0].Type)
	}
	_ = warnings
}

func TestParseMirthChannelVariousConnectors(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>multi-test</id>
  <name>Multi Connector Test</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>TCP Listener</transportName>
    <properties>
      <listenerPort>6661</listenerPort>
      <mode>MLLP</mode>
    </properties>
    <transformer><steps></steps></transformer>
    <filter><rules></rules></filter>
  </sourceConnector>
  <destinationConnectors>
    <connector>
      <name>HTTP Dest</name>
      <transportName>HTTP Sender</transportName>
      <properties>
        <url>http://example.com/api</url>
        <method>POST</method>
      </properties>
      <transformer><steps></steps></transformer>
      <filter><rules></rules></filter>
    </connector>
    <connector>
      <name>TCP Dest</name>
      <transportName>TCP Sender</transportName>
      <properties>
        <remoteAddress>10.0.0.1</remoteAddress>
        <remotePort>6662</remotePort>
      </properties>
      <transformer><steps></steps></transformer>
      <filter><rules></rules></filter>
    </connector>
  </destinationConnectors>
</channel>`

	ch, _, err := parseMirthChannel([]byte(xml))
	if err != nil {
		t.Fatalf("parseMirthChannel failed: %v", err)
	}
	if ch.ListenerType != "tcp" {
		t.Errorf("expected listener type 'tcp', got %q", ch.ListenerType)
	}
	if len(ch.Destinations) != 2 {
		t.Fatalf("expected 2 destinations, got %d", len(ch.Destinations))
	}
	if ch.Destinations[0].Type != "http" {
		t.Errorf("expected first dest type 'http', got %q", ch.Destinations[0].Type)
	}
	if ch.Destinations[1].Type != "tcp" {
		t.Errorf("expected second dest type 'tcp', got %q", ch.Destinations[1].Type)
	}
}

func TestMapMirthDBDriver(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"com.postgresql.Driver", "postgres"},
		{"com.mysql.jdbc.Driver", "mysql"},
		{"oracle.jdbc.driver.OracleDriver", "oracle"},
		{"net.sourceforge.jtds.jdbc.Driver", "sqlserver"},
		{"com.microsoft.sqlserver.jdbc.SQLServerDriver", "sqlserver"},
		{"unknown.driver", "unknown.driver"},
	}
	for _, tt := range tests {
		got := mapMirthDBDriver(tt.input)
		if got != tt.want {
			t.Errorf("mapMirthDBDriver(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestRootCmdSubcommands(t *testing.T) {
	expected := map[string]bool{
		"init":      false,
		"c":         false,
		"channel":   false,
		"serve":     false,
		"validate":  false,
		"build":     false,
		"stats":     false,
		"deploy":    false,
		"undeploy":  false,
		"enable":    false,
		"disable":   false,
		"prune":     false,
		"dashboard": false,
		"message":   false,
		"reprocess": false,
		"import":    false,
	}

	for _, sub := range rootCmd.Commands() {
		if _, ok := expected[sub.Name()]; ok {
			expected[sub.Name()] = true
		}
	}

	for name, found := range expected {
		if !found {
			t.Errorf("root command missing subcommand %q", name)
		}
	}
}

func TestIndentCode(t *testing.T) {
	input := "line1\nline2\nline3"
	got := indentCode(input, "  ")
	if !strings.Contains(got, "  line2") {
		t.Errorf("expected indented second line, got %q", got)
	}
	if strings.HasPrefix(got, "  ") {
		t.Errorf("first line should not be indented, got %q", got)
	}
}

func TestWriteChannelYAML(t *testing.T) {
	dir := t.TempDir()
	ch := &mirthImportChannel{
		ID:           "test-ch",
		Name:         "Test Channel",
		ListenerType: "http",
		ListenerConfig: map[string]any{
			"port": "8080",
		},
		Destinations: []mirthImportDest{
			{Name: "dest1", Type: "file", Config: map[string]any{"directory": "/tmp"}},
		},
	}
	if err := writeChannelYAML(dir, ch); err != nil {
		t.Fatalf("writeChannelYAML failed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "channel.yaml"))
	if err != nil {
		t.Fatalf("read channel.yaml: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "id: test-ch") {
		t.Errorf("expected 'id: test-ch' in YAML, got:\n%s", content)
	}
}

func TestWriteTransformerTS(t *testing.T) {
	dir := t.TempDir()
	if err := writeTransformerTS(dir, "var x = 1;"); err != nil {
		t.Fatalf("writeTransformerTS failed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "transformer.ts"))
	if err != nil {
		t.Fatalf("read transformer.ts: %v", err)
	}
	if !strings.Contains(string(data), "var x = 1;") {
		t.Errorf("expected JS code in transformer.ts, got:\n%s", string(data))
	}
}

func TestWriteValidatorTS(t *testing.T) {
	dir := t.TempDir()
	if err := writeValidatorTS(dir, "return msg.type === 'ADT';"); err != nil {
		t.Fatalf("writeValidatorTS failed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "validator.ts"))
	if err != nil {
		t.Fatalf("read validator.ts: %v", err)
	}
	if !strings.Contains(string(data), "return msg.type") {
		t.Errorf("expected JS code in validator.ts, got:\n%s", string(data))
	}
}
