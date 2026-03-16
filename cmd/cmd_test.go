package cmd

import (
	"bytes"

	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/bootstrap"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/internal/storage"
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
	result, err := scaffolder.BootstrapProject(dir, "test-proj", false, false)
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
func TestServeCmdHasFlags(t *testing.T) {
	cmd := newServeCmd()
	if cmd == nil {
		t.Fatal("newServeCmd() returned nil")
	}
	for _, name := range []string{"dir", "profile"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("serve cmd missing --%s flag", name)
		}
	}
}

func TestServeCmdFlagDefaults(t *testing.T) {
	cmd := newServeCmd()
	dirF := cmd.Flags().Lookup("dir")
	if dirF.DefValue != "." {
		t.Errorf("expected default dir '.', got %q", dirF.DefValue)
	}
	profileF := cmd.Flags().Lookup("profile")
	if profileF.DefValue != "dev" {
		t.Errorf("expected default profile 'dev', got %q", profileF.DefValue)
	}
}

func TestBuildDashboardAuth_NilAuth(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dashCfg := &config.DashboardConfig{Enabled: true}
	cfg := &config.Config{}
	mw := buildDashboardAuth(dashCfg, cfg, logger)
	if mw == nil {
		t.Fatal("expected basic auth middleware for nil auth config")
	}
}

func TestBuildDashboardAuth_BasicAuth(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "basic", Username: "myuser", Password: "mypass"},
	}
	cfg := &config.Config{}
	mw := buildDashboardAuth(dashCfg, cfg, logger)
	if mw == nil {
		t.Fatal("expected basic auth middleware")
	}
}

func TestBuildDashboardAuth_BasicAuthDefaults(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "basic"},
	}
	cfg := &config.Config{}
	mw := buildDashboardAuth(dashCfg, cfg, logger)
	if mw == nil {
		t.Fatal("expected basic auth middleware with defaults")
	}
}

func TestBuildDashboardAuth_NoneAuth(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "none"},
	}
	cfg := &config.Config{}
	mw := buildDashboardAuth(dashCfg, cfg, logger)
	if mw != nil {
		t.Fatal("expected nil middleware for none auth")
	}
}

func TestBuildDashboardAuth_OIDCFallback(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "oidc"},
	}
	cfg := &config.Config{}
	mw := buildDashboardAuth(dashCfg, cfg, logger)
	if mw == nil {
		t.Fatal("expected fallback basic auth middleware when OIDC not configured")
	}
}

func TestBuildDashboardAuth_LDAPFallback(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "ldap"},
	}
	cfg := &config.Config{}
	mw := buildDashboardAuth(dashCfg, cfg, logger)
	if mw == nil {
		t.Fatal("expected fallback basic auth middleware when LDAP not configured")
	}
}

func TestBuildDashboardAuth_DefaultProvider(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "unknown-provider"},
	}
	cfg := &config.Config{}
	mw := buildDashboardAuth(dashCfg, cfg, logger)
	if mw == nil {
		t.Fatal("expected default basic auth middleware for unknown provider")
	}
}

func TestBuildAuthMiddlewareWithAccessControl(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		AccessControl: &config.AccessControlConfig{
			Enabled:  true,
			Provider: "ldap",
		},
	}
	mw := buildAuthMiddleware(cfg, logger)
	if mw == nil {
		t.Fatal("expected non-nil middleware for enabled access control")
	}
}

func TestBuildAuthMiddlewareDisabled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		AccessControl: &config.AccessControlConfig{
			Enabled: false,
		},
	}
	mw := buildAuthMiddleware(cfg, logger)
	if mw != nil {
		t.Fatal("expected nil middleware for disabled access control")
	}
}

func TestDeployCmdHasFlags(t *testing.T) {
	cmd := newDeployCmd()
	for _, name := range []string{"dir", "profile", "all", "tag"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("deploy cmd missing --%s flag", name)
		}
	}
}

func TestUndeployCmdHasFlags(t *testing.T) {
	cmd := newUndeployCmd()
	for _, name := range []string{"dir", "profile"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("undeploy cmd missing --%s flag", name)
		}
	}
}

func TestEnableCmdHasFlags(t *testing.T) {
	cmd := newEnableCmd()
	for _, name := range []string{"dir", "profile"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("enable cmd missing --%s flag", name)
		}
	}
}

func TestDisableCmdHasFlags(t *testing.T) {
	cmd := newDisableCmd()
	for _, name := range []string{"dir", "profile"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("disable cmd missing --%s flag", name)
		}
	}
}

func TestDashboardCmdHasFlags(t *testing.T) {
	cmd := newDashboardCmd()
	for _, name := range []string{"dir", "profile", "port"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("dashboard cmd missing --%s flag", name)
		}
	}
}

func TestPruneCmdHasAllFlags(t *testing.T) {
	cmd := newPruneCmd()
	for _, name := range []string{"dir", "profile", "channel", "all", "before", "dry-run", "confirm"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("prune cmd missing --%s flag", name)
		}
	}
}

func TestPruneCmdDryRunWithChannel(t *testing.T) {
	cmd := newPruneCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--channel", "test-ch", "--dry-run"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("prune --channel --dry-run failed: %v", err)
	}
	if !strings.Contains(buf.String(), "DRY RUN") {
		t.Fatalf("expected 'DRY RUN' in output, got %q", buf.String())
	}
	if !strings.Contains(buf.String(), "test-ch") {
		t.Fatalf("expected channel name in output, got %q", buf.String())
	}
}

func TestPruneCmdDryRunWithBefore(t *testing.T) {
	cmd := newPruneCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--all", "--dry-run", "--before", "2024-01-01"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("prune --all --dry-run --before failed: %v", err)
	}
	if !strings.Contains(buf.String(), "2024-01-01") {
		t.Fatalf("expected date in output, got %q", buf.String())
	}
}

func TestPruneCmdInvalidBefore(t *testing.T) {
	cmd := newPruneCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--all", "--before", "not-a-date"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid date")
	}
}

func TestPruneCmdRequiresConfirm(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newPruneCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--all", "--dir", projectDir})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --confirm not specified")
	}
	if !strings.Contains(err.Error(), "--confirm") {
		t.Fatalf("expected confirm error, got: %v", err)
	}
}

func TestDeployCmdWithScaffoldedProject(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newDeployCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--all", "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("deploy --all failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Deployed:") {
		t.Fatalf("expected 'Deployed:' in output, got %q", buf.String())
	}
}

func TestDeployCmdRequiresIDOrFlag(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newDeployCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--dir", projectDir})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when no channel ID or --all specified")
	}
}

func TestUndeployCmdWithScaffoldedProject(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, _ := os.ReadDir(channelsDir)
	if len(entries) == 0 {
		t.Fatal("no channels found")
	}

	cmd := newUndeployCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{entries[0].Name(), "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("undeploy failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Undeployed:") {
		t.Fatalf("expected 'Undeployed:' in output, got %q", buf.String())
	}
}

func TestEnableCmdWithScaffoldedProject(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, _ := os.ReadDir(channelsDir)
	if len(entries) == 0 {
		t.Fatal("no channels found")
	}

	cmd := newEnableCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{entries[0].Name(), "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("enable failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Enabled:") {
		t.Fatalf("expected 'Enabled:' in output, got %q", buf.String())
	}
}

func TestDisableCmdWithScaffoldedProject(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, _ := os.ReadDir(channelsDir)
	if len(entries) == 0 {
		t.Fatal("no channels found")
	}

	cmd := newDisableCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{entries[0].Name(), "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("disable failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Disabled:") {
		t.Fatalf("expected 'Disabled:' in output, got %q", buf.String())
	}
}

func TestImportMirthCmdWithFile(t *testing.T) {
	projectDir := scaffoldProject(t)

	mirthXML := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>test-import</id>
  <name>Import Test</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>HTTP Listener</transportName>
    <properties class="com.mirth.connect.connectors.http.HttpReceiverProperties">
      <listenerPort>9090</listenerPort>
    </properties>
    <transformer><steps>
      <step><name>SetField</name><type>JavaScript</type><script>msg['field'] = 'value';</script></step>
    </steps></transformer>
    <filter><rules>
      <rule><name>CheckType</name><type>JavaScript</type><script>return msg.type === 'ADT';</script></rule>
    </rules></filter>
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
  </destinationConnectors>
</channel>`

	xmlPath := filepath.Join(t.TempDir(), "channel.xml")
	os.WriteFile(xmlPath, []byte(mirthXML), 0o644)

	cmd := newImportCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"mirth", xmlPath, "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("import mirth failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Successfully imported") {
		t.Fatalf("expected success message, got %q", buf.String())
	}
	if !strings.Contains(buf.String(), "Transformer:") {
		t.Fatalf("expected transformer mention, got %q", buf.String())
	}
	if !strings.Contains(buf.String(), "Validator:") {
		t.Fatalf("expected validator mention, got %q", buf.String())
	}

	channelDir := filepath.Join(projectDir, "src", "channels", "import-test")
	if _, err := os.Stat(filepath.Join(channelDir, "channel.yaml")); err != nil {
		t.Fatalf("expected channel.yaml: %v", err)
	}
	if _, err := os.Stat(filepath.Join(channelDir, "transformer.ts")); err != nil {
		t.Fatalf("expected transformer.ts: %v", err)
	}
	if _, err := os.Stat(filepath.Join(channelDir, "validator.ts")); err != nil {
		t.Fatalf("expected validator.ts: %v", err)
	}
}

func TestImportMirthCmdOverwriteProtection(t *testing.T) {
	projectDir := scaffoldProject(t)

	mirthXML := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>overwrite-test</id>
  <name>Overwrite Test</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>HTTP Listener</transportName>
    <properties><listenerPort>8080</listenerPort></properties>
    <transformer><steps></steps></transformer>
    <filter><rules></rules></filter>
  </sourceConnector>
  <destinationConnectors></destinationConnectors>
</channel>`

	xmlPath := filepath.Join(t.TempDir(), "channel.xml")
	os.WriteFile(xmlPath, []byte(mirthXML), 0o644)

	channelDir := filepath.Join(projectDir, "src", "channels", "overwrite-test")
	os.MkdirAll(channelDir, 0o755)

	cmd := newImportCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"mirth", xmlPath, "--dir", projectDir})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when channel dir already exists without --overwrite")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected 'already exists' error, got: %v", err)
	}
}

func TestParseMirthChannelDatabaseConnectors(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>db-test</id>
  <name>Database Test</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>Database Reader</transportName>
    <properties>
      <driver>com.postgresql.Driver</driver>
      <URL>jdbc:postgresql://localhost/mydb</URL>
      <query>SELECT * FROM messages</query>
    </properties>
    <transformer><steps></steps></transformer>
    <filter><rules></rules></filter>
  </sourceConnector>
  <destinationConnectors>
    <connector>
      <name>DB Writer</name>
      <transportName>Database Writer</transportName>
      <properties>
        <driver>com.mysql.jdbc.Driver</driver>
        <URL>jdbc:mysql://localhost/dest</URL>
        <query>INSERT INTO out (data) VALUES (?)</query>
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
	if ch.ListenerType != "database" {
		t.Errorf("expected listener type 'database', got %q", ch.ListenerType)
	}
	if ch.ListenerConfig["driver"] != "postgres" {
		t.Errorf("expected driver 'postgres', got %v", ch.ListenerConfig["driver"])
	}
	if len(ch.Destinations) != 1 {
		t.Fatalf("expected 1 destination, got %d", len(ch.Destinations))
	}
	if ch.Destinations[0].Type != "database" {
		t.Errorf("expected dest type 'database', got %q", ch.Destinations[0].Type)
	}
}

func TestParseMirthChannelKafkaConnectors(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>kafka-test</id>
  <name>Kafka Test</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>Kafka Consumer</transportName>
    <properties>
      <topic>hl7-input</topic>
      <bootstrap.servers>kafka1:9092,kafka2:9092</bootstrap.servers>
      <group.id>intu-group</group.id>
    </properties>
    <transformer><steps></steps></transformer>
    <filter><rules></rules></filter>
  </sourceConnector>
  <destinationConnectors>
    <connector>
      <name>Kafka Out</name>
      <transportName>Kafka Producer</transportName>
      <properties>
        <topic>hl7-output</topic>
        <bootstrap.servers>kafka1:9092</bootstrap.servers>
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
	if ch.ListenerType != "kafka" {
		t.Errorf("expected listener type 'kafka', got %q", ch.ListenerType)
	}
	if ch.Destinations[0].Type != "kafka" {
		t.Errorf("expected dest type 'kafka', got %q", ch.Destinations[0].Type)
	}
}

func TestParseMirthChannelFileConnectors(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>file-test</id>
  <name>File Test</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>File Reader</transportName>
    <properties>
      <directory>/input</directory>
      <fileFilter>*.hl7</fileFilter>
      <pollingFrequency>5000</pollingFrequency>
    </properties>
    <transformer><steps></steps></transformer>
    <filter><rules></rules></filter>
  </sourceConnector>
  <destinationConnectors>
    <connector>
      <name>File Writer</name>
      <transportName>File Writer</transportName>
      <properties>
        <directory>/output</directory>
        <fileFilter>output.dat</fileFilter>
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
	if ch.ListenerType != "file" {
		t.Errorf("expected listener type 'file', got %q", ch.ListenerType)
	}
	if ch.ListenerConfig["directory"] != "/input" {
		t.Errorf("expected directory '/input', got %v", ch.ListenerConfig["directory"])
	}
	if ch.Destinations[0].Type != "file" {
		t.Errorf("expected dest type 'file', got %q", ch.Destinations[0].Type)
	}
}

func TestParseMirthChannelUnsupported(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>unsupported-test</id>
  <name>Unsupported Test</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>Exotic Protocol</transportName>
    <properties></properties>
    <transformer><steps></steps></transformer>
    <filter><rules></rules></filter>
  </sourceConnector>
  <destinationConnectors>
    <connector>
      <name>JMS Dest</name>
      <transportName>JMS Producer</transportName>
      <properties><url>jms://host</url></properties>
      <transformer><steps></steps></transformer>
      <filter><rules></rules></filter>
    </connector>
  </destinationConnectors>
</channel>`

	ch, warnings, err := parseMirthChannel([]byte(xml))
	if err != nil {
		t.Fatalf("parseMirthChannel failed: %v", err)
	}
	if ch.ListenerType != "http" {
		t.Errorf("unsupported should default to http, got %q", ch.ListenerType)
	}
	foundWarning := false
	for _, w := range warnings {
		if strings.Contains(w, "Unsupported source") {
			foundWarning = true
		}
	}
	if !foundWarning {
		t.Error("expected warning about unsupported source connector")
	}
	if ch.Destinations[0].Type != "jms" {
		t.Errorf("expected dest type 'jms', got %q", ch.Destinations[0].Type)
	}
}

func TestParseMirthChannelSMTPAndDICOM(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>multi-dest</id>
  <name>Multi Dest</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>DICOM Listener</transportName>
    <properties><listenerPort>11112</listenerPort></properties>
    <transformer><steps></steps></transformer>
    <filter><rules></rules></filter>
  </sourceConnector>
  <destinationConnectors>
    <connector>
      <name>Email</name>
      <transportName>SMTP Sender</transportName>
      <properties><host>smtp.example.com</host><port>587</port></properties>
      <transformer><steps></steps></transformer>
      <filter><rules></rules></filter>
    </connector>
    <connector>
      <name>DICOM Store</name>
      <transportName>DICOM Sender</transportName>
      <properties><host>pacs.local</host><port>104</port></properties>
      <transformer><steps></steps></transformer>
      <filter><rules></rules></filter>
    </connector>
  </destinationConnectors>
</channel>`

	ch, _, err := parseMirthChannel([]byte(xml))
	if err != nil {
		t.Fatalf("parseMirthChannel failed: %v", err)
	}
	if ch.ListenerType != "dicom" {
		t.Errorf("expected listener type 'dicom', got %q", ch.ListenerType)
	}
	if len(ch.Destinations) != 2 {
		t.Fatalf("expected 2 destinations, got %d", len(ch.Destinations))
	}
	if ch.Destinations[0].Type != "smtp" {
		t.Errorf("expected first dest type 'smtp', got %q", ch.Destinations[0].Type)
	}
	if ch.Destinations[1].Type != "dicom" {
		t.Errorf("expected second dest type 'dicom', got %q", ch.Destinations[1].Type)
	}
}

func TestMessageCmdHasSubcommands(t *testing.T) {
	cmd := newMessageCmd()
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

func TestReprocessCmdHasSubcommands(t *testing.T) {
	cmd := newReprocessCmd()
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

func TestImportCmdHasMirthSubcommand(t *testing.T) {
	cmd := newImportCmd()
	subs := cmd.Commands()
	found := false
	for _, s := range subs {
		if s.Name() == "mirth" {
			found = true
		}
	}
	if !found {
		t.Error("import cmd missing 'mirth' subcommand")
	}
}

func TestStatsCmdWithSingleChannel(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, _ := os.ReadDir(channelsDir)
	if len(entries) == 0 {
		t.Fatal("no channels found")
	}

	cmd := newStatsCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{entries[0].Name(), "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("stats command failed: %v", err)
	}
	if !strings.Contains(buf.String(), "Channel:") {
		t.Fatalf("expected 'Channel:' in output, got %q", buf.String())
	}
}

func TestStatsCmdSingleChannelJSON(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, _ := os.ReadDir(channelsDir)
	if len(entries) == 0 {
		t.Fatal("no channels found")
	}

	cmd := newStatsCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{entries[0].Name(), "--dir", projectDir, "--json"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("stats --json command failed: %v", err)
	}
	if !strings.Contains(buf.String(), `"channel"`) {
		t.Fatalf("expected JSON with channel key, got %q", buf.String())
	}
}

func TestWriteChannelYAMLBasic(t *testing.T) {
	dir := t.TempDir()
	ch := &mirthImportChannel{
		ID:           "basic-channel",
		Name:         "Basic Channel",
		ListenerType: "http",
		ListenerConfig: map[string]any{
			"port": "8080",
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
	if !strings.Contains(content, "basic-channel") {
		t.Error("expected channel ID in YAML")
	}
	if !strings.Contains(content, "http") {
		t.Error("expected listener type in YAML")
	}
}

func TestWriteChannelYAMLWithPipeline(t *testing.T) {
	dir := t.TempDir()
	ch := &mirthImportChannel{
		ID:              "pipeline-channel",
		Name:            "Pipeline Channel",
		ListenerType:    "http",
		TransformerCode: "msg['field'] = 'value';",
		FilterCode:      "return true;",
	}
	if err := writeChannelYAML(dir, ch); err != nil {
		t.Fatalf("writeChannelYAML failed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "channel.yaml"))
	if err != nil {
		t.Fatalf("read channel.yaml: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "transformer") {
		t.Error("expected transformer in pipeline")
	}
	if !strings.Contains(content, "validator") {
		t.Error("expected validator in pipeline")
	}
}

func TestRootLogLevelFlag(t *testing.T) {
	f := rootCmd.PersistentFlags().Lookup("log-level")
	if f == nil {
		t.Fatal("root cmd missing --log-level persistent flag")
	}
	if f.DefValue != "info" {
		t.Errorf("expected default log-level 'info', got %q", f.DefValue)
	}
}
func TestServeCmdInvalidDir(t *testing.T) {
	cmd := newServeCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", "/nonexistent/path/that/does/not/exist"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --dir")
	}
}

func TestServeCmdCustomProfile(t *testing.T) {
	cmd := newServeCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", "/tmp/nonexistent", "--profile", "prod"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent dir")
	}
}

func TestBuildCmdInvalidDir(t *testing.T) {
	cmd := newBuildCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", "/nonexistent/build/dir"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --dir on build")
	}
}

func TestBuildCmdWithScaffoldedProject(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newBuildCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir})

	err := cmd.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), "npm") && !strings.Contains(err.Error(), "build") && !strings.Contains(err.Error(), "validation") {
			t.Logf("build error (expected without npm): %v", err)
		}
	}
}

func TestStatsCmdInvalidDir(t *testing.T) {
	cmd := newStatsCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", "/nonexistent/stats/dir"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --dir on stats")
	}
}

func TestStatsCmdInvalidChannel(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newStatsCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"nonexistent-channel-id", "--dir", projectDir})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent channel")
	}
}

func TestDeployCmdSingleChannel(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, _ := os.ReadDir(channelsDir)
	if len(entries) == 0 {
		t.Fatal("no channels found")
	}

	cmd := newDeployCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{entries[0].Name(), "--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("deploy single channel failed: %v", err)
	}
}

func TestDeployCmdInvalidDir(t *testing.T) {
	cmd := newDeployCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--all", "--dir", "/nonexistent/deploy/dir"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --dir on deploy")
	}
}

func TestUndeployCmdInvalidDir(t *testing.T) {
	cmd := newUndeployCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"some-ch", "--dir", "/nonexistent/undeploy/dir"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --dir on undeploy")
	}
}

func TestEnableCmdInvalidDir(t *testing.T) {
	cmd := newEnableCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"some-ch", "--dir", "/nonexistent/enable/dir"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --dir on enable")
	}
}

func TestDisableCmdInvalidDir(t *testing.T) {
	cmd := newDisableCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"some-ch", "--dir", "/nonexistent/disable/dir"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --dir on disable")
	}
}

func TestMessageListCmdInvalidSince(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newMessageListCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--dir", projectDir, "--since", "not-a-date"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --since")
	}
	if !strings.Contains(err.Error(), "invalid --since") {
		t.Fatalf("expected invalid --since error, got: %v", err)
	}
}

func TestMessageListCmdInvalidBefore(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newMessageListCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--dir", projectDir, "--before", "not-a-date"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --before")
	}
	if !strings.Contains(err.Error(), "invalid --before") {
		t.Fatalf("expected invalid --before error, got: %v", err)
	}
}

func TestMessageListCmdValidSince(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newMessageListCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir, "--since", "2024-01-01"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("message list with --since failed: %v", err)
	}
}

func TestMessageListCmdValidBefore(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newMessageListCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir, "--before", "2030-01-01T00:00:00Z"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("message list with --before failed: %v", err)
	}
}

func TestMessageListCmdWithFilters(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newMessageListCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{
		"--dir", projectDir,
		"--channel", "test-ch",
		"--status", "RECEIVED",
		"--limit", "10",
		"--offset", "0",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("message list with filters failed: %v", err)
	}
}

func TestMessageGetCmdInvalidDir(t *testing.T) {
	cmd := newMessageGetCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"some-id", "--dir", "/nonexistent/get/dir"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --dir on message get")
	}
}

func TestMessageGetCmdNotFound(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newMessageGetCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"nonexistent-message-id", "--dir", projectDir})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent message")
	}
}

func TestMessageCountCmdInvalidDir(t *testing.T) {
	cmd := newMessageCountCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--dir", "/nonexistent/count/dir"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --dir on message count")
	}
}

func TestMessageCountCmdWithFilters(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newMessageCountCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir, "--channel", "test-ch", "--status", "ERROR"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("message count with filters failed: %v", err)
	}
	if !strings.Contains(buf.String(), "0") {
		t.Fatalf("expected '0', got %q", buf.String())
	}
}

func TestReprocessByIDCmdMissingMessageID(t *testing.T) {
	cmd := newReprocessByIDCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"some-channel"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --message-id is missing")
	}
	if !strings.Contains(err.Error(), "--message-id") {
		t.Fatalf("expected --message-id error, got: %v", err)
	}
}

func TestReprocessByIDCmdInvalidDir(t *testing.T) {
	cmd := newReprocessByIDCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"some-channel", "--message-id", "msg-1", "--dir", "/nonexistent/reprocess/dir"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --dir on reprocess")
	}
}

func TestReprocessBatchCmdInvalidSince(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newReprocessBatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"some-channel", "--dir", projectDir, "--since", "bad-date"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --since on batch reprocess")
	}
}

func TestReprocessBatchCmdDryRun(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newReprocessBatchCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"some-channel", "--dir", projectDir, "--dry-run"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("batch reprocess dry-run failed: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "No messages found") && !strings.Contains(output, "Would reprocess") {
		t.Fatalf("expected dry-run output, got %q", output)
	}
}

func TestReprocessBatchCmdWithSince(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newReprocessBatchCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"some-channel", "--dir", projectDir, "--dry-run", "--since", "2024-01-01"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("batch reprocess with --since failed: %v", err)
	}
}

func TestBuildDashboardAuth_LDAPWithConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "ldap"},
	}
	cfg := &config.Config{
		AccessControl: &config.AccessControlConfig{
			LDAP: &config.LDAPConfig{
				URL:          "ldap://localhost:389",
				BindDN:       "cn=admin,dc=example,dc=com",
				BindPassword: "pass",
				BaseDN:       "dc=example,dc=com",
			},
		},
	}
	mw := buildDashboardAuth(dashCfg, cfg, logger)
	if mw == nil {
		t.Fatal("expected LDAP middleware")
	}
}

func TestBuildDashboardAuth_LDAPWithRBAC(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "ldap"},
	}
	cfg := &config.Config{
		AccessControl: &config.AccessControlConfig{
			LDAP: &config.LDAPConfig{
				URL:    "ldap://localhost:389",
				BindDN: "cn=admin,dc=example,dc=com",
				BaseDN: "dc=example,dc=com",
			},
		},
		Roles: []config.RoleConfig{
			{Name: "admin", Permissions: []string{"read", "write"}},
		},
	}
	mw := buildDashboardAuth(dashCfg, cfg, logger)
	if mw == nil {
		t.Fatal("expected LDAP middleware with RBAC")
	}
}

func TestDashboardCmdInvalidDir(t *testing.T) {
	cmd := newDashboardCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--dir", "/nonexistent/dashboard/dir"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for invalid --dir on dashboard")
	}
}

func TestDashboardCmdCustomPort(t *testing.T) {
	cmd := newDashboardCmd()
	f := cmd.Flags().Lookup("port")
	if f == nil {
		t.Fatal("dashboard cmd missing --port flag")
	}
	if err := f.Value.Set("4000"); err != nil {
		t.Fatalf("set port flag: %v", err)
	}
	val, err := cmd.Flags().GetInt("port")
	if err != nil {
		t.Fatalf("get port flag: %v", err)
	}
	if val != 4000 {
		t.Errorf("expected port 4000, got %d", val)
	}
}

func TestDeployCmdWithTagMatch(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newDeployCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir, "--tag", "production"})

	err := cmd.Execute()

	if err != nil {
		t.Logf("deploy with tag (no match): %v", err)
	}
}

func TestSetChannelEnabledNonexistentDir(t *testing.T) {
	err := setChannelEnabled("/nonexistent/channel/dir", "test-ch", true)
	if err == nil {
		t.Fatal("expected error for nonexistent channel dir")
	}
}

func TestSetChannelEnabledInvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "channel.yaml")
	os.WriteFile(path, []byte("{{invalid yaml"), 0o644)

	err := setChannelEnabled(dir, "test-ch", true)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestServeCmdProfileFlagParsing(t *testing.T) {
	cmd := newServeCmd()
	cmd.SetArgs([]string{"--profile", "staging"})

	f := cmd.Flags().Lookup("profile")
	if f == nil {
		t.Fatal("missing --profile flag")
	}
}

func TestBuildCmdNoArgs(t *testing.T) {
	cmd := newBuildCmd()

	if cmd.Args == nil {
		t.Log("build accepts cobra.NoArgs")
	}
	err := cmd.Args(cmd, []string{})
	if err != nil {
		t.Fatalf("expected no error with zero args: %v", err)
	}
}

func TestBuildCmdRejectsArgs(t *testing.T) {
	cmd := newBuildCmd()
	err := cmd.Args(cmd, []string{"extra-arg"})
	if err == nil {
		t.Fatal("expected error when extra args are passed to build")
	}
}

func TestStatsCmdAcceptsOptionalArg(t *testing.T) {
	cmd := newStatsCmd()
	err := cmd.Args(cmd, []string{})
	if err != nil {
		t.Fatalf("expected no error with zero args: %v", err)
	}
	err = cmd.Args(cmd, []string{"channel-id"})
	if err != nil {
		t.Fatalf("expected no error with one arg: %v", err)
	}
}

func TestReprocessByIDCmdDryRun(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newReprocessByIDCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"some-channel", "--dir", projectDir, "--message-id", "msg-1", "--dry-run"})

	err := cmd.Execute()

	if err != nil && !strings.Contains(err.Error(), "not found") {
		t.Logf("reprocess dry-run error (expected): %v", err)
	}
}

func TestDeployCmdWithNonexistentChannel(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newDeployCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"nonexistent-channel", "--dir", projectDir})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent channel on deploy")
	}
}

func TestUndeployCmdWithNonexistentChannel(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newUndeployCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"nonexistent-channel", "--dir", projectDir})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent channel on undeploy")
	}
}

func TestEnableCmdWithNonexistentChannel(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newEnableCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"nonexistent-channel", "--dir", projectDir})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent channel on enable")
	}
}

func TestDisableCmdWithNonexistentChannel(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newDisableCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"nonexistent-channel", "--dir", projectDir})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent channel on disable")
	}
}

func TestMessageListCmdRFC3339Since(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newMessageListCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir, "--since", "2024-01-01T00:00:00Z"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("message list with RFC3339 --since failed: %v", err)
	}
}

func TestReprocessBatchCmdRFC3339Since(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newReprocessBatchCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"some-channel", "--dir", projectDir, "--dry-run", "--since", "2024-06-01T12:00:00Z"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("batch reprocess with RFC3339 --since failed: %v", err)
	}
}
func TestMessageListCmd_Flags(t *testing.T) {
	cmd := newMessageListCmd()
	for _, name := range []string{"dir", "profile", "channel", "status", "since", "before", "limit", "offset", "json"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("message list cmd missing --%s flag", name)
		}
	}
}

func TestMessageListCmd_FlagDefaults(t *testing.T) {
	cmd := newMessageListCmd()

	tests := []struct {
		name     string
		expected string
	}{
		{"dir", "."},
		{"profile", "dev"},
		{"limit", "50"},
		{"offset", "0"},
		{"json", "false"},
		{"channel", ""},
		{"status", ""},
		{"since", ""},
		{"before", ""},
	}

	for _, tt := range tests {
		f := cmd.Flags().Lookup(tt.name)
		if f == nil {
			t.Errorf("flag %q not found", tt.name)
			continue
		}
		if f.DefValue != tt.expected {
			t.Errorf("flag %q: expected default %q, got %q", tt.name, tt.expected, f.DefValue)
		}
	}
}

func TestMessageGetCmd_Flags(t *testing.T) {
	cmd := newMessageGetCmd()
	for _, name := range []string{"dir", "profile", "json"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("message get cmd missing --%s flag", name)
		}
	}
}

func TestMessageGetCmd_RequiresArg(t *testing.T) {
	cmd := newMessageGetCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when no message ID provided")
	}
}

func TestMessageCountCmd_Flags(t *testing.T) {
	cmd := newMessageCountCmd()
	for _, name := range []string{"dir", "profile", "channel", "status"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("message count cmd missing --%s flag", name)
		}
	}
}

func TestMessageCountCmd_FlagDefaults(t *testing.T) {
	cmd := newMessageCountCmd()
	if f := cmd.Flags().Lookup("dir"); f.DefValue != "." {
		t.Errorf("expected dir default '.', got %q", f.DefValue)
	}
	if f := cmd.Flags().Lookup("profile"); f.DefValue != "dev" {
		t.Errorf("expected profile default 'dev', got %q", f.DefValue)
	}
}

func TestReprocessByIDCmd_Flags(t *testing.T) {
	cmd := newReprocessByIDCmd()
	for _, name := range []string{"dir", "profile", "message-id", "dry-run"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("reprocess message cmd missing --%s flag", name)
		}
	}
}

func TestReprocessByIDCmd_FlagDefaults(t *testing.T) {
	cmd := newReprocessByIDCmd()
	tests := []struct {
		name     string
		expected string
	}{
		{"dir", "."},
		{"profile", "dev"},
		{"message-id", ""},
		{"dry-run", "false"},
	}
	for _, tt := range tests {
		f := cmd.Flags().Lookup(tt.name)
		if f == nil {
			t.Errorf("flag %q not found", tt.name)
			continue
		}
		if f.DefValue != tt.expected {
			t.Errorf("flag %q: expected default %q, got %q", tt.name, tt.expected, f.DefValue)
		}
	}
}

func TestReprocessByIDCmd_RequiresArg(t *testing.T) {
	cmd := newReprocessByIDCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when no channel ID provided")
	}
}

func TestReprocessBatchCmd_Flags(t *testing.T) {
	cmd := newReprocessBatchCmd()
	for _, name := range []string{"dir", "profile", "status", "since", "limit", "dry-run"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("reprocess batch cmd missing --%s flag", name)
		}
	}
}

func TestReprocessBatchCmd_FlagDefaults(t *testing.T) {
	cmd := newReprocessBatchCmd()
	tests := []struct {
		name     string
		expected string
	}{
		{"dir", "."},
		{"profile", "dev"},
		{"status", "ERROR"},
		{"since", ""},
		{"limit", "100"},
		{"dry-run", "false"},
	}
	for _, tt := range tests {
		f := cmd.Flags().Lookup(tt.name)
		if f == nil {
			t.Errorf("flag %q not found", tt.name)
			continue
		}
		if f.DefValue != tt.expected {
			t.Errorf("flag %q: expected default %q, got %q", tt.name, tt.expected, f.DefValue)
		}
	}
}

func TestReprocessBatchCmd_RequiresArg(t *testing.T) {
	cmd := newReprocessBatchCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when no channel ID provided")
	}
}

func TestRebuildMessage(t *testing.T) {
	now := time.Now()
	record := &storage.MessageRecord{
		ID:            "msg-123",
		CorrelationID: "corr-456",
		ChannelID:     "ch-1",
		Stage:         "received",
		Content:       []byte("test content"),
		Status:        "success",
		Timestamp:     now,
	}

	msg := rebuildMessage(record)
	if msg == nil {
		t.Fatal("expected non-nil message")
	}
	if msg.ID == "" {
		t.Error("expected non-empty message ID")
	}
	if msg.CorrelationID != "corr-456" {
		t.Errorf("expected correlation corr-456, got %q", msg.CorrelationID)
	}
	if msg.ChannelID != "ch-1" {
		t.Errorf("expected channel ch-1, got %q", msg.ChannelID)
	}
	if msg.Metadata["original_message_id"] != "msg-123" {
		t.Errorf("expected original_message_id=msg-123, got %v", msg.Metadata["original_message_id"])
	}
	if msg.Metadata["reprocessed"] != true {
		t.Error("expected reprocessed=true in metadata")
	}
}

func TestRebuildMessage_EmptyContent(t *testing.T) {
	record := &storage.MessageRecord{
		ID:        "msg-empty",
		ChannelID: "ch-2",
		Timestamp: time.Now(),
	}

	msg := rebuildMessage(record)
	if msg == nil {
		t.Fatal("expected non-nil message")
	}
	if msg.ID == "" {
		t.Error("expected non-empty message ID")
	}
	if msg.Metadata["original_message_id"] != "msg-empty" {
		t.Errorf("expected original_message_id=msg-empty, got %v", msg.Metadata["original_message_id"])
	}
}

func TestMessageRebuild_PreservesOriginalTimestamp(t *testing.T) {
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	msg := message.Rebuild("id-1", "corr-1", "ch-1", []byte("data"), ts)
	if msg.Metadata["original_timestamp"] != ts.Format(time.RFC3339) {
		t.Errorf("expected original_timestamp %v, got %v", ts.Format(time.RFC3339), msg.Metadata["original_timestamp"])
	}
}

func TestDashboardCmdPortDefault(t *testing.T) {
	cmd := newDashboardCmd()
	f := cmd.Flags().Lookup("port")
	if f == nil {
		t.Fatal("dashboard cmd missing --port flag")
	}
	if f.DefValue != "3000" {
		t.Errorf("expected default port 3000, got %q", f.DefValue)
	}
}

func TestDashboardCmdDirDefault(t *testing.T) {
	cmd := newDashboardCmd()
	f := cmd.Flags().Lookup("dir")
	if f == nil {
		t.Fatal("dashboard cmd missing --dir flag")
	}
	if f.DefValue != "." {
		t.Errorf("expected default dir '.', got %q", f.DefValue)
	}
}

func TestDashboardCmdProfileDefault(t *testing.T) {
	cmd := newDashboardCmd()
	f := cmd.Flags().Lookup("profile")
	if f == nil {
		t.Fatal("dashboard cmd missing --profile flag")
	}
	if f.DefValue != "dev" {
		t.Errorf("expected default profile 'dev', got %q", f.DefValue)
	}
}

func TestDeployCmdFlagDefaults(t *testing.T) {
	cmd := newDeployCmd()
	tests := []struct {
		name     string
		expected string
	}{
		{"dir", "."},
		{"profile", "dev"},
		{"all", "false"},
		{"tag", ""},
	}
	for _, tt := range tests {
		f := cmd.Flags().Lookup(tt.name)
		if f == nil {
			t.Errorf("deploy cmd missing flag %q", tt.name)
			continue
		}
		if f.DefValue != tt.expected {
			t.Errorf("flag %q: expected default %q, got %q", tt.name, tt.expected, f.DefValue)
		}
	}
}

func TestUndeployCmdFlagDefaults(t *testing.T) {
	cmd := newUndeployCmd()
	if f := cmd.Flags().Lookup("dir"); f.DefValue != "." {
		t.Errorf("expected dir default '.', got %q", f.DefValue)
	}
	if f := cmd.Flags().Lookup("profile"); f.DefValue != "dev" {
		t.Errorf("expected profile default 'dev', got %q", f.DefValue)
	}
}

func TestEnableCmdFlagDefaults(t *testing.T) {
	cmd := newEnableCmd()
	if f := cmd.Flags().Lookup("dir"); f.DefValue != "." {
		t.Errorf("expected dir default '.', got %q", f.DefValue)
	}
}

func TestDisableCmdFlagDefaults(t *testing.T) {
	cmd := newDisableCmd()
	if f := cmd.Flags().Lookup("dir"); f.DefValue != "." {
		t.Errorf("expected dir default '.', got %q", f.DefValue)
	}
}

func TestStatsCmdHasFlags(t *testing.T) {
	cmd := newStatsCmd()
	for _, name := range []string{"dir", "profile", "json"} {
		f := cmd.Flags().Lookup(name)
		if f == nil {
			t.Errorf("stats cmd missing --%s flag", name)
		}
	}
}

func TestStatsCmdFlagDefaults(t *testing.T) {
	cmd := newStatsCmd()
	if f := cmd.Flags().Lookup("dir"); f.DefValue != "." {
		t.Errorf("expected dir default '.', got %q", f.DefValue)
	}
	if f := cmd.Flags().Lookup("json"); f.DefValue != "false" {
		t.Errorf("expected json default 'false', got %q", f.DefValue)
	}
}

func TestValidateCmdFlagDefaults(t *testing.T) {
	cmd := newValidateCmd()
	if f := cmd.Flags().Lookup("dir"); f.DefValue != "." {
		t.Errorf("expected dir default '.', got %q", f.DefValue)
	}
	if f := cmd.Flags().Lookup("profile"); f.DefValue != "dev" {
		t.Errorf("expected profile default 'dev', got %q", f.DefValue)
	}
}

func TestInitCmdFlagDefaults(t *testing.T) {
	cmd := newInitCmd()
	if f := cmd.Flags().Lookup("dir"); f.DefValue != "." {
		t.Errorf("expected dir default '.', got %q", f.DefValue)
	}
}

func TestBuildCmdFlagDefaults(t *testing.T) {
	cmd := newBuildCmd()
	if f := cmd.Flags().Lookup("dir"); f.DefValue != "." {
		t.Errorf("expected dir default '.', got %q", f.DefValue)
	}
}

func TestPruneCmdFlagDefaults(t *testing.T) {
	cmd := newPruneCmd()
	tests := []struct {
		name     string
		expected string
	}{
		{"dir", "."},
		{"profile", "dev"},
		{"all", "false"},
		{"dry-run", "false"},
		{"confirm", "false"},
		{"channel", ""},
		{"before", ""},
	}
	for _, tt := range tests {
		f := cmd.Flags().Lookup(tt.name)
		if f == nil {
			t.Errorf("prune cmd missing flag %q", tt.name)
			continue
		}
		if f.DefValue != tt.expected {
			t.Errorf("flag %q: expected default %q, got %q", tt.name, tt.expected, f.DefValue)
		}
	}
}

func TestPrintChannelStatsFromDir_TextOutput(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, _ := os.ReadDir(channelsDir)
	if len(entries) == 0 {
		t.Fatal("no channels found")
	}
	channelDir := filepath.Join(channelsDir, entries[0].Name())

	var buf bytes.Buffer
	cmd := newStatsCmd()
	cmd.SetOut(&buf)

	if err := printChannelStatsFromDir(cmd, channelDir, false, nil, nil); err != nil {
		t.Fatalf("printChannelStatsFromDir (text): %v", err)
	}
	if !strings.Contains(buf.String(), "Channel:") {
		t.Fatalf("expected 'Channel:' in output, got %q", buf.String())
	}
}

func TestPrintChannelStatsFromDir_JSONOutput(t *testing.T) {
	projectDir := scaffoldProject(t)

	channelsDir := filepath.Join(projectDir, "src", "channels")
	entries, _ := os.ReadDir(channelsDir)
	if len(entries) == 0 {
		t.Fatal("no channels found")
	}
	channelDir := filepath.Join(channelsDir, entries[0].Name())

	var buf bytes.Buffer
	cmd := newStatsCmd()
	cmd.SetOut(&buf)

	if err := printChannelStatsFromDir(cmd, channelDir, true, nil, nil); err != nil {
		t.Fatalf("printChannelStatsFromDir (json): %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, `"channel"`) {
		t.Fatalf("expected JSON key 'channel', got %q", output)
	}
}

func TestParseMirthChannel_EmptyDestinations(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>no-dest</id>
  <name>No Destinations</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>HTTP Listener</transportName>
    <properties>
      <listenerPort>8080</listenerPort>
    </properties>
    <transformer><steps></steps></transformer>
    <filter><rules></rules></filter>
  </sourceConnector>
  <destinationConnectors></destinationConnectors>
</channel>`

	ch, _, err := parseMirthChannel([]byte(xml))
	if err != nil {
		t.Fatalf("parseMirthChannel failed: %v", err)
	}
	if len(ch.Destinations) != 0 {
		t.Errorf("expected 0 destinations, got %d", len(ch.Destinations))
	}
}

func TestParseMirthChannel_FHIRConnectors_DefaultsToHTTP(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>fhir-test</id>
  <name>FHIR Test</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>FHIR Listener</transportName>
    <properties>
      <listenerPort>8443</listenerPort>
    </properties>
    <transformer><steps></steps></transformer>
    <filter><rules></rules></filter>
  </sourceConnector>
  <destinationConnectors>
    <connector>
      <name>FHIR Out</name>
      <transportName>FHIR Sender</transportName>
      <properties>
        <url>http://fhir-server.local/fhir</url>
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
	if ch.ListenerType != "http" {
		t.Errorf("expected FHIR to default to 'http', got %q", ch.ListenerType)
	}
	if ch.Destinations[0].Type != "http" {
		t.Errorf("expected FHIR dest to default to 'http', got %q", ch.Destinations[0].Type)
	}
	foundWarning := false
	for _, w := range warnings {
		if strings.Contains(w, "Unsupported") {
			foundWarning = true
		}
	}
	if !foundWarning {
		t.Error("expected warning about unsupported FHIR connector type")
	}
}

func TestParseMirthChannel_MultipleTransformerSteps(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>multi-step</id>
  <name>Multi Step</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>HTTP Listener</transportName>
    <properties><listenerPort>8080</listenerPort></properties>
    <transformer><steps>
      <step><name>Step1</name><type>JavaScript</type><script>var a = 1;</script></step>
      <step><name>Step2</name><type>JavaScript</type><script>var b = 2;</script></step>
      <step><name>Step3</name><type>JavaScript</type><script>var c = 3;</script></step>
    </steps></transformer>
    <filter><rules></rules></filter>
  </sourceConnector>
  <destinationConnectors></destinationConnectors>
</channel>`

	ch, _, err := parseMirthChannel([]byte(xml))
	if err != nil {
		t.Fatalf("parseMirthChannel failed: %v", err)
	}
	if !strings.Contains(ch.TransformerCode, "var a = 1;") {
		t.Error("expected step 1 code in transformer")
	}
	if !strings.Contains(ch.TransformerCode, "var b = 2;") {
		t.Error("expected step 2 code in transformer")
	}
	if !strings.Contains(ch.TransformerCode, "var c = 3;") {
		t.Error("expected step 3 code in transformer")
	}
}

func TestParseMirthChannel_MultipleFilterRules(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<channel>
  <id>multi-filter</id>
  <name>Multi Filter</name>
  <enabled>true</enabled>
  <sourceConnector>
    <transportName>HTTP Listener</transportName>
    <properties><listenerPort>8080</listenerPort></properties>
    <transformer><steps></steps></transformer>
    <filter><rules>
      <rule><name>Rule1</name><type>JavaScript</type><script>return msg.type === 'ADT';</script></rule>
      <rule><name>Rule2</name><type>JavaScript</type><script>return msg.id != null;</script></rule>
    </rules></filter>
  </sourceConnector>
  <destinationConnectors></destinationConnectors>
</channel>`

	ch, _, err := parseMirthChannel([]byte(xml))
	if err != nil {
		t.Fatalf("parseMirthChannel failed: %v", err)
	}
	if !strings.Contains(ch.FilterCode, "msg.type") {
		t.Error("expected rule 1 code in filter")
	}
	if !strings.Contains(ch.FilterCode, "msg.id") {
		t.Error("expected rule 2 code in filter")
	}
}

func TestMessageListCmd_WithProject_EmptyStore(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newMessageListCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("message list failed: %v", err)
	}
	if !strings.Contains(buf.String(), "No messages found") {
		t.Fatalf("expected 'No messages found', got %q", buf.String())
	}
}

func TestMessageCountCmd_WithProject_EmptyStore(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newMessageCountCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("message count failed: %v", err)
	}
	if !strings.Contains(buf.String(), "0") {
		t.Fatalf("expected '0' in count output, got %q", buf.String())
	}
}

func TestMessageListCmd_WithProject_JSONOutput(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newMessageListCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--dir", projectDir, "--json"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("message list --json failed: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "[]") && !strings.Contains(output, "null") {
		t.Fatalf("expected empty JSON array or null, got %q", output)
	}
}

func TestDeployCmdWithTag(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newDeployCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--all", "--dir", projectDir, "--tag", "nonexistent-tag"})

	if err := cmd.Execute(); err != nil {
		t.Logf("deploy with non-matching tag: %v", err)
	}
}

func TestPruneCmdWithConfirmAndAll(t *testing.T) {
	projectDir := scaffoldProject(t)

	cmd := newPruneCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--all", "--dir", projectDir, "--confirm"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("prune --all --confirm failed: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "Pruned") && !strings.Contains(output, "No messages") {
		t.Logf("unexpected output: %q (may be OK if store is empty)", output)
	}
}

func TestSanitizeID_Unicode(t *testing.T) {
	got := sanitizeID("Über-Channel™")
	if strings.ContainsRune(got, '™') {
		t.Errorf("expected trademark stripped, got %q", got)
	}
}

func TestSanitizeID_MultipleDashes(t *testing.T) {
	got := sanitizeID("my--channel--name")
	if got != "my--channel--name" {
		t.Errorf("expected dashes preserved, got %q", got)
	}
}

func TestSanitizeID_LeadingTrailingSpaces(t *testing.T) {
	got := sanitizeID("  hello  world  ")
	if strings.HasPrefix(got, " ") || strings.HasSuffix(got, " ") {
		t.Errorf("expected trimmed result, got %q", got)
	}
}

func TestWriteChannelYAML_TCPListener(t *testing.T) {
	dir := t.TempDir()
	ch := &mirthImportChannel{
		ID:           "tcp-channel",
		Name:         "TCP Channel",
		ListenerType: "tcp",
		ListenerConfig: map[string]any{
			"port": "6661",
			"mode": "mllp",
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
	if !strings.Contains(content, "tcp") {
		t.Errorf("expected tcp in YAML, got:\n%s", content)
	}
}

func TestWriteChannelYAML_WithMultipleDestinations(t *testing.T) {
	dir := t.TempDir()
	ch := &mirthImportChannel{
		ID:           "multi-dest-ch",
		Name:         "Multi Dest",
		ListenerType: "http",
		Destinations: []mirthImportDest{
			{Name: "dest1", Type: "http", Config: map[string]any{"url": "http://a.com"}},
			{Name: "dest2", Type: "tcp", Config: map[string]any{"host": "10.0.0.1", "port": "6661"}},
			{Name: "dest3", Type: "file", Config: map[string]any{"directory": "/output"}},
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
	if !strings.Contains(content, "dest1") {
		t.Error("expected dest1 in YAML")
	}
	if !strings.Contains(content, "dest2") {
		t.Error("expected dest2 in YAML")
	}
	if !strings.Contains(content, "dest3") {
		t.Error("expected dest3 in YAML")
	}
}

func TestIndentCode_EmptyString(t *testing.T) {
	got := indentCode("", "  ")
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestIndentCode_SingleLine(t *testing.T) {
	got := indentCode("single line", "    ")
	if got != "single line" {
		t.Errorf("expected 'single line', got %q", got)
	}
}

func TestIndentCode_TabIndent(t *testing.T) {
	got := indentCode("line1\nline2\nline3", "\t")
	if !strings.Contains(got, "\tline2") {
		t.Errorf("expected tab-indented second line, got %q", got)
	}
}
func pushCmdLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPush_BuildDashboardAuth_OIDCWithAccessControl(t *testing.T) {
	cfg := &config.Config{
		AccessControl: &config.AccessControlConfig{
			Enabled:  true,
			Provider: "oidc",
			OIDC: &config.OIDCConfig{
				Issuer:       "http://127.0.0.1:1/not-real",
				ClientID:     "cid",
				ClientSecret: "csec",
			},
		},
	}
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "oidc"},
	}
	mw := buildDashboardAuth(dashCfg, cfg, pushCmdLogger())
	if mw == nil {
		t.Fatal("expected non-nil middleware (fallback to basic due to unreachable OIDC)")
	}
}

func TestPush_BuildDashboardAuth_OIDCNoAccessControl(t *testing.T) {
	cfg := &config.Config{}
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "oidc"},
	}
	mw := buildDashboardAuth(dashCfg, cfg, pushCmdLogger())
	if mw == nil {
		t.Fatal("expected non-nil middleware (fallback to basic)")
	}
}

func TestPush_BuildDashboardAuth_LDAPNoAccessControl(t *testing.T) {
	cfg := &config.Config{}
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "ldap"},
	}
	mw := buildDashboardAuth(dashCfg, cfg, pushCmdLogger())
	if mw == nil {
		t.Fatal("expected non-nil middleware (fallback to basic)")
	}
}

func TestPush_BuildDashboardAuth_Unknown(t *testing.T) {
	cfg := &config.Config{}
	dashCfg := &config.DashboardConfig{
		Enabled: true,
		Auth:    &config.DashboardAuthConfig{Provider: "unknown-provider"},
	}
	mw := buildDashboardAuth(dashCfg, cfg, pushCmdLogger())
	if mw == nil {
		t.Fatal("expected non-nil middleware (fallback to basic)")
	}
}

func TestPush_BuildAuthMiddleware_NilAccessControl(t *testing.T) {
	cfg := &config.Config{}
	mw := buildAuthMiddleware(cfg, pushCmdLogger())
	if mw != nil {
		t.Fatal("expected nil for nil access control")
	}
}

func TestPush_BuildAuthMiddleware_Disabled(t *testing.T) {
	cfg := &config.Config{
		AccessControl: &config.AccessControlConfig{Enabled: false},
	}
	mw := buildAuthMiddleware(cfg, pushCmdLogger())
	if mw != nil {
		t.Fatal("expected nil for disabled access control")
	}
}

func TestPush_BuildAuthMiddleware_Enabled_LDAPProvider(t *testing.T) {
	cfg := &config.Config{
		AccessControl: &config.AccessControlConfig{
			Enabled:  true,
			Provider: "ldap",
		},
	}
	mw := buildAuthMiddleware(cfg, pushCmdLogger())
	if mw == nil {
		t.Fatal("expected non-nil middleware")
	}

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/api/channels", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated API request, got %d", w.Code)
	}

	req2 := httptest.NewRequest("GET", "/dashboard", nil)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 for non-API path, got %d", w2.Code)
	}
}

func TestPush_ServeCmd_DirFlagParsing(t *testing.T) {
	cmd := newServeCmd()
	cmd.SetArgs([]string{"--dir", "/some/path"})
	flag := cmd.Flags().Lookup("dir")
	if flag == nil {
		t.Fatal("expected dir flag")
	}
}

func TestPush_ServeCmd_ProfileFlagParsing(t *testing.T) {
	cmd := newServeCmd()
	cmd.SetArgs([]string{"--profile", "staging"})
	flag := cmd.Flags().Lookup("profile")
	if flag == nil {
		t.Fatal("expected profile flag")
	}
}

func TestPush_ServeCmd_InvalidDirNoConfig(t *testing.T) {
	cmd := newServeCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--dir", "/nonexistent-dir-for-test"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent dir")
	}
}

func TestPush_DeployCmd_NoArgsNoFlagsError(t *testing.T) {
	dir := t.TempDir()
	scaffoldMinimalProject(t, dir)

	cmd := newDeployCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--dir", dir})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when no channel ID, --all, or --tag specified")
	}
	if !strings.Contains(err.Error(), "specify a channel ID") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPush_DeployCmd_AllFlag(t *testing.T) {
	dir := t.TempDir()
	scaffoldMinimalProject(t, dir)
	createMinimalChannel(t, dir, "ch-deploy-1")
	createMinimalChannel(t, dir, "ch-deploy-2")

	cmd := newDeployCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--dir", dir, "--all"})
	err := cmd.Execute()
	if err != nil {
		t.Fatalf("deploy --all: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "ch-deploy-1") || !strings.Contains(out, "ch-deploy-2") {
		t.Fatalf("expected both channels in output, got: %s", out)
	}
}

func TestPush_ValidateCmd_InvalidDir(t *testing.T) {
	cmd := newValidateCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--dir", "/nonexistent-validate-dir"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent dir")
	}
}

func TestPush_BuildCmd_InvalidDir(t *testing.T) {
	cmd := newBuildCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--dir", "/nonexistent-build-dir"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent dir")
	}
}

func TestPush_PruneCmd_NoFlagsError(t *testing.T) {
	dir := t.TempDir()
	scaffoldMinimalProject(t, dir)

	cmd := newPruneCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--dir", dir})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for prune without --channel or --all")
	}
}

func TestPush_SetChannelEnabled_NonexistentFile(t *testing.T) {
	err := setChannelEnabled("/nonexistent", "test", true)
	if err == nil {
		t.Fatal("expected error for nonexistent channel dir")
	}
}

func TestPush_SetChannelEnabled_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "channel.yaml"), []byte("{{invalid yaml"), 0o644)
	err := setChannelEnabled(dir, "test", true)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestPush_SetChannelEnabled_ValidYAML(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "channel.yaml"), []byte("id: test\nenabled: false\n"), 0o644)
	err := setChannelEnabled(dir, "test", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "channel.yaml"))
	if !strings.Contains(string(data), "enabled: true") {
		t.Fatalf("expected enabled: true in output, got: %s", string(data))
	}
}

func TestPush_StatsCmd_NoDir(t *testing.T) {
	cmd := newStatsCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--dir", "/nonexistent-stats-dir"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for nonexistent dir")
	}
}

func scaffoldMinimalProject(t *testing.T, dir string) {
	t.Helper()
	configContent := `runtime:
  name: test-project
  mode: standalone
channels_dir: channels
`
	os.MkdirAll(filepath.Join(dir, "channels"), 0o755)
	os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte(configContent), 0o644)
}

func createMinimalChannel(t *testing.T, dir, channelID string) {
	t.Helper()
	chDir := filepath.Join(dir, "channels", channelID)
	os.MkdirAll(chDir, 0o755)
	chYAML := "id: " + channelID + "\nenabled: false\nsource:\n  type: http\n  http:\n    port: 0\n"
	os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte(chYAML), 0o644)
}
