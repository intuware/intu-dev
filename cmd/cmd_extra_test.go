package cmd

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/intuware/intu-dev/pkg/config"
)

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
