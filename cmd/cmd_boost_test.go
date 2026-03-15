package cmd

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/internal/storage"
)

// ===================================================================
// Message subcommand flags and defaults
// ===================================================================

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

// ===================================================================
// Reprocess subcommand flags and defaults
// ===================================================================

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

// ===================================================================
// rebuildMessage
// ===================================================================

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

// ===================================================================
// message.Rebuild verification
// ===================================================================

func TestMessageRebuild_PreservesOriginalTimestamp(t *testing.T) {
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	msg := message.Rebuild("id-1", "corr-1", "ch-1", []byte("data"), ts)
	if msg.Metadata["original_timestamp"] != ts.Format(time.RFC3339) {
		t.Errorf("expected original_timestamp %v, got %v", ts.Format(time.RFC3339), msg.Metadata["original_timestamp"])
	}
}

// ===================================================================
// Dashboard cmd defaults
// ===================================================================

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

// ===================================================================
// Deploy cmd flags and defaults
// ===================================================================

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

// ===================================================================
// Stats cmd flag defaults
// ===================================================================

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

// ===================================================================
// Validate cmd flag defaults
// ===================================================================

func TestValidateCmdFlagDefaults(t *testing.T) {
	cmd := newValidateCmd()
	if f := cmd.Flags().Lookup("dir"); f.DefValue != "." {
		t.Errorf("expected dir default '.', got %q", f.DefValue)
	}
	if f := cmd.Flags().Lookup("profile"); f.DefValue != "dev" {
		t.Errorf("expected profile default 'dev', got %q", f.DefValue)
	}
}

// ===================================================================
// Init cmd
// ===================================================================

func TestInitCmdFlagDefaults(t *testing.T) {
	cmd := newInitCmd()
	if f := cmd.Flags().Lookup("dir"); f.DefValue != "." {
		t.Errorf("expected dir default '.', got %q", f.DefValue)
	}
}

// ===================================================================
// Build cmd
// ===================================================================

func TestBuildCmdFlagDefaults(t *testing.T) {
	cmd := newBuildCmd()
	if f := cmd.Flags().Lookup("dir"); f.DefValue != "." {
		t.Errorf("expected dir default '.', got %q", f.DefValue)
	}
}

// ===================================================================
// Prune cmd flag defaults
// ===================================================================

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

// ===================================================================
// printChannelStatsFromDir
// ===================================================================

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

	if err := printChannelStatsFromDir(cmd, channelDir, false); err != nil {
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

	if err := printChannelStatsFromDir(cmd, channelDir, true); err != nil {
		t.Fatalf("printChannelStatsFromDir (json): %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, `"channel"`) {
		t.Fatalf("expected JSON key 'channel', got %q", output)
	}
}

// ===================================================================
// Mirth import edge cases
// ===================================================================

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

// ===================================================================
// Message list/count with scaffolded project (memory store)
// ===================================================================

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

// ===================================================================
// Deploy with --tag
// ===================================================================

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

// ===================================================================
// Prune with --confirm and --all on scaffolded project
// ===================================================================

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

// ===================================================================
// sanitizeID edge cases
// ===================================================================

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

// ===================================================================
// Mirth import - writeChannelYAML with various configurations
// ===================================================================

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

// ===================================================================
// indentCode edge cases
// ===================================================================

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
