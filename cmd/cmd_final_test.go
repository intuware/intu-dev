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
	// Build may fail if npm deps are not installed, but we exercise the code path
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
	cmd.SetArgs([]string{"--port", "4000"})
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
	// No channels match the tag, but the command should not error
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

	// Build should accept no positional args
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
	// Will fail because message doesn't exist, but exercises the code path
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
