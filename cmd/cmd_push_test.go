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

	"github.com/intuware/intu-dev/pkg/config"
)

func pushCmdLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ===================================================================
// buildDashboardAuth — OIDC with valid AccessControl config
// ===================================================================

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

// ===================================================================
// buildAuthMiddleware
// ===================================================================

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

// ===================================================================
// Serve command — flag parsing
// ===================================================================

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

// ===================================================================
// Deploy command edge cases
// ===================================================================

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

// ===================================================================
// Validate command
// ===================================================================

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

// ===================================================================
// Build command
// ===================================================================

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

// ===================================================================
// Prune command
// ===================================================================

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

// ===================================================================
// setChannelEnabled edge cases
// ===================================================================

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

// ===================================================================
// Stats command edge cases
// ===================================================================

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

// ===================================================================
// Helpers
// ===================================================================

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
