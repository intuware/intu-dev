package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewLoader(t *testing.T) {
	l := NewLoader("/some/path")
	if l == nil {
		t.Fatal("expected non-nil Loader")
	}
	if l.root != "/some/path" {
		t.Fatalf("expected root=/some/path, got %s", l.root)
	}
}

func TestLoaderLoad_BasicYAML(t *testing.T) {
	dir := t.TempDir()
	yaml := `runtime:
  name: test-engine
  log_level: debug
channels_dir: channels
`
	os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte(yaml), 0o644)

	l := NewLoader(dir)
	cfg, err := l.Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Runtime.Name != "test-engine" {
		t.Fatalf("expected name=test-engine, got %s", cfg.Runtime.Name)
	}
	if cfg.Runtime.LogLevel != "debug" {
		t.Fatalf("expected log_level=debug, got %s", cfg.Runtime.LogLevel)
	}
	if cfg.ChannelsDir != "channels" {
		t.Fatalf("expected channels_dir=channels, got %s", cfg.ChannelsDir)
	}
}

func TestLoaderLoad_ProfileMerge(t *testing.T) {
	dir := t.TempDir()
	base := `runtime:
  name: base-engine
  log_level: info
channels_dir: channels
`
	profile := `runtime:
  log_level: debug
  mode: cluster
`
	os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte(base), 0o644)
	os.WriteFile(filepath.Join(dir, "intu.dev.yaml"), []byte(profile), 0o644)

	l := NewLoader(dir)
	cfg, err := l.Load("dev")
	if err != nil {
		t.Fatalf("Load with profile failed: %v", err)
	}
	if cfg.Runtime.Name != "base-engine" {
		t.Fatalf("expected name from base, got %s", cfg.Runtime.Name)
	}
	if cfg.Runtime.LogLevel != "debug" {
		t.Fatalf("expected log_level=debug from profile override, got %s", cfg.Runtime.LogLevel)
	}
	if cfg.Runtime.Mode != "cluster" {
		t.Fatalf("expected mode=cluster from profile, got %s", cfg.Runtime.Mode)
	}
	if cfg.ChannelsDir != "channels" {
		t.Fatalf("expected channels_dir from base, got %s", cfg.ChannelsDir)
	}
}

func TestLoaderLoad_MissingBaseFile(t *testing.T) {
	dir := t.TempDir()
	l := NewLoader(dir)
	_, err := l.Load("")
	if err == nil {
		t.Fatal("expected error for missing intu.yaml")
	}
}

func TestLoaderLoad_MissingProfileFileIgnored(t *testing.T) {
	dir := t.TempDir()
	yaml := `runtime:
  name: test
`
	os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte(yaml), 0o644)

	l := NewLoader(dir)
	cfg, err := l.Load("nonexistent-profile")
	if err != nil {
		t.Fatalf("expected no error when profile file missing, got: %v", err)
	}
	if cfg.Runtime.Name != "test" {
		t.Fatalf("expected name=test, got %s", cfg.Runtime.Name)
	}
}

func TestLoaderLoad_EnvVarExpansion(t *testing.T) {
	dir := t.TempDir()
	os.Setenv("TEST_LOADER_NAME", "env-engine")
	defer os.Unsetenv("TEST_LOADER_NAME")

	yaml := `runtime:
  name: $TEST_LOADER_NAME
  log_level: info
`
	os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte(yaml), 0o644)

	l := NewLoader(dir)
	cfg, err := l.Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Runtime.Name != "env-engine" {
		t.Fatalf("expected name=env-engine from env var, got %s", cfg.Runtime.Name)
	}
}

func TestLoaderLoad_EnvVarExpansionBraces(t *testing.T) {
	dir := t.TempDir()
	os.Setenv("TEST_LOADER_LEVEL", "warn")
	defer os.Unsetenv("TEST_LOADER_LEVEL")

	yaml := `runtime:
  name: test
  log_level: ${TEST_LOADER_LEVEL}
`
	os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte(yaml), 0o644)

	l := NewLoader(dir)
	cfg, err := l.Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Runtime.LogLevel != "warn" {
		t.Fatalf("expected log_level=warn from env var, got %s", cfg.Runtime.LogLevel)
	}
}

func TestLoaderLoad_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte("runtime: [invalid yaml\n"), 0o644)

	l := NewLoader(dir)
	_, err := l.Load("")
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoaderLoad_InvalidProfileYAML(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte("runtime:\n  name: ok\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "intu.bad.yaml"), []byte("runtime: [broken\n"), 0o644)

	l := NewLoader(dir)
	_, err := l.Load("bad")
	if err == nil {
		t.Fatal("expected error for invalid profile YAML")
	}
}

func TestLoaderLoad_FullConfig(t *testing.T) {
	dir := t.TempDir()
	yaml := `runtime:
  name: full-test
  log_level: info
  mode: standalone
  worker_pool: 4
channels_dir: my-channels
message_storage:
  driver: memory
  mode: full
dashboard:
  enabled: true
  port: 3000
`
	os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte(yaml), 0o644)

	l := NewLoader(dir)
	cfg, err := l.Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Runtime.WorkerPool != 4 {
		t.Fatalf("expected worker_pool=4, got %d", cfg.Runtime.WorkerPool)
	}
	if cfg.MessageStorage == nil || cfg.MessageStorage.Driver != "memory" {
		t.Fatal("expected message_storage with driver=memory")
	}
	if cfg.Dashboard == nil || !cfg.Dashboard.Enabled || cfg.Dashboard.Port != 3000 {
		t.Fatal("expected dashboard config")
	}
}

func TestLoaderLoad_EmptyYAML(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte(""), 0o644)

	l := NewLoader(dir)
	cfg, err := l.Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config for empty YAML")
	}
}

func TestLoaderLoad_ProfileEnvVarExpansion(t *testing.T) {
	dir := t.TempDir()
	os.Setenv("TEST_PROFILE_MODE", "cluster")
	defer os.Unsetenv("TEST_PROFILE_MODE")

	base := `runtime:
  name: test
  mode: standalone
`
	profile := `runtime:
  mode: $TEST_PROFILE_MODE
`
	os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte(base), 0o644)
	os.WriteFile(filepath.Join(dir, "intu.prod.yaml"), []byte(profile), 0o644)

	l := NewLoader(dir)
	cfg, err := l.Load("prod")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Runtime.Mode != "cluster" {
		t.Fatalf("expected mode=cluster from profile env, got %s", cfg.Runtime.Mode)
	}
}

// TestLoaderLoad_DotEnvFillsProfileYAML ensures .env is loaded before profile YAML so ${VAR} in intu.yaml / intu.<profile>.yaml resolve.
func TestLoaderLoad_DotEnvFillsProfileYAML(t *testing.T) {
	dir := t.TempDir()
	os.Unsetenv("INTU_DOTENV_TEST_NAME") // so .env can fill it
	envContent := "INTU_DOTENV_TEST_NAME=from-dotenv\n"
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(envContent), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	yaml := "runtime:\n  name: ${INTU_DOTENV_TEST_NAME}\n  log_level: info\nchannels_dir: channels\n"
	if err := os.WriteFile(filepath.Join(dir, "intu.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatalf("write intu.yaml: %v", err)
	}

	l := NewLoader(dir)
	cfg, err := l.Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Runtime.Name != "from-dotenv" {
		t.Fatalf("expected runtime.name=from-dotenv from .env, got %q", cfg.Runtime.Name)
	}
}

func TestLoadEnvFile_Optional(t *testing.T) {
	dir := t.TempDir()
	if err := loadEnvFile(filepath.Join(dir, ".env")); err != nil {
		t.Fatalf("loadEnvFile on missing .env should not error: %v", err)
	}
}
