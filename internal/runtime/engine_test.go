package runtime

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/cluster"
	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/internal/observability"
	"github.com/intuware/intu-dev/internal/retry"
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func minimalConfig() *config.Config {
	return &config.Config{
		Runtime: config.RuntimeConfig{
			Name: "test-engine",
		},
		ChannelsDir: "channels",
	}
}

func TestNewDefaultEngine(t *testing.T) {
	cfg := minimalConfig()
	engine := NewDefaultEngine("/tmp/project", cfg, nil, discardLogger())

	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.rootDir != "/tmp/project" {
		t.Errorf("rootDir = %q, want /tmp/project", engine.rootDir)
	}
	if engine.cfg != cfg {
		t.Error("cfg not set correctly")
	}
	if engine.channels == nil {
		t.Error("channels map should be initialized")
	}
	if len(engine.channels) != 0 {
		t.Errorf("expected 0 channels, got %d", len(engine.channels))
	}
	if engine.clusterMode {
		t.Error("clusterMode should be false for standalone")
	}
	if engine.metrics == nil {
		t.Error("metrics should be set from observability.Global()")
	}
	if engine.maps == nil {
		t.Error("maps should be initialized")
	}
	if engine.acqWg == nil {
		t.Error("acqWg should be initialized")
	}
}

func TestNewDefaultEngine_ClusterMode(t *testing.T) {
	cfg := minimalConfig()
	cfg.Runtime.Mode = "cluster"
	engine := NewDefaultEngine("/tmp/project", cfg, nil, discardLogger())

	if !engine.clusterMode {
		t.Error("clusterMode should be true when mode=cluster")
	}
}

func TestNewDefaultEngine_StandaloneExplicit(t *testing.T) {
	cfg := minimalConfig()
	cfg.Runtime.Mode = "standalone"
	engine := NewDefaultEngine("/tmp/project", cfg, nil, discardLogger())

	if engine.clusterMode {
		t.Error("clusterMode should be false when mode=standalone")
	}
}

func TestNewDefaultEngine_EmptyModeDefaultsToStandalone(t *testing.T) {
	cfg := minimalConfig()
	cfg.Runtime.Mode = ""
	engine := NewDefaultEngine("/tmp/project", cfg, nil, discardLogger())

	if engine.clusterMode {
		t.Error("clusterMode should be false when mode is empty (defaults to standalone)")
	}
}

func TestEngine_SetMessageStore(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	if engine.store != nil {
		t.Error("store should be nil initially")
	}

	memStore := storage.NewMemoryStore(0, 0)
	engine.SetMessageStore(memStore)

	if engine.store != memStore {
		t.Error("store not set correctly")
	}
}

func TestEngine_MessageStore(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	if engine.MessageStore() != nil {
		t.Error("MessageStore() should return nil initially")
	}

	memStore := storage.NewMemoryStore(0, 0)
	engine.SetMessageStore(memStore)

	if engine.MessageStore() != memStore {
		t.Error("MessageStore() should return the set store")
	}
}

func TestEngine_SetAlertManager(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	if engine.alertMgr != nil {
		t.Error("alertMgr should be nil initially")
	}

	engine.SetAlertManager(nil)
	if engine.alertMgr != nil {
		t.Error("alertMgr should remain nil after setting nil")
	}
}

func TestEngine_SetCoordinator(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	if engine.coordinator != nil {
		t.Error("coordinator should be nil initially")
	}

	engine.SetCoordinator(nil)
	if engine.coordinator != nil {
		t.Error("coordinator should remain nil after setting nil")
	}
}

func TestEngine_SetDeduplicator(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	if engine.dedup != nil {
		t.Error("dedup should be nil initially")
	}

	engine.SetDeduplicator(nil)
	if engine.dedup != nil {
		t.Error("dedup should remain nil after setting nil")
	}
}

func TestEngine_SetRedisClient(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	if engine.redisClient != nil {
		t.Error("redisClient should be nil initially")
	}
	if engine.redisKeyPrefix != "" {
		t.Error("redisKeyPrefix should be empty initially")
	}

	engine.SetRedisClient(nil, "prefix:")
	if engine.redisKeyPrefix != "prefix:" {
		t.Errorf("redisKeyPrefix = %q, want 'prefix:'", engine.redisKeyPrefix)
	}
}

func TestEngine_Metrics(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	m := engine.Metrics()
	if m == nil {
		t.Error("Metrics() should return non-nil")
	}
}

func TestEngine_RootDir(t *testing.T) {
	engine := NewDefaultEngine("/my/project", minimalConfig(), nil, discardLogger())

	if engine.RootDir() != "/my/project" {
		t.Errorf("RootDir() = %q, want /my/project", engine.RootDir())
	}
}

func TestEngine_Config(t *testing.T) {
	cfg := minimalConfig()
	engine := NewDefaultEngine("/tmp", cfg, nil, discardLogger())

	if engine.Config() != cfg {
		t.Error("Config() should return the config passed to constructor")
	}
}

func TestEngine_ListChannelIDs_Empty(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	ids := engine.ListChannelIDs()
	if len(ids) != 0 {
		t.Errorf("expected 0 channel IDs, got %d", len(ids))
	}
}

func TestEngine_GetChannelRuntime_Empty(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	cr, ok := engine.GetChannelRuntime("nonexistent")
	if ok {
		t.Error("expected ok=false for nonexistent channel")
	}
	if cr != nil {
		t.Error("expected nil ChannelRuntime for nonexistent channel")
	}
}

func TestEngine_GetChannelRuntime_WithChannel(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	engine.channels["test-ch"] = &ChannelRuntime{ID: "test-ch"}

	cr, ok := engine.GetChannelRuntime("test-ch")
	if !ok {
		t.Error("expected ok=true for existing channel")
	}
	if cr == nil {
		t.Error("expected non-nil ChannelRuntime")
	}
	if cr.ID != "test-ch" {
		t.Errorf("ID = %q, want test-ch", cr.ID)
	}
}

func TestEngine_ListChannelIDs_WithChannels(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	engine.channels["alpha"] = &ChannelRuntime{ID: "alpha"}
	engine.channels["beta"] = &ChannelRuntime{ID: "beta"}
	engine.channels["gamma"] = &ChannelRuntime{ID: "gamma"}

	ids := engine.ListChannelIDs()
	if len(ids) != 3 {
		t.Fatalf("expected 3 channel IDs, got %d", len(ids))
	}

	sort.Strings(ids)
	expected := []string{"alpha", "beta", "gamma"}
	for i, id := range ids {
		if id != expected[i] {
			t.Errorf("ids[%d] = %q, want %q", i, id, expected[i])
		}
	}
}

func TestEngine_DependenciesMet(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	t.Run("no dependencies always met", func(t *testing.T) {
		chCfg := &config.ChannelConfig{ID: "ch-1"}
		if !engine.dependenciesMet(chCfg, map[string]bool{}) {
			t.Error("expected dependencies met when no dependencies")
		}
	})

	t.Run("all dependencies met", func(t *testing.T) {
		chCfg := &config.ChannelConfig{
			ID:        "ch-2",
			DependsOn: []string{"dep-1", "dep-2"},
		}
		started := map[string]bool{"dep-1": true, "dep-2": true}
		if !engine.dependenciesMet(chCfg, started) {
			t.Error("expected dependencies met when all deps started")
		}
	})

	t.Run("missing one dependency", func(t *testing.T) {
		chCfg := &config.ChannelConfig{
			ID:        "ch-3",
			DependsOn: []string{"dep-1", "dep-2"},
		}
		started := map[string]bool{"dep-1": true}
		if engine.dependenciesMet(chCfg, started) {
			t.Error("expected dependencies NOT met when dep-2 missing")
		}
	})

	t.Run("no dependencies started", func(t *testing.T) {
		chCfg := &config.ChannelConfig{
			ID:        "ch-4",
			DependsOn: []string{"dep-1"},
		}
		if engine.dependenciesMet(chCfg, map[string]bool{}) {
			t.Error("expected dependencies NOT met when no deps started")
		}
	})

	t.Run("nil started map treated as empty", func(t *testing.T) {
		chCfg := &config.ChannelConfig{
			ID:        "ch-5",
			DependsOn: []string{"dep-1"},
		}
		if engine.dependenciesMet(chCfg, nil) {
			t.Error("expected dependencies NOT met with nil started map")
		}
	})
}

func TestEngine_FindChannelDir(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	channelDir := filepath.Join(channelsDir, "my-channel")
	os.MkdirAll(channelDir, 0o755)

	channelYAML := `id: my-channel
enabled: true
listener:
  type: http
  http:
    port: 8080
`
	os.WriteFile(filepath.Join(channelDir, "channel.yaml"), []byte(channelYAML), 0o644)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	dir := engine.findChannelDir("my-channel")
	if dir != channelDir {
		t.Errorf("findChannelDir() = %q, want %q", dir, channelDir)
	}
}

func TestEngine_FindChannelDir_NotFound(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	dir := engine.findChannelDir("nonexistent-channel")
	if dir != "" {
		t.Errorf("findChannelDir() = %q, want empty for nonexistent channel", dir)
	}
}

func TestEngine_FindChannelDir_NestedChannels(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	nestedDir := filepath.Join(channelsDir, "group", "nested-channel")
	os.MkdirAll(nestedDir, 0o755)

	channelYAML := `id: nested-channel
enabled: true
listener:
  type: http
  http:
    port: 0
`
	os.WriteFile(filepath.Join(nestedDir, "channel.yaml"), []byte(channelYAML), 0o644)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	dir := engine.findChannelDir("nested-channel")
	if dir != nestedDir {
		t.Errorf("findChannelDir() = %q, want %q", dir, nestedDir)
	}
}

func TestEngine_ResolveChannelStore_NilGlobalStore(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	chCfg := &config.ChannelConfig{ID: "ch-1"}
	store := engine.resolveChannelStore(chCfg)
	if store != nil {
		t.Error("expected nil store when global store is nil")
	}
}

func TestEngine_ResolveChannelStore_NoChannelStorage(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	memStore := storage.NewMemoryStore(0, 0)
	engine.SetMessageStore(memStore)

	chCfg := &config.ChannelConfig{ID: "ch-1"}
	store := engine.resolveChannelStore(chCfg)
	if store != memStore {
		t.Error("expected global store when channel has no storage config")
	}
}

func TestEngine_ResolveChannelStore_ChannelStorageEnabled(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	memStore := storage.NewMemoryStore(0, 0)
	engine.SetMessageStore(memStore)

	chCfg := &config.ChannelConfig{
		ID: "ch-1",
		MessageStorage: &config.ChannelStorageConfig{
			Enabled: true,
		},
	}
	store := engine.resolveChannelStore(chCfg)
	if store == nil {
		t.Error("expected non-nil store for enabled channel storage")
	}
}

func TestEngine_ResolveChannelStore_WithMode(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	memStore := storage.NewMemoryStore(0, 0)
	engine.SetMessageStore(memStore)

	chCfg := &config.ChannelConfig{
		ID: "ch-1",
		MessageStorage: &config.ChannelStorageConfig{
			Mode:   "status",
			Stages: []string{"received", "sent"},
		},
	}
	store := engine.resolveChannelStore(chCfg)
	if store == nil {
		t.Error("expected non-nil composite store")
	}
	if store == memStore {
		t.Error("expected composite store different from base store")
	}
}

func TestEngine_ResolveChannelStore_EmptyModeEmptyStages(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	memStore := storage.NewMemoryStore(0, 0)
	engine.SetMessageStore(memStore)

	chCfg := &config.ChannelConfig{
		ID: "ch-1",
		MessageStorage: &config.ChannelStorageConfig{
			Enabled: true,
			Mode:    "",
			Stages:  nil,
		},
	}
	store := engine.resolveChannelStore(chCfg)
	if store != memStore {
		t.Error("expected global store when mode and stages are empty")
	}
}

func TestEngine_UndeployChannel_NotRunning(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	err := engine.UndeployChannel(nil, "nonexistent")
	if err != nil {
		t.Errorf("UndeployChannel should return nil for non-running channel, got %v", err)
	}
}

func TestEngine_ResolveChannelStore_NoneMode(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	memStore := storage.NewMemoryStore(0, 0)
	engine.SetMessageStore(memStore)

	chCfg := &config.ChannelConfig{
		ID: "ch-1",
		MessageStorage: &config.ChannelStorageConfig{
			Mode: "none",
		},
	}
	store := engine.resolveChannelStore(chCfg)
	if store == nil {
		t.Error("expected non-nil store for none mode (returns composite store)")
	}
	if store == memStore {
		t.Error("expected composite store wrapping the base store")
	}
}

func TestEngine_ResolveChannelStore_WithStagesOnly(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	memStore := storage.NewMemoryStore(0, 0)
	engine.SetMessageStore(memStore)

	chCfg := &config.ChannelConfig{
		ID: "ch-1",
		MessageStorage: &config.ChannelStorageConfig{
			Stages: []string{"received"},
		},
	}
	store := engine.resolveChannelStore(chCfg)
	if store == nil {
		t.Error("expected non-nil composite store when stages are set")
	}
	if store == memStore {
		t.Error("expected different composite store from base")
	}
}

func TestEngine_CloseRuntime_NilRunner(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	if err := engine.CloseRuntime(); err != nil {
		t.Fatalf("CloseRuntime with nil runner should not error: %v", err)
	}
}

func TestEngine_DependenciesMet_SingleDep(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	chCfg := &config.ChannelConfig{
		ID:        "ch-1",
		DependsOn: []string{"dep-1"},
	}

	if engine.dependenciesMet(chCfg, map[string]bool{"dep-1": true}) != true {
		t.Error("single dep met")
	}
	if engine.dependenciesMet(chCfg, map[string]bool{"dep-2": true}) != false {
		t.Error("wrong dep should not satisfy")
	}
}

func TestEngine_ListChannelIDs_Order(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	engine.channels["zebra"] = &ChannelRuntime{ID: "zebra"}
	engine.channels["alpha"] = &ChannelRuntime{ID: "alpha"}
	engine.channels["middle"] = &ChannelRuntime{ID: "middle"}

	ids := engine.ListChannelIDs()
	sort.Strings(ids)
	if len(ids) != 3 {
		t.Fatalf("expected 3 IDs, got %d", len(ids))
	}
	if ids[0] != "alpha" || ids[1] != "middle" || ids[2] != "zebra" {
		t.Fatalf("unexpected sorted order: %v", ids)
	}
}

func TestEngine_GetChannelRuntime_MultipleChannels(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	engine.channels["ch-a"] = &ChannelRuntime{ID: "ch-a"}
	engine.channels["ch-b"] = &ChannelRuntime{ID: "ch-b"}

	cr, ok := engine.GetChannelRuntime("ch-a")
	if !ok || cr.ID != "ch-a" {
		t.Error("expected ch-a")
	}

	cr, ok = engine.GetChannelRuntime("ch-b")
	if !ok || cr.ID != "ch-b" {
		t.Error("expected ch-b")
	}

	_, ok = engine.GetChannelRuntime("ch-c")
	if ok {
		t.Error("ch-c should not exist")
	}
}

func TestEngine_SetMessageStore_Twice(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())

	store1 := storage.NewMemoryStore(100, 0)
	store2 := storage.NewMemoryStore(200, 0)

	engine.SetMessageStore(store1)
	if engine.MessageStore() != store1 {
		t.Error("expected store1")
	}

	engine.SetMessageStore(store2)
	if engine.MessageStore() != store2 {
		t.Error("expected store2 after second set")
	}
}

func TestEngine_ConfigReturnsCorrectValue(t *testing.T) {
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test-proj"},
		ChannelsDir: "my-channels",
	}
	engine := NewDefaultEngine("/my/root", cfg, nil, discardLogger())

	if engine.Config().Runtime.Name != "test-proj" {
		t.Errorf("expected name=test-proj, got %q", engine.Config().Runtime.Name)
	}
	if engine.Config().ChannelsDir != "my-channels" {
		t.Errorf("expected channelsDir=my-channels, got %q", engine.Config().ChannelsDir)
	}
	if engine.RootDir() != "/my/root" {
		t.Errorf("expected rootDir=/my/root, got %q", engine.RootDir())
	}
}

func TestEngine_FindChannelDir_MultipleChannels(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")

	for _, name := range []string{"alpha", "beta", "gamma"} {
		chDir := filepath.Join(channelsDir, name)
		os.MkdirAll(chDir, 0o755)
		yaml := "id: " + name + "\nenabled: true\nlistener:\n  type: http\n  http:\n    port: 0\n"
		os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte(yaml), 0o644)
	}

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	for _, name := range []string{"alpha", "beta", "gamma"} {
		dir := engine.findChannelDir(name)
		expected := filepath.Join(channelsDir, name)
		if dir != expected {
			t.Errorf("findChannelDir(%q) = %q, want %q", name, dir, expected)
		}
	}
}

func TestEngine_FindChannelDir_EmptyChannelsDir(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	dir := engine.findChannelDir("any")
	if dir != "" {
		t.Errorf("expected empty string, got %q", dir)
	}
}

func TestEngine_FindChannelDir_NoChannelsDirectory(t *testing.T) {
	projectDir := t.TempDir()
	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	dir := engine.findChannelDir("test")
	if dir != "" {
		t.Errorf("expected empty string when channels dir doesn't exist, got %q", dir)
	}
}

func TestEngine_ReprocessMessage_ChannelNotFound(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	msg := message.New("", []byte("test"))
	err := engine.ReprocessMessage(context.Background(), "nonexistent", msg)
	if err == nil {
		t.Error("expected error for nonexistent channel")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got %q", err.Error())
	}
}

func TestEngine_DeployChannel_NotFound(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	err := engine.DeployChannel(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent channel")
	}
}

func TestEngine_DeployChannel_AlreadyRunning(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), nil, discardLogger())
	engine.channels["already-running"] = &ChannelRuntime{ID: "already-running"}

	err := engine.DeployChannel(context.Background(), "already-running")
	if err != nil {
		t.Fatalf("expected no error when channel is already running, got %v", err)
	}
}

func TestEngine_DeployChannel_Disabled(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	channelDir := filepath.Join(channelsDir, "disabled-ch")
	os.MkdirAll(channelDir, 0o755)

	yaml := "id: disabled-ch\nenabled: false\nlistener:\n  type: http\n  http:\n    port: 0\n"
	os.WriteFile(filepath.Join(channelDir, "channel.yaml"), []byte(yaml), 0o644)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	err := engine.DeployChannel(context.Background(), "disabled-ch")
	if err == nil {
		t.Error("expected error for disabled channel")
	}
	if !strings.Contains(err.Error(), "disabled") {
		t.Errorf("expected 'disabled' in error, got %q", err.Error())
	}
}

func TestEngine_RestartChannel_NotRunning(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	err := engine.RestartChannel(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent channel restart")
	}
}

// mockCoordinator for cluster tests
type mockCoordinator struct {
	shouldAcquire bool
	acquireResult bool
	acquireErr    error
	released      []string
	owned         []string
}

func (m *mockCoordinator) Start(ctx context.Context) error { return nil }
func (m *mockCoordinator) Stop()                           {}
func (m *mockCoordinator) GetPeers() []cluster.PeerInfo {
	return nil
}
func (m *mockCoordinator) InstanceID() string                         { return "test-instance" }
func (m *mockCoordinator) IsLeader() bool                             { return true }
func (m *mockCoordinator) RenewChannelLease(ctx context.Context, channelID string) error { return nil }
func (m *mockCoordinator) OwnedChannels() []string                    { return m.owned }
func (m *mockCoordinator) ShouldAcquireChannel(channelID string, tags []string) bool {
	return m.shouldAcquire
}
func (m *mockCoordinator) AcquireChannel(ctx context.Context, channelID string) (bool, error) {
	return m.acquireResult, m.acquireErr
}
func (m *mockCoordinator) ReleaseChannel(ctx context.Context, channelID string) error {
	m.released = append(m.released, channelID)
	return nil
}

type mockSource struct{}

func (m *mockSource) Start(ctx context.Context, handler connector.MessageHandler) error { return nil }
func (m *mockSource) Stop(ctx context.Context) error                                   { return nil }
func (m *mockSource) Type() string                                                     { return "mock" }

type mockDest struct{ name string }

func (m *mockDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	return &message.Response{StatusCode: 200}, nil
}
func (m *mockDest) Stop(ctx context.Context) error { return nil }
func (m *mockDest) Type() string                   { return "mock" }

type mockFactory struct{}

func (m *mockFactory) CreateSource(lc config.ListenerConfig) (connector.SourceConnector, error) {
	return &mockSource{}, nil
}
func (m *mockFactory) CreateDestination(name string, dest config.Destination) (connector.DestinationConnector, error) {
	return &mockDest{name: name}, nil
}

func TestBuildChannelRuntime_MinimalConfig(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), &mockFactory{}, discardLogger())
	memStore := storage.NewMemoryStore(0, 0)
	engine.SetMessageStore(memStore)

	chCfg := &config.ChannelConfig{
		ID: "test-ch",
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
	}

	cr, err := engine.buildChannelRuntime("/tmp/channels/test-ch", chCfg)
	if err != nil {
		t.Fatalf("buildChannelRuntime failed: %v", err)
	}
	if cr == nil {
		t.Fatal("expected non-nil channel runtime")
	}
	if cr.ID != "test-ch" {
		t.Errorf("expected ID 'test-ch', got %q", cr.ID)
	}
	if cr.Source == nil {
		t.Error("expected non-nil source")
	}
	if cr.Pipeline == nil {
		t.Error("expected non-nil pipeline")
	}
	if cr.Store != memStore {
		t.Error("expected store to be set from global")
	}
}

func TestBuildChannelRuntime_WithDestinations(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), &mockFactory{}, discardLogger())

	chCfg := &config.ChannelConfig{
		ID: "test-ch",
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest-1", Type: "http", HTTP: &config.HTTPDestConfig{URL: "http://example.com"}},
			{Name: "dest-2", Type: "file", File: &config.FileDestConfig{Directory: "/tmp"}},
		},
	}

	cr, err := engine.buildChannelRuntime("/tmp/channels/test-ch", chCfg)
	if err != nil {
		t.Fatalf("buildChannelRuntime failed: %v", err)
	}
	if len(cr.Destinations) != 2 {
		t.Errorf("expected 2 destinations, got %d", len(cr.Destinations))
	}
	if _, ok := cr.Destinations["dest-1"]; !ok {
		t.Error("expected dest-1 in destinations")
	}
	if _, ok := cr.Destinations["dest-2"]; !ok {
		t.Error("expected dest-2 in destinations")
	}
}

func TestBuildChannelRuntime_WithRefDestination(t *testing.T) {
	cfg := minimalConfig()
	cfg.Destinations = map[string]config.Destination{
		"shared-http": {
			Type: "http",
			HTTP: &config.HTTPDestConfig{URL: "http://shared.example.com"},
		},
	}
	engine := NewDefaultEngine("/tmp", cfg, &mockFactory{}, discardLogger())

	chCfg := &config.ChannelConfig{
		ID: "test-ch",
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		Destinations: []config.ChannelDestination{
			{Name: "dest-1", Ref: "shared-http"},
		},
	}

	cr, err := engine.buildChannelRuntime("/tmp/channels/test-ch", chCfg)
	if err != nil {
		t.Fatalf("buildChannelRuntime failed: %v", err)
	}
	if len(cr.Destinations) != 1 {
		t.Errorf("expected 1 destination, got %d", len(cr.Destinations))
	}
}

func TestBuildChannelRuntime_WithChannelStorage(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), &mockFactory{}, discardLogger())
	memStore := storage.NewMemoryStore(0, 0)
	engine.SetMessageStore(memStore)

	chCfg := &config.ChannelConfig{
		ID: "test-ch",
		Listener: config.ListenerConfig{
			Type: "http",
			HTTP: &config.HTTPListener{Port: 0},
		},
		MessageStorage: &config.ChannelStorageConfig{
			Mode:   "status",
			Stages: []string{"received", "sent"},
		},
	}

	cr, err := engine.buildChannelRuntime("/tmp/channels/test-ch", chCfg)
	if err != nil {
		t.Fatalf("buildChannelRuntime failed: %v", err)
	}
	if cr.Store == nil {
		t.Error("expected non-nil store")
	}
	if cr.Store == memStore {
		t.Error("expected composite store, not global store")
	}
}

func TestInitRetryAndQueue_NoRetryConfig(t *testing.T) {
	cr := &ChannelRuntime{
		Logger: discardLogger(),
		Destinations: map[string]connector.DestinationConnector{
			"d1": &mockDest{},
		},
		DestConfigs: []config.ChannelDestination{
			{Name: "d1"},
		},
	}
	rootCfg := minimalConfig()
	cr.initRetryAndQueue(rootCfg, nil, false, "")

	if len(cr.retryers) != 0 {
		t.Errorf("expected 0 retryers, got %d", len(cr.retryers))
	}
	if len(cr.queues) != 0 {
		t.Errorf("expected 0 queues, got %d", len(cr.queues))
	}
}

func TestInitRetryAndQueue_WithRetry(t *testing.T) {
	cr := &ChannelRuntime{
		Logger: discardLogger(),
		Destinations: map[string]connector.DestinationConnector{
			"d1": &mockDest{},
		},
		DestConfigs: []config.ChannelDestination{
			{
				Name: "d1",
				Retry: &config.RetryConfig{
					MaxAttempts: 3,
					Backoff:     "fixed",
				},
			},
		},
	}
	rootCfg := minimalConfig()
	cr.initRetryAndQueue(rootCfg, nil, false, "")

	if len(cr.retryers) != 1 {
		t.Errorf("expected 1 retryer, got %d", len(cr.retryers))
	}
	if _, ok := cr.retryers["d1"]; !ok {
		t.Error("expected retryer for d1")
	}
}

func TestInitRetryAndQueue_WithQueue(t *testing.T) {
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Logger: discardLogger(),
		Destinations: map[string]connector.DestinationConnector{
			"d1": &mockDest{},
		},
		DestConfigs: []config.ChannelDestination{
			{
				Name: "d1",
				Queue: &config.QueueConfig{
					Enabled: true,
					MaxSize: 100,
				},
			},
		},
	}
	rootCfg := minimalConfig()
	cr.initRetryAndQueue(rootCfg, nil, false, "")

	if len(cr.queues) != 1 {
		t.Errorf("expected 1 queue, got %d", len(cr.queues))
	}
}

func TestInitRetryAndQueue_WithDLQ(t *testing.T) {
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Logger: discardLogger(),
		Destinations: map[string]connector.DestinationConnector{
			"dlq-dest": &mockDest{},
		},
		DestConfigs: []config.ChannelDestination{
			{Name: "dlq-dest"},
		},
	}
	rootCfg := minimalConfig()
	rootCfg.DeadLetter = &config.DeadLetterConfig{
		Enabled:     true,
		Destination: "dlq-dest",
	}
	cr.initRetryAndQueue(rootCfg, nil, false, "")

	if cr.dlq == nil {
		t.Error("expected DLQ to be initialized")
	}
}

func TestInitRetryAndQueue_WithRefRetry(t *testing.T) {
	cr := &ChannelRuntime{
		Logger: discardLogger(),
		Destinations: map[string]connector.DestinationConnector{
			"ref-d": &mockDest{},
		},
		DestConfigs: []config.ChannelDestination{
			{Name: "ref-d", Ref: "shared-dest"},
		},
	}
	rootCfg := minimalConfig()
	rootCfg.Destinations = map[string]config.Destination{
		"shared-dest": {
			Type: "http",
			HTTP: &config.HTTPDestConfig{URL: "http://example.com"},
			Retry: &config.RetryMapConfig{
				MaxAttempts: 5,
				Backoff:     "exponential",
			},
		},
	}
	cr.initRetryAndQueue(rootCfg, nil, false, "")

	if len(cr.retryers) != 1 {
		t.Errorf("expected 1 retryer from ref, got %d", len(cr.retryers))
	}
}

func TestEngine_WatchChannels_TempDir(t *testing.T) {
	tmpDir := t.TempDir()
	channelsDir := filepath.Join(tmpDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	chDir := filepath.Join(channelsDir, "test-ch")
	os.MkdirAll(chDir, 0o755)
	os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte("id: test-ch\nenabled: false\nlistener:\n  http:\n    port: 0\n"), 0o644)

	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine(tmpDir, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := engine.WatchChannels(ctx)
	if err != nil {
		t.Fatalf("WatchChannels: %v", err)
	}

	if engine.hotReloader == nil {
		t.Fatal("expected hotReloader to be set")
	}

	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestEngine_WatchChannels_EmptyDir(t *testing.T) {
	tmpDir := t.TempDir()
	channelsDir := filepath.Join(tmpDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine(tmpDir, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := engine.WatchChannels(ctx)
	if err != nil {
		t.Fatalf("WatchChannels on empty dir: %v", err)
	}
	cancel()
}

func TestEngine_WatchChannels_MissingDir(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "nonexistent-channels",
	}
	engine := NewDefaultEngine(tmpDir, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	err := engine.WatchChannels(context.Background())
	if err == nil {
		t.Fatal("expected error for missing channels dir")
	}
}

func TestEngine_TryAcquirePendingChannels_SkipsExisting(t *testing.T) {
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test", Mode: "cluster"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine("/tmp", cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	engine.clusterMode = true

	coord := &mockCoordinator{shouldAcquire: true, acquireResult: true}
	engine.coordinator = coord

	engine.channels["ch-1"] = &ChannelRuntime{ID: "ch-1"}
	engine.pendingChannels = []pendingChannel{
		{dir: "/tmp/channels/ch-1", cfg: &config.ChannelConfig{ID: "ch-1"}},
	}

	engine.tryAcquirePendingChannels(context.Background())

	if len(engine.pendingChannels) != 0 {
		t.Fatalf("expected 0 pending, got %d", len(engine.pendingChannels))
	}
}

func TestEngine_TryAcquirePendingChannels_ShouldNotAcquire(t *testing.T) {
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test", Mode: "cluster"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine("/tmp", cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	engine.clusterMode = true

	coord := &mockCoordinator{shouldAcquire: false}
	engine.coordinator = coord

	engine.pendingChannels = []pendingChannel{
		{dir: "/tmp/channels/ch-1", cfg: &config.ChannelConfig{ID: "ch-1"}},
	}

	engine.tryAcquirePendingChannels(context.Background())

	if len(engine.pendingChannels) != 1 {
		t.Fatalf("expected 1 pending (not acquired), got %d", len(engine.pendingChannels))
	}
}

func TestEngine_TryAcquirePendingChannels_AcquireFails(t *testing.T) {
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test", Mode: "cluster"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine("/tmp", cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	engine.clusterMode = true

	coord := &mockCoordinator{
		shouldAcquire: true,
		acquireResult: false,
		acquireErr:    nil,
	}
	engine.coordinator = coord

	engine.pendingChannels = []pendingChannel{
		{dir: "/tmp/channels/ch-1", cfg: &config.ChannelConfig{ID: "ch-1"}},
	}

	engine.tryAcquirePendingChannels(context.Background())

	if len(engine.pendingChannels) != 1 {
		t.Fatalf("expected 1 pending (acquire returned false), got %d", len(engine.pendingChannels))
	}
}

func TestEngine_ChannelAcquisitionLoop_Cancellation(t *testing.T) {
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test", Mode: "cluster"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine("/tmp", cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	engine.clusterMode = true

	coord := &mockCoordinator{shouldAcquire: false}
	engine.coordinator = coord

	ctx, cancel := context.WithCancel(context.Background())
	engine.acqWg.Add(1)

	done := make(chan struct{})
	go func() {
		engine.channelAcquisitionLoop(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("channelAcquisitionLoop did not exit after cancel")
	}
}

func TestEngine_PreloadChannelScripts_EmptyPipeline(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine(tmpDir, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	engine.jsRunner = &NodeRunner{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	chCfg := &config.ChannelConfig{
		ID:      "ch1",
		Enabled: true,
	}

	engine.preloadChannelScripts(filepath.Join(tmpDir, "channels", "ch1"), chCfg)
}

func TestEngine_PreloadChannelScripts_WithScripts(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine(tmpDir, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	engine.jsRunner = &NodeRunner{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	chDir := filepath.Join(tmpDir, "channels", "ch1")
	os.MkdirAll(chDir, 0o755)

	chCfg := &config.ChannelConfig{
		ID:      "ch1",
		Enabled: true,
		Pipeline: &config.PipelineConfig{
			Validator:   "validator.ts",
			Transformer: "transformer.ts",
		},
		Validator: &config.ScriptRef{
			Entrypoint: "val.ts",
		},
		Transformer: &config.ScriptRef{
			Entrypoint: "transform.ts",
		},
	}

	engine.preloadChannelScripts(chDir, chCfg)
}

type noopSource struct{}

func (n *noopSource) Start(ctx context.Context, h connector.MessageHandler) error {
	return nil
}
func (n *noopSource) Stop(ctx context.Context) error { return nil }
func (n *noopSource) Type() string                   { return "noop" }

func TestEngine_Stop_EmptyEngine(t *testing.T) {
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine("/tmp", cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	err := engine.Stop(context.Background())
	if err != nil {
		t.Fatalf("Stop on empty engine: %v", err)
	}
}

func TestEngine_Stop_WithChannels(t *testing.T) {
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine("/tmp", cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	engine.channels["ch-1"] = &ChannelRuntime{
		ID:     "ch-1",
		Source: &noopSource{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	err := engine.Stop(context.Background())
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestChannelRuntime_StoreIntuMessage_NilStore(t *testing.T) {
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Store:  nil,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := message.New("ch-1", []byte("test"))

	cr.storeIntuMessage(msg, "source", "RECEIVED")
}

func TestChannelRuntime_StoreIntuMessage_MemoryStore(t *testing.T) {
	memStore := storage.NewMemoryStore(100, 0)
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Store:  memStore,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := message.New("ch-1", []byte(`{"hello":"world"}`))

	cr.storeIntuMessage(msg, "source", "RECEIVED")

	records, err := memStore.Query(storage.QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Stage != "source" {
		t.Fatalf("expected stage 'source', got %q", records[0].Stage)
	}
	if records[0].Status != "RECEIVED" {
		t.Fatalf("expected status 'RECEIVED', got %q", records[0].Status)
	}
}

func TestChannelRuntime_StoreIntuMessage_WithDuration(t *testing.T) {
	memStore := storage.NewMemoryStore(100, 0)
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Store:  memStore,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := message.New("ch-1", []byte("data"))

	cr.storeIntuMessage(msg, "transform", "TRANSFORMED", 42)

	records, err := memStore.Query(storage.QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].DurationMs != 42 {
		t.Fatalf("expected DurationMs 42, got %d", records[0].DurationMs)
	}
}

func TestChannelRuntime_StoreIntuMessage_CompositeStore_FilteredStage(t *testing.T) {
	memStore := storage.NewMemoryStore(100, 0)
	cs := storage.NewCompositeStore(memStore, "full", []string{"source"})
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Store:  cs,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := message.New("ch-1", []byte("data"))

	cr.storeIntuMessage(msg, "transform", "TRANSFORMED")

	records, err := memStore.Query(storage.QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records (filtered by composite), got %d", len(records))
	}
}

func TestChannelRuntime_StoreResponseMessage_NilStore(t *testing.T) {
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Store:  nil,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := message.New("ch-1", []byte("test"))
	resp := &message.Response{StatusCode: 200, Body: []byte("ok")}

	cr.storeResponseMessage(msg, resp, "SENT")
}

func TestChannelRuntime_StoreResponseMessage_NilResponse(t *testing.T) {
	memStore := storage.NewMemoryStore(100, 0)
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Store:  memStore,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := message.New("ch-1", []byte("test"))

	cr.storeResponseMessage(msg, nil, "")

	records, _ := memStore.Query(storage.QueryOpts{ChannelID: "ch-1"})
	if len(records) != 0 {
		t.Fatalf("expected 0 records for nil response, got %d", len(records))
	}
}

func TestChannelRuntime_StoreResponseMessage_Success(t *testing.T) {
	memStore := storage.NewMemoryStore(100, 0)
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Store:  memStore,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := message.New("ch-1", []byte("test"))
	resp := &message.Response{
		StatusCode: 200,
		Body:       []byte(`{"result":"ok"}`),
		Headers:    map[string]string{"Content-Type": "application/json"},
	}

	cr.storeResponseMessage(msg, resp, "SENT")

	records, err := memStore.Query(storage.QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Stage != "response" {
		t.Fatalf("expected stage 'response', got %q", records[0].Stage)
	}
	if records[0].Status != "SENT" {
		t.Fatalf("expected status 'SENT', got %q", records[0].Status)
	}
}

func TestChannelRuntime_ResolveActiveDestinations_EmptyRoute(t *testing.T) {
	cr := &ChannelRuntime{
		DestConfigs: []config.ChannelDestination{
			{Name: "d1"},
			{Name: "d2"},
		},
	}
	active := cr.resolveActiveDestinations(nil)
	if len(active) != 2 {
		t.Fatalf("expected all 2 dests for empty route, got %d", len(active))
	}
}

func TestChannelRuntime_ResolveActiveDestinations_SpecificRoutes(t *testing.T) {
	cr := &ChannelRuntime{
		DestConfigs: []config.ChannelDestination{
			{Name: "d1"},
			{Name: "d2"},
			{Name: "d3"},
		},
	}
	active := cr.resolveActiveDestinations([]string{"d1", "d3"})
	if len(active) != 2 {
		t.Fatalf("expected 2, got %d", len(active))
	}
	if active[0].Name != "d1" || active[1].Name != "d3" {
		t.Fatalf("wrong destinations: %v, %v", active[0].Name, active[1].Name)
	}
}

func TestChannelRuntime_ResolveActiveDestinations_NoMatch(t *testing.T) {
	cr := &ChannelRuntime{
		DestConfigs: []config.ChannelDestination{
			{Name: "d1"},
		},
	}
	active := cr.resolveActiveDestinations([]string{"nonexistent"})
	if len(active) != 0 {
		t.Fatalf("expected 0 for non-matching route, got %d", len(active))
	}
}

func TestChannelRuntime_ResolveActiveDestinations_RefFallback(t *testing.T) {
	cr := &ChannelRuntime{
		DestConfigs: []config.ChannelDestination{
			{Ref: "shared-dest"},
			{Name: "d2"},
		},
	}
	active := cr.resolveActiveDestinations([]string{"shared-dest"})
	if len(active) != 1 {
		t.Fatalf("expected 1, got %d", len(active))
	}
	if active[0].Ref != "shared-dest" {
		t.Fatalf("expected ref 'shared-dest', got %q", active[0].Ref)
	}
}

func TestChannelRuntime_HandleMessage_WithMetrics(t *testing.T) {
	metrics := observability.NewMetrics()
	store := storage.NewMemoryStore(0, 0)
	stubSrc := connector.NewStubSource("stub", discardLogger())
	logDest := connector.NewLogDest("log", discardLogger())

	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "ch1")
	os.MkdirAll(filepath.Join(channelDir, "dist"), 0o755)

	chCfg := &config.ChannelConfig{
		ID:      "ch1",
		Enabled: true,
	}

	pipeline := NewPipeline(channelDir, dir, "ch1", chCfg, nil, discardLogger())

	cr := &ChannelRuntime{
		ID:           "ch1",
		Config:       chCfg,
		Source:       stubSrc,
		Destinations: map[string]connector.DestinationConnector{"log": logDest},
		DestConfigs:  []config.ChannelDestination{{Name: "log"}},
		Pipeline:     pipeline,
		Logger:       discardLogger(),
		Metrics:      metrics,
		Store:        store,
	}

	msg := message.New("ch1", []byte(`{"test":"data"}`))
	err := cr.HandleMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	snap := metrics.Snapshot()
	counters, ok := snap["counters"].(map[string]int64)
	if !ok {
		t.Fatal("expected counters map in snapshot")
	}
	if counters["messages_received_total.ch1"] != 1 {
		t.Fatalf("expected 1 received, got %v", counters)
	}
	if counters["messages_processed_total.ch1"] != 1 {
		t.Fatalf("expected 1 processed, got %v", counters)
	}
}

func TestChannelRuntime_SendToDestination_Direct(t *testing.T) {
	logDest := connector.NewLogDest("log", discardLogger())
	cr := &ChannelRuntime{
		ID:           "ch1",
		Logger:       discardLogger(),
		Destinations: map[string]connector.DestinationConnector{"log": logDest},
		retryers:     make(map[string]*retry.Retryer),
		queues:       make(map[string]*retry.DestinationQueue),
		redisQueues:  make(map[string]*retry.RedisDestinationQueue),
	}

	msg := message.New("ch1", []byte("data"))
	resp, err := cr.sendToDestination(context.Background(), "log", logDest, msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestChannelRuntime_StoreIntuMessage_CompositeStore_None(t *testing.T) {
	inner := storage.NewMemoryStore(0, 0)
	cs := storage.NewCompositeStore(inner, "none", nil)

	cr := &ChannelRuntime{
		ID:     "ch1",
		Logger: discardLogger(),
		Store:  cs,
	}

	msg := message.New("ch1", []byte("test"))
	cr.storeIntuMessage(msg, "received", "RECEIVED")
	records, _ := inner.Query(storage.QueryOpts{ChannelID: "ch1"})
	if len(records) != 0 {
		t.Fatalf("expected 0 records in 'none' mode, got %d", len(records))
	}
}

func TestChannelRuntime_StoreIntuMessage_CompositeStore_Full(t *testing.T) {
	inner := storage.NewMemoryStore(0, 0)
	cs := storage.NewCompositeStore(inner, "full", []string{"received", "error"})

	cr := &ChannelRuntime{
		ID:     "ch1",
		Logger: discardLogger(),
		Store:  cs,
	}

	msg := message.New("ch1", []byte("test"))

	cr.storeIntuMessage(msg, "received", "RECEIVED")
	records, _ := inner.Query(storage.QueryOpts{ChannelID: "ch1"})
	if len(records) != 1 {
		t.Fatalf("expected 1 record for 'received' stage, got %d", len(records))
	}

	cr.storeIntuMessage(msg, "transformed", "TRANSFORMED")
	records, _ = inner.Query(storage.QueryOpts{ChannelID: "ch1"})
	if len(records) != 1 {
		t.Fatalf("expected still 1 record (transformed filtered in full mode), got %d", len(records))
	}
}

func TestChannelRuntime_Stop(t *testing.T) {
	stubSrc := connector.NewStubSource("stub", discardLogger())
	logDest := connector.NewLogDest("log", discardLogger())

	cr := &ChannelRuntime{
		ID:           "ch1",
		Source:       stubSrc,
		Destinations: map[string]connector.DestinationConnector{"log": logDest},
		Logger:       discardLogger(),
		queues:       make(map[string]*retry.DestinationQueue),
		redisQueues:  make(map[string]*retry.RedisDestinationQueue),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cr.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
