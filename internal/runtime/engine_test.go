package runtime

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"

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
