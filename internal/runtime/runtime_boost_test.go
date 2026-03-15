package runtime

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// DefaultEngine — additional utility functions
// ===================================================================

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

// ===================================================================
// HotReloader — additional edge cases
// ===================================================================

func TestHotReloader_ShouldDebounce_Precision(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	hr.debounceMu.Lock()
	hr.debounce["fast-ch"] = time.Now().Add(-100 * time.Millisecond)
	hr.debounceMu.Unlock()

	if !hr.shouldDebounce("fast-ch") {
		t.Error("100ms ago should still be debounced (within 2s window)")
	}

	hr.debounceMu.Lock()
	hr.debounce["old-ch"] = time.Now().Add(-3 * time.Second)
	hr.debounceMu.Unlock()

	if hr.shouldDebounce("old-ch") {
		t.Error("3s ago should NOT be debounced (outside 2s window)")
	}
}

func TestHotReloader_ChannelIDFromDir_EmptyDir(t *testing.T) {
	emptyDir := t.TempDir()
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	id := hr.channelIDFromDir(emptyDir)
	if id != "" {
		t.Errorf("expected empty ID for dir without channel.yaml, got %q", id)
	}
}

func TestHotReloader_ChannelIDFromDir_NonexistentDir(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	id := hr.channelIDFromDir("/nonexistent/path/to/channel")
	if id != "" {
		t.Errorf("expected empty ID for nonexistent dir, got %q", id)
	}
}

func TestHotReloader_WatchChannelDir_EmptyDir(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	channelDir := filepath.Join(channelsDir, "empty-channel")
	os.MkdirAll(channelDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	hr.watchChannelDir(channelDir)
}

func TestHotReloader_WatchChannelDir_WithTSFiles(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	channelDir := filepath.Join(channelsDir, "ts-channel")
	os.MkdirAll(channelDir, 0o755)

	os.WriteFile(filepath.Join(channelDir, "channel.yaml"), []byte("id: ts\nenabled: true\nlistener:\n  type: http\n  http:\n    port: 0\n"), 0o644)
	os.WriteFile(filepath.Join(channelDir, "transformer.ts"), []byte("export function transform(msg) { return msg; }"), 0o644)
	os.WriteFile(filepath.Join(channelDir, "validator.ts"), []byte("export function validate(msg) { return msg; }"), 0o644)
	os.WriteFile(filepath.Join(channelDir, "notes.txt"), []byte("not watched"), 0o644)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	hr.watchChannelDir(channelDir)
}

func TestHotReloader_WatchChannelDir_NonexistentDir(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	engine := NewDefaultEngine(projectDir, minimalConfig(), nil, discardLogger())
	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	hr.watchChannelDir("/nonexistent/dir")
}

func TestHotReloader_StartStopMultipleTimes(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}

	ctx := context.Background()
	if err := hr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	hr.Stop()
}

func TestHotReloader_StartWithNestedChannels(t *testing.T) {
	projectDir := t.TempDir()
	channelsDir := filepath.Join(projectDir, "channels")

	nestedDir := filepath.Join(channelsDir, "group", "subgroup", "deep-channel")
	os.MkdirAll(nestedDir, 0o755)
	os.WriteFile(filepath.Join(nestedDir, "channel.yaml"), []byte("id: deep-channel\nenabled: true\nlistener:\n  type: http\n  http:\n    port: 0\n"), 0o644)

	cfg := minimalConfig()
	cfg.ChannelsDir = "channels"
	engine := NewDefaultEngine(projectDir, cfg, nil, discardLogger())

	hr, err := NewHotReloader(engine, channelsDir, discardLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}

	ctx := context.Background()
	if err := hr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	hr.Stop()
}

// ===================================================================
// Engine — FindChannelDir edge cases
// ===================================================================

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

// ===================================================================
// Engine — ReprocessMessage without running channel
// ===================================================================

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

// ===================================================================
// Engine — DeployChannel errors
// ===================================================================

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

// ===================================================================
// Engine — RestartChannel
// ===================================================================

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
