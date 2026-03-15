package runtime

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/intuware/intu-dev/internal/cluster"
	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// WatchChannels — with temp dir containing channel configs
// ===================================================================

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

// ===================================================================
// tryAcquirePendingChannels — with mock coordinator
// ===================================================================

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

// ===================================================================
// channelAcquisitionLoop — lifecycle
// ===================================================================

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

// ===================================================================
// preloadChannelScripts — empty project and with scripts
// ===================================================================

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

	// Should not panic with nil pipeline
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

	// Should not panic — preload logs debug messages for missing files
	engine.preloadChannelScripts(chDir, chCfg)
}

// ===================================================================
// Engine Start/Stop lifecycle
// ===================================================================

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

type noopSource struct{}

func (n *noopSource) Start(ctx context.Context, h connector.MessageHandler) error {
	return nil
}
func (n *noopSource) Stop(ctx context.Context) error { return nil }
func (n *noopSource) Type() string                   { return "noop" }

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

// ===================================================================
// storeIntuMessage helper
// ===================================================================

func TestChannelRuntime_StoreIntuMessage_NilStore(t *testing.T) {
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Store:  nil,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := message.New("ch-1", []byte("test"))

	// Should not panic
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

	// This stage is not in the allowed stages ("source"), so it should be filtered
	cr.storeIntuMessage(msg, "transform", "TRANSFORMED")

	records, err := memStore.Query(storage.QueryOpts{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records (filtered by composite), got %d", len(records))
	}
}

// ===================================================================
// storeResponseMessage helper
// ===================================================================

func TestChannelRuntime_StoreResponseMessage_NilStore(t *testing.T) {
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Store:  nil,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := message.New("ch-1", []byte("test"))
	resp := &message.Response{StatusCode: 200, Body: []byte("ok")}

	// Should not panic
	cr.storeResponseMessage(msg, resp)
}

func TestChannelRuntime_StoreResponseMessage_NilResponse(t *testing.T) {
	memStore := storage.NewMemoryStore(100, 0)
	cr := &ChannelRuntime{
		ID:     "ch-1",
		Store:  memStore,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	msg := message.New("ch-1", []byte("test"))

	cr.storeResponseMessage(msg, nil)

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

	cr.storeResponseMessage(msg, resp)

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

// ===================================================================
// resolveActiveDestinations — with routing rules
// ===================================================================

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

// ===================================================================
// HotReloader — handleEvent with different event types
// ===================================================================

func TestHotReloader_HandleEvent_ChannelYAML_Write(t *testing.T) {
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

	hr, err := NewHotReloader(engine, channelsDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	event := fsnotify.Event{
		Name: filepath.Join(chDir, "channel.yaml"),
		Op:   fsnotify.Write,
	}
	hr.handleEvent(event)
}

func TestHotReloader_HandleEvent_ChannelYAML_Remove(t *testing.T) {
	tmpDir := t.TempDir()
	channelsDir := filepath.Join(tmpDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	chDir := filepath.Join(channelsDir, "test-ch")
	os.MkdirAll(chDir, 0o755)
	os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte("id: test-ch\nenabled: true\nlistener:\n  http:\n    port: 0\n"), 0o644)

	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine(tmpDir, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	hr, err := NewHotReloader(engine, channelsDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	event := fsnotify.Event{
		Name: filepath.Join(chDir, "channel.yaml"),
		Op:   fsnotify.Remove,
	}
	hr.handleEvent(event)
}

func TestHotReloader_HandleEvent_TSFile_NoChannel(t *testing.T) {
	tmpDir := t.TempDir()
	channelsDir := filepath.Join(tmpDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	chDir := filepath.Join(channelsDir, "test-ch")
	os.MkdirAll(chDir, 0o755)

	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine(tmpDir, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	hr, err := NewHotReloader(engine, channelsDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	event := fsnotify.Event{
		Name: filepath.Join(chDir, "transformer.ts"),
		Op:   fsnotify.Write,
	}
	// Should not panic — no channel.yaml in dir
	hr.handleEvent(event)
}

func TestHotReloader_HandleEvent_NewDir_Create(t *testing.T) {
	tmpDir := t.TempDir()
	channelsDir := filepath.Join(tmpDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine(tmpDir, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	hr, err := NewHotReloader(engine, channelsDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	newChDir := filepath.Join(channelsDir, "new-channel")
	os.MkdirAll(newChDir, 0o755)
	os.WriteFile(filepath.Join(newChDir, "channel.yaml"), []byte("id: new-channel\nenabled: false\nlistener:\n  http:\n    port: 0\n"), 0o644)

	event := fsnotify.Event{
		Name: newChDir,
		Op:   fsnotify.Create,
	}
	hr.handleEvent(event)
}

func TestHotReloader_HandleEvent_NonChannelFile(t *testing.T) {
	tmpDir := t.TempDir()
	channelsDir := filepath.Join(tmpDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine(tmpDir, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	hr, err := NewHotReloader(engine, channelsDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	defer hr.watcher.Close()

	event := fsnotify.Event{
		Name: filepath.Join(channelsDir, "readme.md"),
		Op:   fsnotify.Write,
	}
	// Non-yaml, non-ts files should be ignored
	hr.handleEvent(event)
}

// ===================================================================
// HotReloader — watchLoop with temp dirs
// ===================================================================

func TestHotReloader_WatchLoop_CancelledContext(t *testing.T) {
	tmpDir := t.TempDir()
	channelsDir := filepath.Join(tmpDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	engine := NewDefaultEngine(tmpDir, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	hr, err := NewHotReloader(engine, channelsDir, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	hr.ctx = ctx
	hr.cancel = cancel

	done := make(chan struct{})
	hr.wg.Add(1)
	go func() {
		hr.watchLoop()
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("watchLoop did not exit")
	}
	hr.watcher.Close()
}

func TestHotReloader_WatchLoop_FileChange(t *testing.T) {
	tmpDir := t.TempDir()
	channelsDir := filepath.Join(tmpDir, "channels")
	os.MkdirAll(channelsDir, 0o755)

	chDir := filepath.Join(channelsDir, "ch1")
	os.MkdirAll(chDir, 0o755)
	yamlPath := filepath.Join(chDir, "channel.yaml")
	os.WriteFile(yamlPath, []byte("id: ch1\nenabled: false\nlistener:\n  http:\n    port: 0\n"), 0o644)

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

	// Modify the file and give fsnotify a moment to detect it
	os.WriteFile(yamlPath, []byte("id: ch1\nenabled: false\nlistener:\n  http:\n    port: 9999\n"), 0o644)
	time.Sleep(200 * time.Millisecond)

	cancel()
	time.Sleep(100 * time.Millisecond)
}
