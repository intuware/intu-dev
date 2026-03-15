package runtime

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/internal/observability"
	"github.com/intuware/intu-dev/internal/retry"
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
)

func pushTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ===================================================================
// Pipeline helper functions
// ===================================================================

func TestPush_Pipeline_ResolveScriptPath_EmptyScript(t *testing.T) {
	p := &Pipeline{channelDir: "/ch", projectDir: "/proj", channelID: "ch1", logger: pushTestLogger()}
	path := p.resolveScriptPath("")
	if path != "/ch" {
		t.Fatalf("expected /ch (channelDir), got %q", path)
	}
}

func TestPush_Pipeline_ResolveScriptPath_TSExtension(t *testing.T) {
	dir := t.TempDir()
	distDir := filepath.Join(dir, "dist")
	os.MkdirAll(distDir, 0o755)
	os.WriteFile(filepath.Join(distDir, "transform.js"), []byte("// js"), 0o644)

	p := &Pipeline{channelDir: dir, projectDir: dir, channelID: "ch1", logger: pushTestLogger()}
	path := p.resolveScriptPath("transform.ts")
	if path == "" {
		t.Fatal("expected resolved path for .ts file")
	}
}

func TestPush_Pipeline_ResolveTransformer_Empty(t *testing.T) {
	cfg := &config.ChannelConfig{}
	p := &Pipeline{config: cfg, logger: pushTestLogger()}
	path := p.resolveTransformer()
	if path != "" {
		t.Fatalf("expected empty, got %q", path)
	}
}

func TestPush_Pipeline_ResolveTransformer_FromPipelineConfig(t *testing.T) {
	dir := t.TempDir()
	distDir := filepath.Join(dir, "dist")
	os.MkdirAll(distDir, 0o755)
	os.WriteFile(filepath.Join(distDir, "tx.js"), []byte("// js"), 0o644)

	cfg := &config.ChannelConfig{
		Pipeline: &config.PipelineConfig{Transformer: "tx.ts"},
	}
	p := &Pipeline{config: cfg, channelDir: dir, projectDir: dir, logger: pushTestLogger()}
	path := p.resolveTransformer()
	if path == "" {
		t.Fatal("expected resolved transformer path")
	}
}

func TestPush_Pipeline_ResolveValidator_Empty(t *testing.T) {
	cfg := &config.ChannelConfig{}
	p := &Pipeline{config: cfg, logger: pushTestLogger()}
	path := p.resolveValidator()
	if path != "" {
		t.Fatalf("expected empty, got %q", path)
	}
}

func TestPush_Pipeline_ResolveValidator_FromPipelineConfig(t *testing.T) {
	dir := t.TempDir()
	distDir := filepath.Join(dir, "dist")
	os.MkdirAll(distDir, 0o755)
	os.WriteFile(filepath.Join(distDir, "validate.js"), []byte("// js"), 0o644)

	cfg := &config.ChannelConfig{
		Pipeline: &config.PipelineConfig{Validator: "validate.ts"},
	}
	p := &Pipeline{config: cfg, channelDir: dir, projectDir: dir, logger: pushTestLogger()}
	path := p.resolveValidator()
	if path == "" {
		t.Fatal("expected resolved validator path")
	}
}

func TestPush_Pipeline_SetMessageStore(t *testing.T) {
	p := &Pipeline{logger: pushTestLogger()}
	store := storage.NewMemoryStore(0, 0)
	p.SetMessageStore(store)
	if p.store != store {
		t.Fatal("store not set")
	}
}

func TestPush_Pipeline_SetResolvedDestinations(t *testing.T) {
	p := &Pipeline{logger: pushTestLogger()}
	dests := map[string]config.Destination{
		"d1": {Type: "http"},
	}
	p.SetResolvedDestinations(dests)
	if p.resolvedDests["d1"].Type != "http" {
		t.Fatal("resolved destinations not set")
	}
}

func TestPush_Pipeline_BuildIntuMessage(t *testing.T) {
	cfg := &config.ChannelConfig{ID: "ch1"}
	p := &Pipeline{config: cfg, logger: pushTestLogger(), channelID: "ch1"}
	msg := message.New("ch1", []byte(`{"key":"value"}`))
	msg.ContentType = "json"
	intuMsg := p.buildIntuMessage(msg, nil)
	if intuMsg == nil {
		t.Fatal("expected non-nil intuMsg")
	}
	if intuMsg["contentType"] != "json" {
		t.Fatalf("expected contentType=json, got %v", intuMsg["contentType"])
	}
}

func TestPush_Pipeline_ToBytes_String(t *testing.T) {
	p := &Pipeline{logger: pushTestLogger()}
	b := p.toBytes("hello")
	if string(b) != "hello" {
		t.Fatalf("expected hello, got %q", string(b))
	}
}

func TestPush_Pipeline_ToBytes_Map(t *testing.T) {
	p := &Pipeline{logger: pushTestLogger()}
	b := p.toBytes(map[string]any{"a": 1})
	if len(b) == 0 {
		t.Fatal("expected non-empty bytes")
	}
}

func TestPush_Pipeline_ToBytes_Bytes(t *testing.T) {
	p := &Pipeline{logger: pushTestLogger()}
	in := []byte("raw bytes")
	b := p.toBytes(in)
	if string(b) != "raw bytes" {
		t.Fatalf("expected raw bytes, got %q", string(b))
	}
}

// ===================================================================
// Engine helper functions
// ===================================================================

func TestPush_Engine_DependenciesMet_NoDeps(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}}
	e := NewDefaultEngine("/tmp", cfg, nil, pushTestLogger())
	chCfg := &config.ChannelConfig{ID: "ch1"}
	if !e.dependenciesMet(chCfg, map[string]bool{}) {
		t.Fatal("should be true with no dependencies")
	}
}

func TestPush_Engine_DependenciesMet_WithDeps(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}}
	e := NewDefaultEngine("/tmp", cfg, nil, pushTestLogger())
	chCfg := &config.ChannelConfig{ID: "ch2", DependsOn: []string{"ch1"}}
	if e.dependenciesMet(chCfg, map[string]bool{}) {
		t.Fatal("should be false when dep not started")
	}
	if !e.dependenciesMet(chCfg, map[string]bool{"ch1": true}) {
		t.Fatal("should be true when dep started")
	}
}

func TestPush_Engine_Setters(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}}
	e := NewDefaultEngine("/tmp", cfg, nil, pushTestLogger())

	store := storage.NewMemoryStore(0, 0)
	e.SetMessageStore(store)
	if e.MessageStore() != store {
		t.Fatal("store not set")
	}

	m := e.Metrics()
	if m == nil {
		t.Fatal("expected non-nil metrics")
	}

	if e.RootDir() != "/tmp" {
		t.Fatalf("expected /tmp, got %q", e.RootDir())
	}

	if e.Config() != cfg {
		t.Fatal("config mismatch")
	}
}

func TestPush_Engine_ListChannelIDs_Empty(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}}
	e := NewDefaultEngine("/tmp", cfg, nil, pushTestLogger())
	ids := e.ListChannelIDs()
	if len(ids) != 0 {
		t.Fatalf("expected 0 IDs, got %d", len(ids))
	}
}

func TestPush_Engine_GetChannelRuntime_Missing(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}}
	e := NewDefaultEngine("/tmp", cfg, nil, pushTestLogger())
	cr, ok := e.GetChannelRuntime("nonexistent")
	if ok || cr != nil {
		t.Fatal("expected nil for missing channel")
	}
}

func TestPush_Engine_FindChannelDir(t *testing.T) {
	dir := t.TempDir()
	chDir := filepath.Join(dir, "channels", "my-channel")
	os.MkdirAll(chDir, 0o755)
	os.WriteFile(filepath.Join(chDir, "channel.yaml"), []byte("id: my-channel\nenabled: true\n"), 0o644)

	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Mode: "standalone"},
		ChannelsDir: "channels",
	}
	e := NewDefaultEngine(dir, cfg, nil, pushTestLogger())
	found := e.findChannelDir("my-channel")
	if found == "" {
		t.Fatal("expected to find channel dir")
	}
}

func TestPush_Engine_FindChannelDir_NotFound(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "channels"), 0o755)

	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Mode: "standalone"},
		ChannelsDir: "channels",
	}
	e := NewDefaultEngine(dir, cfg, nil, pushTestLogger())
	found := e.findChannelDir("nonexistent")
	if found != "" {
		t.Fatalf("expected empty, got %q", found)
	}
}

func TestPush_Engine_StopEmpty(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}}
	e := NewDefaultEngine("/tmp", cfg, nil, pushTestLogger())
	if err := e.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestPush_Engine_DeployChannel_NotFound(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "channels"), 0o755)
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Mode: "standalone"},
		ChannelsDir: "channels",
	}
	e := NewDefaultEngine(dir, cfg, nil, pushTestLogger())
	err := e.DeployChannel(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing channel")
	}
}

func TestPush_Engine_UndeployChannel_NotRunning(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}}
	e := NewDefaultEngine("/tmp", cfg, nil, pushTestLogger())
	err := e.UndeployChannel(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("expected nil for non-running channel, got %v", err)
	}
}

func TestPush_Engine_RestartChannel_NotRunning(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "channels"), 0o755)
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Mode: "standalone"},
		ChannelsDir: "channels",
	}
	e := NewDefaultEngine(dir, cfg, nil, pushTestLogger())
	err := e.RestartChannel(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for non-running channel")
	}
}

// ===================================================================
// ChannelRuntime — storeIntuMessage / storeResponseMessage
// ===================================================================

func TestPush_ChannelRuntime_StoreIntuMessage_NilStore(t *testing.T) {
	cr := &ChannelRuntime{
		ID:     "ch1",
		Logger: pushTestLogger(),
		Store:  nil,
	}
	msg := message.New("ch1", []byte("test"))
	cr.storeIntuMessage(msg, "received", "RECEIVED")
}

func TestPush_ChannelRuntime_StoreIntuMessage_MemoryStore(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	cr := &ChannelRuntime{
		ID:     "ch1",
		Logger: pushTestLogger(),
		Store:  store,
	}
	msg := message.New("ch1", []byte(`{"key":"value"}`))
	cr.storeIntuMessage(msg, "received", "RECEIVED")

	records, _ := store.Query(storage.QueryOpts{ChannelID: "ch1"})
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Stage != "received" {
		t.Fatalf("expected stage=received, got %q", records[0].Stage)
	}
}

func TestPush_ChannelRuntime_StoreIntuMessage_WithDuration(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	cr := &ChannelRuntime{
		ID:     "ch1",
		Logger: pushTestLogger(),
		Store:  store,
	}
	msg := message.New("ch1", []byte("test"))
	cr.storeIntuMessage(msg, "error", "ERROR", int64(42))

	records, _ := store.Query(storage.QueryOpts{ChannelID: "ch1"})
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].DurationMs != 42 {
		t.Fatalf("expected durationMs=42, got %d", records[0].DurationMs)
	}
}

func TestPush_ChannelRuntime_StoreResponseMessage_NilStore(t *testing.T) {
	cr := &ChannelRuntime{ID: "ch1", Logger: pushTestLogger(), Store: nil}
	msg := message.New("ch1", []byte("test"))
	resp := &message.Response{StatusCode: 200, Body: []byte("ok")}
	cr.storeResponseMessage(msg, resp)
}

func TestPush_ChannelRuntime_StoreResponseMessage_NilResponse(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	cr := &ChannelRuntime{ID: "ch1", Logger: pushTestLogger(), Store: store}
	msg := message.New("ch1", []byte("test"))
	cr.storeResponseMessage(msg, nil)
	records, _ := store.Query(storage.QueryOpts{ChannelID: "ch1"})
	if len(records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(records))
	}
}

func TestPush_ChannelRuntime_StoreResponseMessage_Success(t *testing.T) {
	store := storage.NewMemoryStore(0, 0)
	cr := &ChannelRuntime{ID: "ch1", Logger: pushTestLogger(), Store: store}
	msg := message.New("ch1", []byte("test"))
	resp := &message.Response{StatusCode: 200, Body: []byte(`{"ok":true}`)}
	cr.storeResponseMessage(msg, resp)

	records, _ := store.Query(storage.QueryOpts{ChannelID: "ch1"})
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Stage != "response" {
		t.Fatalf("expected stage=response, got %q", records[0].Stage)
	}
}

// ===================================================================
// ChannelRuntime — resolveActiveDestinations
// ===================================================================

func TestPush_ChannelRuntime_ResolveActiveDestinations_Empty(t *testing.T) {
	destConfigs := []config.ChannelDestination{
		{Name: "d1"},
		{Name: "d2"},
	}
	cr := &ChannelRuntime{DestConfigs: destConfigs}
	active := cr.resolveActiveDestinations(nil)
	if len(active) != 2 {
		t.Fatalf("expected 2, got %d", len(active))
	}
}

func TestPush_ChannelRuntime_ResolveActiveDestinations_Selective(t *testing.T) {
	destConfigs := []config.ChannelDestination{
		{Name: "d1"},
		{Name: "d2"},
		{Name: "d3"},
	}
	cr := &ChannelRuntime{DestConfigs: destConfigs}
	active := cr.resolveActiveDestinations([]string{"d1", "d3"})
	if len(active) != 2 {
		t.Fatalf("expected 2, got %d", len(active))
	}
	if active[0].Name != "d1" || active[1].Name != "d3" {
		t.Fatalf("wrong active destinations: %v", active)
	}
}

func TestPush_ChannelRuntime_ResolveActiveDestinations_RefFallback(t *testing.T) {
	destConfigs := []config.ChannelDestination{
		{Ref: "ref-d1"},
		{Name: "d2"},
	}
	cr := &ChannelRuntime{DestConfigs: destConfigs}
	active := cr.resolveActiveDestinations([]string{"ref-d1"})
	if len(active) != 1 {
		t.Fatalf("expected 1, got %d", len(active))
	}
}

// ===================================================================
// Engine — metrics and store getters
// ===================================================================

func TestPush_Engine_MetricsNotNil(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}}
	e := NewDefaultEngine("/tmp", cfg, nil, pushTestLogger())
	m := e.Metrics()
	if m == nil {
		t.Fatal("expected metrics to be non-nil from global")
	}
}

func TestPush_Engine_SetMessageStore_Twice(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}}
	e := NewDefaultEngine("/tmp", cfg, nil, pushTestLogger())
	s1 := storage.NewMemoryStore(0, 0)
	s2 := storage.NewMemoryStore(0, 0)
	e.SetMessageStore(s1)
	e.SetMessageStore(s2)
	if e.MessageStore() != s2 {
		t.Fatal("expected second store")
	}
}

// ===================================================================
// SyncMap coverage
// ===================================================================

func TestPush_SyncMap_Operations(t *testing.T) {
	m := NewConnectorMap()

	m.Put("key1", "val1")
	val, ok := m.Get("key1")
	if !ok || val != "val1" {
		t.Fatalf("expected val1, got %v", val)
	}

	_, ok = m.Get("nonexistent")
	if ok {
		t.Fatal("expected false for missing key")
	}

	m.Put("key2", "val2")
	snap := m.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2, got %d", len(snap))
	}

	m.Remove("key1")
	_, ok = m.Get("key1")
	if ok {
		t.Fatal("expected false after remove")
	}

	m.Clear()
	snap = m.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("expected 0 after clear, got %d", len(snap))
	}
}

// ===================================================================
// MapVariables
// ===================================================================

func TestPush_MapVariables(t *testing.T) {
	m := NewMapVariables()
	if m == nil {
		t.Fatal("expected non-nil MapVariables")
	}
}

// ===================================================================
// Metrics and observability wiring
// ===================================================================

func TestPush_ChannelRuntime_HandleMessage_WithMetrics(t *testing.T) {
	metrics := observability.NewMetrics()
	store := storage.NewMemoryStore(0, 0)
	stubSrc := connector.NewStubSource("stub", pushTestLogger())
	logDest := connector.NewLogDest("log", pushTestLogger())

	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "ch1")
	os.MkdirAll(filepath.Join(channelDir, "dist"), 0o755)

	chCfg := &config.ChannelConfig{
		ID:      "ch1",
		Enabled: true,
	}

	pipeline := NewPipeline(channelDir, dir, "ch1", chCfg, nil, pushTestLogger())

	cr := &ChannelRuntime{
		ID:           "ch1",
		Config:       chCfg,
		Source:       stubSrc,
		Destinations: map[string]connector.DestinationConnector{"log": logDest},
		DestConfigs:  []config.ChannelDestination{{Name: "log"}},
		Pipeline:     pipeline,
		Logger:       pushTestLogger(),
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

// ===================================================================
// ChannelRuntime — sendToDestination direct
// ===================================================================

func TestPush_ChannelRuntime_SendToDestination_Direct(t *testing.T) {
	logDest := connector.NewLogDest("log", pushTestLogger())
	cr := &ChannelRuntime{
		ID:           "ch1",
		Logger:       pushTestLogger(),
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

// ===================================================================
// HotReloader — constructor
// ===================================================================

func TestPush_HotReloader_NewHotReloader(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}, ChannelsDir: "channels"}
	e := NewDefaultEngine(dir, cfg, nil, pushTestLogger())
	chDir := filepath.Join(dir, "channels")
	os.MkdirAll(chDir, 0o755)

	hr, err := NewHotReloader(e, chDir, pushTestLogger())
	if err != nil {
		t.Fatalf("NewHotReloader: %v", err)
	}
	if hr == nil {
		t.Fatal("expected non-nil HotReloader")
	}
}

// ===================================================================
// PluginRegistry
// ===================================================================

func TestPush_PluginRegistry_EmptyHasNoPlugins(t *testing.T) {
	pr := NewPluginRegistry()
	if pr.HasPlugins(Phase("pre_transform")) {
		t.Fatal("should have no plugins")
	}
}

func TestPush_PluginRegistry_ExecuteEmptyPhase(t *testing.T) {
	pr := NewPluginRegistry()
	msg := message.New("", []byte("test"))
	result, err := pr.Execute(context.Background(), Phase("pre_transform"), msg, pushTestLogger())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result != msg {
		t.Fatal("expected same message returned for empty phase")
	}
}

// ===================================================================
// CodeTemplateLoader
// ===================================================================

func TestPush_CodeTemplateLoader_Basic(t *testing.T) {
	dir := t.TempDir()
	loader := NewCodeTemplateLoader(dir, pushTestLogger())
	if loader == nil {
		t.Fatal("expected non-nil loader")
	}
}

func TestPush_CodeTemplateLoader_LoadLibrary(t *testing.T) {
	dir := t.TempDir()
	libDir := filepath.Join(dir, "libs", "mylib")
	os.MkdirAll(libDir, 0o755)
	os.WriteFile(filepath.Join(libDir, "helper.ts"), []byte("export function helper() {}"), 0o644)

	loader := NewCodeTemplateLoader(dir, pushTestLogger())
	err := loader.LoadLibrary("mylib", "libs/mylib")
	if err != nil {
		t.Fatalf("LoadLibrary: %v", err)
	}
}

func TestPush_CodeTemplateLoader_ResolveFunction(t *testing.T) {
	dir := t.TempDir()
	libDir := filepath.Join(dir, "libs", "mylib")
	os.MkdirAll(libDir, 0o755)
	os.WriteFile(filepath.Join(libDir, "helper.ts"), []byte("export function helper() {}"), 0o644)

	loader := NewCodeTemplateLoader(dir, pushTestLogger())
	loader.LoadLibrary("mylib", "libs/mylib")

	_, found := loader.ResolveFunction("helper")
	_ = found
}

// ===================================================================
// Engine — TryAcquirePendingChannels
// ===================================================================

func TestPush_Engine_TryAcquirePendingChannels_Empty(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}}
	e := NewDefaultEngine("/tmp", cfg, nil, pushTestLogger())
	e.tryAcquirePendingChannels(context.Background())
}

// ===================================================================
// Engine — CloseRuntime with nil runner
// ===================================================================

func TestPush_Engine_CloseRuntime_NilRunner(t *testing.T) {
	cfg := &config.Config{Runtime: config.RuntimeConfig{Mode: "standalone"}}
	e := NewDefaultEngine("/tmp", cfg, nil, pushTestLogger())
	e.CloseRuntime()
}

// ===================================================================
// ChannelRuntime — CompositeStore filtering
// ===================================================================

func TestPush_ChannelRuntime_StoreIntuMessage_CompositeStore_None(t *testing.T) {
	inner := storage.NewMemoryStore(0, 0)
	cs := storage.NewCompositeStore(inner, "none", nil)

	cr := &ChannelRuntime{
		ID:     "ch1",
		Logger: pushTestLogger(),
		Store:  cs,
	}

	msg := message.New("ch1", []byte("test"))
	cr.storeIntuMessage(msg, "received", "RECEIVED")
	records, _ := inner.Query(storage.QueryOpts{ChannelID: "ch1"})
	if len(records) != 0 {
		t.Fatalf("expected 0 records in 'none' mode, got %d", len(records))
	}
}

func TestPush_ChannelRuntime_StoreIntuMessage_CompositeStore_Full(t *testing.T) {
	inner := storage.NewMemoryStore(0, 0)
	cs := storage.NewCompositeStore(inner, "full", []string{"received", "error"})

	cr := &ChannelRuntime{
		ID:     "ch1",
		Logger: pushTestLogger(),
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

// ===================================================================
// ChannelRuntime — Stop
// ===================================================================

func TestPush_ChannelRuntime_Stop(t *testing.T) {
	stubSrc := connector.NewStubSource("stub", pushTestLogger())
	logDest := connector.NewLogDest("log", pushTestLogger())

	cr := &ChannelRuntime{
		ID:           "ch1",
		Source:       stubSrc,
		Destinations: map[string]connector.DestinationConnector{"log": logDest},
		Logger:       pushTestLogger(),
		queues:       make(map[string]*retry.DestinationQueue),
		redisQueues:  make(map[string]*retry.RedisDestinationQueue),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cr.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
