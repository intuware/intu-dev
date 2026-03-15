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
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
)

type stubSource struct {
	handler connector.MessageHandler
}

func (s *stubSource) Start(ctx context.Context, handler connector.MessageHandler) error {
	s.handler = handler
	return nil
}
func (s *stubSource) Stop(ctx context.Context) error { return nil }
func (s *stubSource) Type() string                   { return "stub" }

type stubDest struct {
	sent []*message.Message
}

func (d *stubDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	d.sent = append(d.sent, msg)
	return &message.Response{StatusCode: 200, Body: []byte(`{"ok":true}`)}, nil
}
func (d *stubDest) Stop(ctx context.Context) error { return nil }
func (d *stubDest) Type() string                   { return "log" }

type stubFactory struct {
	source *stubSource
	dest   *stubDest
}

func (f *stubFactory) CreateSource(cfg config.ListenerConfig) (connector.SourceConnector, error) {
	f.source = &stubSource{}
	return f.source, nil
}
func (f *stubFactory) CreateDestination(name string, dest config.Destination) (connector.DestinationConnector, error) {
	f.dest = &stubDest{}
	return f.dest, nil
}

func TestChannelRuntime_StartStop(t *testing.T) {
	src := &stubSource{}
	dest := &stubDest{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cr := &ChannelRuntime{
		ID: "test-ch",
		Config: &config.ChannelConfig{
			ID:      "test-ch",
			Enabled: true,
		},
		Source:       src,
		Destinations: map[string]connector.DestinationConnector{"log-dest": dest},
		Pipeline:     NewPipeline(t.TempDir(), t.TempDir(), "test-ch", &config.ChannelConfig{ID: "test-ch"}, nil, logger),
		Logger:       logger,
		Metrics:      observability.NewMetrics(),
	}

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if src.handler == nil {
		t.Fatal("expected handler to be set after Start")
	}
	if err := cr.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestChannelRuntime_HandleMessage_NoTransformer(t *testing.T) {
	src := &stubSource{}
	dest := &stubDest{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := storage.NewMemoryStore(0, 0)

	chCfg := &config.ChannelConfig{
		ID:      "test-ch",
		Enabled: true,
		Listener: config.ListenerConfig{
			Type: "http",
		},
		Destinations: []config.ChannelDestination{
			{Name: "log-dest", Type: "log"},
		},
	}

	pipeline := NewPipeline(t.TempDir(), t.TempDir(), "test-ch", chCfg, nil, logger)

	cr := &ChannelRuntime{
		ID:     "test-ch",
		Config: chCfg,
		Source: src,
		Destinations: map[string]connector.DestinationConnector{
			"log-dest": dest,
		},
		DestConfigs: chCfg.Destinations,
		Pipeline:    pipeline,
		Logger:      logger,
		Metrics:     observability.NewMetrics(),
		Store:       store,
		Maps:        NewMapVariables(),
	}

	ctx := context.Background()
	if err := cr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	msg := message.New("", []byte(`{"test":"data"}`))
	msg.Transport = "http"

	if err := cr.HandleMessage(ctx, msg); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	if len(dest.sent) != 1 {
		t.Fatalf("expected 1 sent message, got %d", len(dest.sent))
	}
}

func TestChannelRuntime_StoreIntuMessage(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := storage.NewMemoryStore(0, 0)

	cr := &ChannelRuntime{
		ID:     "store-ch",
		Config: &config.ChannelConfig{ID: "store-ch"},
		Logger: logger,
		Store:  store,
	}

	msg := message.New("", []byte("test body"))
	cr.storeIntuMessage(msg, "received", "RECEIVED")

	records, err := store.Query(storage.QueryOpts{ChannelID: "store-ch"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Stage != "received" {
		t.Errorf("expected stage 'received', got %q", records[0].Stage)
	}
}

func TestChannelRuntime_StoreIntuMessage_NilStore(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cr := &ChannelRuntime{
		ID:     "nil-store-ch",
		Config: &config.ChannelConfig{ID: "nil-store-ch"},
		Logger: logger,
		Store:  nil,
	}

	msg := message.New("", []byte("test body"))
	cr.storeIntuMessage(msg, "received", "RECEIVED")
}

func TestChannelRuntime_StoreResponseMessage(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := storage.NewMemoryStore(0, 0)

	cr := &ChannelRuntime{
		ID:     "resp-ch",
		Config: &config.ChannelConfig{ID: "resp-ch"},
		Logger: logger,
		Store:  store,
	}

	msg := message.New("", []byte("test body"))
	resp := &message.Response{
		StatusCode: 200,
		Body:       []byte(`{"ok":true}`),
		Headers:    map[string]string{"Content-Type": "application/json"},
	}
	cr.storeResponseMessage(msg, resp)
}

func TestChannelRuntime_StoreResponseMessage_NilStore(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cr := &ChannelRuntime{
		ID:     "nil-resp-ch",
		Logger: logger,
		Store:  nil,
	}

	msg := message.New("", []byte("test body"))
	resp := &message.Response{StatusCode: 200}
	cr.storeResponseMessage(msg, resp)
}

func TestChannelRuntime_StoreResponseMessage_NilResp(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := storage.NewMemoryStore(0, 0)

	cr := &ChannelRuntime{
		ID:     "nil-resp-msg-ch",
		Logger: logger,
		Store:  store,
	}

	msg := message.New("", []byte("test body"))
	cr.storeResponseMessage(msg, nil)
}

func TestChannelRuntime_StoreIntuMessage_WithDuration(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := storage.NewMemoryStore(0, 0)

	cr := &ChannelRuntime{
		ID:     "dur-ch",
		Config: &config.ChannelConfig{ID: "dur-ch"},
		Logger: logger,
		Store:  store,
	}

	msg := message.New("", []byte("test body"))
	cr.storeIntuMessage(msg, "processed", "SENT", 150)

	records, _ := store.Query(storage.QueryOpts{ChannelID: "dur-ch"})
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].DurationMs != 150 {
		t.Errorf("expected DurationMs=150, got %d", records[0].DurationMs)
	}
}

func TestChannelRuntime_ResolveActiveDestinations(t *testing.T) {
	cr := &ChannelRuntime{
		DestConfigs: []config.ChannelDestination{
			{Name: "dest-a"},
			{Name: "dest-b"},
			{Name: "dest-c"},
		},
	}

	all := cr.resolveActiveDestinations(nil)
	if len(all) != 3 {
		t.Fatalf("expected 3, got %d", len(all))
	}

	routed := cr.resolveActiveDestinations([]string{"dest-b"})
	if len(routed) != 1 {
		t.Fatalf("expected 1, got %d", len(routed))
	}
	if routed[0].Name != "dest-b" {
		t.Errorf("expected dest-b, got %q", routed[0].Name)
	}
}

func TestChannelRuntime_ResolveActiveDestinations_RefOnly(t *testing.T) {
	cr := &ChannelRuntime{
		DestConfigs: []config.ChannelDestination{
			{Ref: "ref-a"},
			{Ref: "ref-b"},
		},
	}

	routed := cr.resolveActiveDestinations([]string{"ref-a"})
	if len(routed) != 1 {
		t.Fatalf("expected 1, got %d", len(routed))
	}
}

func TestEngine_InitRuntime_CloseRuntime(t *testing.T) {
	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			Name:       "test-engine",
			WorkerPool: 1,
		},
		ChannelsDir: "channels",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)

	ctx := context.Background()
	err := engine.InitRuntime(ctx)
	if err != nil {
		t.Logf("InitRuntime may fail without node: %v", err)
		return
	}

	if err := engine.CloseRuntime(); err != nil {
		t.Fatalf("CloseRuntime: %v", err)
	}
}

func TestEngine_CloseRuntime_NilRunner(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)

	if err := engine.CloseRuntime(); err != nil {
		t.Fatalf("CloseRuntime on nil runner: %v", err)
	}
}

func TestEngine_ReprocessMessage_ChannelNotFound(t *testing.T) {
	cfg := &config.Config{
		Runtime:     config.RuntimeConfig{Name: "test"},
		ChannelsDir: "channels",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rootDir := t.TempDir()
	os.MkdirAll(filepath.Join(rootDir, "channels"), 0o755)

	engine := NewDefaultEngine(rootDir, cfg, nil, logger)

	msg := message.New("", []byte("test"))
	err := engine.ReprocessMessage(context.Background(), "nonexistent-ch", msg)
	if err == nil {
		t.Fatal("expected error for nonexistent channel")
	}
}

func TestEngine_PreloadChannelScripts(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)

	chCfg := &config.ChannelConfig{
		ID: "test-ch",
		Pipeline: &config.PipelineConfig{
			Validator:    "validator.ts",
			Transformer:  "transformer.ts",
			Preprocessor: "preprocess.ts",
		},
	}

	// Should not panic even with nil jsRunner when preloading
	engine.preloadChannelScripts(t.TempDir(), chCfg)
}

func TestEngine_DependenciesMet(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)

	chCfg := &config.ChannelConfig{
		ID:        "dep-ch",
		DependsOn: []string{"base-ch"},
	}

	started := map[string]bool{"base-ch": true}
	if !engine.dependenciesMet(chCfg, started) {
		t.Error("expected dependencies met")
	}

	notStarted := map[string]bool{}
	if engine.dependenciesMet(chCfg, notStarted) {
		t.Error("expected dependencies NOT met")
	}

	noDeps := &config.ChannelConfig{ID: "no-deps"}
	if !engine.dependenciesMet(noDeps, notStarted) {
		t.Error("expected no deps to always be met")
	}
}

func TestEngine_ResolveChannelStore_NilStore(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)

	chCfg := &config.ChannelConfig{ID: "test"}
	result := engine.resolveChannelStore(chCfg)
	if result != nil {
		t.Error("expected nil store when engine has no store")
	}
}

func TestEngine_ResolveChannelStore_WithStore(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)
	engine.store = storage.NewMemoryStore(0, 0)

	chCfg := &config.ChannelConfig{ID: "test"}
	result := engine.resolveChannelStore(chCfg)
	if result == nil {
		t.Error("expected non-nil store")
	}
}

func TestEngine_ResolveChannelStore_ChannelOverride(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)
	engine.store = storage.NewMemoryStore(0, 0)

	chCfg := &config.ChannelConfig{
		ID: "test",
		MessageStorage: &config.ChannelStorageConfig{
			Mode: "status",
		},
	}
	result := engine.resolveChannelStore(chCfg)
	if result == nil {
		t.Error("expected non-nil composite store")
	}
}

func TestEngine_ResolveChannelStore_EnabledNoMode(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)
	engine.store = storage.NewMemoryStore(0, 0)

	chCfg := &config.ChannelConfig{
		ID: "test",
		MessageStorage: &config.ChannelStorageConfig{
			Enabled: true,
		},
	}
	result := engine.resolveChannelStore(chCfg)
	if result == nil {
		t.Error("expected non-nil store")
	}
}

func TestEngine_FindChannelDir_NotFound(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rootDir := t.TempDir()
	os.MkdirAll(filepath.Join(rootDir, "channels"), 0o755)
	engine := NewDefaultEngine(rootDir, cfg, nil, logger)

	dir := engine.findChannelDir("nonexistent")
	if dir != "" {
		t.Errorf("expected empty string, got %q", dir)
	}
}

func TestEngine_Getters(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine("/tmp/test-proj", cfg, nil, logger)
	store := storage.NewMemoryStore(0, 0)
	engine.SetMessageStore(store)

	if engine.RootDir() != "/tmp/test-proj" {
		t.Errorf("RootDir = %q", engine.RootDir())
	}
	if engine.Config() != cfg {
		t.Error("Config mismatch")
	}
	if engine.MessageStore() != store {
		t.Error("MessageStore mismatch")
	}
	if engine.Metrics() == nil {
		t.Error("Metrics should not be nil")
	}
}

func TestEngine_ListChannelIDs_Empty(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)

	ids := engine.ListChannelIDs()
	if len(ids) != 0 {
		t.Errorf("expected 0 IDs, got %d", len(ids))
	}
}

func TestEngine_GetChannelRuntime_NotFound(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)

	_, ok := engine.GetChannelRuntime("nonexistent")
	if ok {
		t.Error("expected channel not found")
	}
}

func TestEngine_UndeployChannel_NotRunning(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)

	err := engine.UndeployChannel(context.Background(), "not-running")
	if err != nil {
		t.Fatalf("expected nil error for non-running channel, got %v", err)
	}
}

func TestEngine_DeployChannel_NotFound(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rootDir := t.TempDir()
	os.MkdirAll(filepath.Join(rootDir, "channels"), 0o755)
	engine := NewDefaultEngine(rootDir, cfg, nil, logger)

	err := engine.DeployChannel(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent channel")
	}
}

func TestScriptPlugin_ResolveScriptPath_TS(t *testing.T) {
	sp := &ScriptPlugin{
		channelDir: "/project/channels/my-ch",
		projectDir: "/project",
	}
	path := sp.resolveScriptPath("transform.ts")
	expected := filepath.Join("/project", "dist", "channels", "my-ch", "transform.js")
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestScriptPlugin_ResolveScriptPath_JS(t *testing.T) {
	sp := &ScriptPlugin{
		channelDir: "/project/channels/my-ch",
		projectDir: "/project",
	}
	path := sp.resolveScriptPath("transform.js")
	expected := filepath.Join("/project", "channels", "my-ch", "transform.js")
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestNewScriptPlugin_InvalidPhase(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := NewScriptPlugin(config.PluginConfig{
		Name:       "test-plugin",
		Phase:      "invalid_phase",
		Entrypoint: "plugin.ts",
	}, "/ch", "/proj", nil, logger)
	if err == nil {
		t.Fatal("expected error for invalid phase")
	}
}

func TestNewScriptPlugin_ValidPhases(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	phases := []string{
		"before_validation", "after_validation",
		"before_transform", "after_transform",
		"before_destination", "after_destination",
	}
	for _, phase := range phases {
		sp, err := NewScriptPlugin(config.PluginConfig{
			Name:       "test-plugin",
			Phase:      phase,
			Entrypoint: "plugin.ts",
		}, "/ch", "/proj", nil, logger)
		if err != nil {
			t.Fatalf("unexpected error for phase %q: %v", phase, err)
		}
		if sp.Name() != "test-plugin" {
			t.Errorf("expected name 'test-plugin', got %q", sp.Name())
		}
		if string(sp.Phase()) != phase {
			t.Errorf("expected phase %q, got %q", phase, sp.Phase())
		}
	}
}

func TestPluginRegistry_NoPlugins(t *testing.T) {
	reg := NewPluginRegistry()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	msg := message.New("", []byte("test"))
	out, err := reg.Execute(context.Background(), PhaseBeforeValidation, msg, logger)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if out != msg {
		t.Error("expected same message returned when no plugins")
	}
	if reg.HasPlugins(PhaseBeforeValidation) {
		t.Error("expected no plugins")
	}
}

func TestPipeline_ResolveScriptPath_TS(t *testing.T) {
	p := &Pipeline{
		channelDir: "/project/channels/my-ch",
		projectDir: "/project",
	}
	path := p.resolveScriptPath("transformer.ts")
	expected := filepath.Join("/project", "dist", "channels", "my-ch", "transformer.js")
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestPipeline_ResolveScriptPath_JS(t *testing.T) {
	p := &Pipeline{
		channelDir: "/project/channels/my-ch",
		projectDir: "/project",
	}
	path := p.resolveScriptPath("helper.js")
	expected := filepath.Join("/project", "channels", "my-ch", "helper.js")
	if path != expected {
		t.Errorf("expected %q, got %q", expected, path)
	}
}

func TestPipeline_ResolveValidator(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{
			Pipeline: &config.PipelineConfig{
				Validator: "validator.ts",
			},
		},
	}
	if v := p.resolveValidator(); v != "validator.ts" {
		t.Errorf("expected 'validator.ts', got %q", v)
	}

	p2 := &Pipeline{
		config: &config.ChannelConfig{
			Validator: &config.ScriptRef{
				Entrypoint: "validate.ts",
			},
		},
	}
	if v := p2.resolveValidator(); v != "validate.ts" {
		t.Errorf("expected 'validate.ts', got %q", v)
	}

	p3 := &Pipeline{config: &config.ChannelConfig{}}
	if v := p3.resolveValidator(); v != "" {
		t.Errorf("expected empty, got %q", v)
	}
}

func TestPipeline_ResolveTransformer(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{
			Pipeline: &config.PipelineConfig{
				Transformer: "transformer.ts",
			},
		},
	}
	if v := p.resolveTransformer(); v != "transformer.ts" {
		t.Errorf("expected 'transformer.ts', got %q", v)
	}

	p2 := &Pipeline{
		config: &config.ChannelConfig{
			Transformer: &config.ScriptRef{
				Entrypoint: "transform.ts",
			},
		},
	}
	if v := p2.resolveTransformer(); v != "transform.ts" {
		t.Errorf("expected 'transform.ts', got %q", v)
	}

	p3 := &Pipeline{config: &config.ChannelConfig{}}
	if v := p3.resolveTransformer(); v != "" {
		t.Errorf("expected empty, got %q", v)
	}
}

func TestPipeline_ResolveDestType(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}

	tests := []struct {
		destCfg  config.ChannelDestination
		expected string
	}{
		{config.ChannelDestination{Type: "http"}, "http"},
		{config.ChannelDestination{HTTP: &config.HTTPDestConfig{}}, "http"},
		{config.ChannelDestination{File: &config.FileDestConfig{}}, "file"},
		{config.ChannelDestination{Kafka: &config.KafkaDestConfig{}}, "kafka"},
		{config.ChannelDestination{TCP: &config.TCPDestConfig{}}, "tcp"},
		{config.ChannelDestination{SMTP: &config.SMTPDestConfig{}}, "smtp"},
		{config.ChannelDestination{Database: &config.DBDestConfig{}}, "database"},
		{config.ChannelDestination{DICOM: &config.DICOMDestConfig{}}, "dicom"},
		{config.ChannelDestination{ChannelDest: &config.ChannelDestRef{}}, "channel"},
		{config.ChannelDestination{FHIR: &config.FHIRDestConfig{}}, "fhir"},
		{config.ChannelDestination{JMS: &config.JMSDestConfig{}}, "jms"},
		{config.ChannelDestination{Direct: &config.DirectDestConfig{}}, "direct"},
		{config.ChannelDestination{}, ""},
	}

	for _, tt := range tests {
		got := p.resolveDestType("test", tt.destCfg)
		if got != tt.expected {
			t.Errorf("resolveDestType for %+v: expected %q, got %q", tt.destCfg, tt.expected, got)
		}
	}
}

func TestPipeline_ResolveDestType_FromResolvedDests(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{},
		resolvedDests: map[string]config.Destination{
			"my-dest": {Type: "fhir"},
		},
	}

	got := p.resolveDestType("my-dest", config.ChannelDestination{})
	if got != "fhir" {
		t.Errorf("expected 'fhir', got %q", got)
	}
}

func TestPipeline_ToBytes(t *testing.T) {
	p := &Pipeline{}

	tests := []struct {
		input    any
		expected string
	}{
		{[]byte("hello"), "hello"},
		{"world", "world"},
		{42, "42"},
		{map[string]string{"a": "b"}, `{"a":"b"}`},
	}

	for _, tt := range tests {
		got := string(p.toBytes(tt.input))
		if got != tt.expected {
			t.Errorf("toBytes(%v): expected %q, got %q", tt.input, tt.expected, got)
		}
	}
}

func TestPipeline_BuildIntuMessage(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	msg := message.New("", []byte("test body"))
	msg.Transport = "http"
	msg.HTTP = &message.HTTPMeta{
		Headers:     map[string]string{"X-Test": "1"},
		QueryParams: map[string]string{"q": "search"},
		Method:      "POST",
	}
	msg.Metadata = map[string]any{"key": "value"}
	msg.SourceCharset = "iso-8859-1"

	im := p.buildIntuMessage(msg, "test body")
	if im["transport"] != "http" {
		t.Errorf("expected transport 'http', got %v", im["transport"])
	}
	if im["sourceCharset"] != "iso-8859-1" {
		t.Error("expected sourceCharset")
	}
	if im["metadata"] == nil {
		t.Error("expected metadata")
	}
	httpData, ok := im["http"].(map[string]any)
	if !ok {
		t.Fatal("expected http metadata")
	}
	if httpData["method"] != "POST" {
		t.Errorf("expected POST, got %v", httpData["method"])
	}
}

func TestPipeline_BuildIntuMessage_AllTransports(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}

	msg := message.New("", []byte("test"))
	msg.File = &message.FileMeta{Filename: "test.hl7", Directory: "/input"}
	msg.FTP = &message.FTPMeta{Filename: "ftp.hl7", Directory: "/ftp"}
	msg.Kafka = &message.KafkaMeta{Topic: "hl7", Key: "k1", Partition: 0}
	msg.TCP = &message.TCPMeta{RemoteAddr: "10.0.0.1:6661"}
	msg.SMTP = &message.SMTPMeta{From: "a@b.com", To: []string{"c@d.com"}, Subject: "Test"}
	msg.DICOM = &message.DICOMMeta{CallingAE: "AE1", CalledAE: "AE2"}
	msg.Database = &message.DatabaseMeta{Query: "SELECT 1", Params: map[string]any{"p": "v"}}

	im := p.buildIntuMessage(msg, "test")
	for _, key := range []string{"file", "ftp", "kafka", "tcp", "smtp", "dicom", "database"} {
		if im[key] == nil {
			t.Errorf("expected %q in intu message", key)
		}
	}
}

func TestPipeline_ExecutePostprocessor_NoPipeline(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	err := p.ExecutePostprocessor(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatalf("expected no error: %v", err)
	}
}

func TestPipeline_ExecutePostprocessor_NilPostprocessor(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{
			Pipeline: &config.PipelineConfig{},
		},
	}
	err := p.ExecutePostprocessor(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatalf("expected no error: %v", err)
	}
}

func TestPipeline_ExecuteResponseTransformer_NoTransformer(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	dest := config.ChannelDestination{}
	err := p.ExecuteResponseTransformer(context.Background(), nil, dest, nil)
	if err != nil {
		t.Fatalf("expected no error: %v", err)
	}
}

func TestResolveDestTransformer(t *testing.T) {
	d1 := config.ChannelDestination{}
	if resolveDestTransformer(d1) != "" {
		t.Error("expected empty")
	}

	d2 := config.ChannelDestination{
		Transformer: &config.ScriptRef{Entrypoint: "transform.ts"},
	}
	if resolveDestTransformer(d2) != "transform.ts" {
		t.Error("expected transform.ts")
	}
}

func TestResolveDestResponseTransformer(t *testing.T) {
	d1 := config.ChannelDestination{}
	if resolveDestResponseTransformer(d1) != "" {
		t.Error("expected empty")
	}

	d2 := config.ChannelDestination{
		ResponseTransformer: &config.ScriptRef{Entrypoint: "resp.ts"},
	}
	if resolveDestResponseTransformer(d2) != "resp.ts" {
		t.Error("expected resp.ts")
	}
}

func TestParseHTTPMeta(t *testing.T) {
	data := map[string]any{
		"headers":     map[string]any{"X-Test": "1"},
		"queryParams": map[string]any{"q": "search"},
		"pathParams":  map[string]any{"id": "123"},
		"method":      "GET",
		"statusCode":  float64(200),
	}
	meta := parseHTTPMeta(data)
	if meta.Method != "GET" {
		t.Errorf("expected GET, got %q", meta.Method)
	}
	if meta.StatusCode != 200 {
		t.Errorf("expected 200, got %d", meta.StatusCode)
	}
}

func TestParseFileMeta(t *testing.T) {
	data := map[string]any{"filename": "test.hl7", "directory": "/input"}
	meta := parseFileMeta(data)
	if meta.Filename != "test.hl7" {
		t.Errorf("expected 'test.hl7', got %q", meta.Filename)
	}
}

func TestParseFTPMeta(t *testing.T) {
	data := map[string]any{"filename": "ftp.hl7", "directory": "/ftp"}
	meta := parseFTPMeta(data)
	if meta.Filename != "ftp.hl7" {
		t.Errorf("expected 'ftp.hl7', got %q", meta.Filename)
	}
}

func TestParseKafkaMeta(t *testing.T) {
	data := map[string]any{
		"topic":     "hl7-topic",
		"key":       "key1",
		"partition": float64(3),
		"offset":    float64(100),
		"headers":   map[string]any{"h1": "v1"},
	}
	meta := parseKafkaMeta(data)
	if meta.Topic != "hl7-topic" {
		t.Errorf("expected 'hl7-topic', got %q", meta.Topic)
	}
	if meta.Partition != 3 {
		t.Errorf("expected partition 3, got %d", meta.Partition)
	}
	if meta.Offset != 100 {
		t.Errorf("expected offset 100, got %d", meta.Offset)
	}
}

func TestParseTCPMeta(t *testing.T) {
	data := map[string]any{"remoteAddr": "10.0.0.1:6661"}
	meta := parseTCPMeta(data)
	if meta.RemoteAddr != "10.0.0.1:6661" {
		t.Errorf("expected '10.0.0.1:6661', got %q", meta.RemoteAddr)
	}
}

func TestParseSMTPMeta(t *testing.T) {
	data := map[string]any{
		"from":    "a@b.com",
		"to":      []any{"c@d.com"},
		"subject": "Test",
		"cc":      []any{"e@f.com"},
		"bcc":     []any{"g@h.com"},
	}
	meta := parseSMTPMeta(data)
	if meta.From != "a@b.com" {
		t.Errorf("expected 'a@b.com', got %q", meta.From)
	}
	if len(meta.To) != 1 {
		t.Errorf("expected 1 recipient, got %d", len(meta.To))
	}
	if meta.Subject != "Test" {
		t.Errorf("expected 'Test', got %q", meta.Subject)
	}
}

func TestParseDICOMMeta(t *testing.T) {
	data := map[string]any{"callingAE": "AE1", "calledAE": "AE2"}
	meta := parseDICOMMeta(data)
	if meta.CallingAE != "AE1" {
		t.Errorf("expected 'AE1', got %q", meta.CallingAE)
	}
}

func TestParseDatabaseMeta(t *testing.T) {
	data := map[string]any{
		"query":  "SELECT * FROM t",
		"params": map[string]any{"p1": "v1"},
	}
	meta := parseDatabaseMeta(data)
	if meta.Query != "SELECT * FROM t" {
		t.Errorf("expected query, got %q", meta.Query)
	}
}

func TestToStringMap(t *testing.T) {
	m := toStringMap(map[string]any{"a": "b", "c": 42})
	if m["a"] != "b" {
		t.Errorf("expected 'b', got %q", m["a"])
	}
	if m["c"] != "42" {
		t.Errorf("expected '42', got %q", m["c"])
	}

	empty := toStringMap(nil)
	if len(empty) != 0 {
		t.Error("expected empty map")
	}
}

func TestToStringSlice(t *testing.T) {
	s := toStringSlice([]any{"a", "b", "c"})
	if len(s) != 3 {
		t.Fatalf("expected 3, got %d", len(s))
	}

	nilSlice := toStringSlice(nil)
	if nilSlice != nil {
		t.Error("expected nil")
	}

	mixed := toStringSlice([]any{"a", 42, "b"})
	if len(mixed) != 2 {
		t.Errorf("expected 2, got %d", len(mixed))
	}
}

func TestNonNilMap(t *testing.T) {
	m := nonNilMap(nil)
	if m == nil {
		t.Error("expected non-nil map")
	}

	existing := map[string]string{"a": "b"}
	same := nonNilMap(existing)
	if same["a"] != "b" {
		t.Error("expected existing map returned")
	}
}

func TestNonNilStrMap(t *testing.T) {
	m := nonNilStrMap(nil)
	if m == nil {
		t.Error("expected non-nil map")
	}
	existing := map[string]string{"x": "y"}
	same := nonNilStrMap(existing)
	if same["x"] != "y" {
		t.Error("expected existing map returned")
	}
}

func TestIsValidUTF8(t *testing.T) {
	if !isValidUTF8([]byte("hello world")) {
		t.Error("expected valid UTF-8")
	}
	if isValidUTF8([]byte{0xff, 0xfe}) {
		t.Error("expected invalid UTF-8")
	}
}

func TestEnsureValidUTF8(t *testing.T) {
	valid := ensureValidUTF8([]byte("hello"))
	if string(valid) != "hello" {
		t.Errorf("expected 'hello', got %q", string(valid))
	}

	invalid := ensureValidUTF8([]byte{0xff, 'a', 0xfe})
	if len(invalid) == 0 {
		t.Error("expected non-empty result")
	}
}

func TestCloneMessageShell(t *testing.T) {
	msg := message.New("", []byte("body"))
	msg.Transport = "http"
	msg.ContentType = "json"
	msg.HTTP = &message.HTTPMeta{Method: "POST"}
	msg.Metadata = map[string]any{"key": "val"}

	clone := cloneMessageShell(msg)
	if clone.ID != msg.ID {
		t.Error("ID mismatch")
	}
	if clone.Transport != msg.Transport {
		t.Error("Transport mismatch")
	}
	if clone.HTTP == nil || clone.HTTP.Method != "POST" {
		t.Error("HTTP metadata not cloned")
	}
}

func TestParseIntuResult_NonMap(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	msg := message.New("", []byte("test"))
	result := p.parseIntuResult("raw string", msg)
	if result.Output != "raw string" {
		t.Error("expected raw string output")
	}
}

func TestParseIntuResult_NoBody(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	msg := message.New("", []byte("test"))
	result := p.parseIntuResult(map[string]any{"other": "data"}, msg)
	if result.Output == nil {
		t.Error("expected non-nil output")
	}
}

func TestParseIntuResult_WithBody(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	msg := message.New("", []byte("test"))
	msg.Transport = "http"

	result := p.parseIntuResult(map[string]any{
		"body":        "transformed data",
		"contentType": "json",
		"transport":   "fhir",
		"metadata":    map[string]any{"newKey": "newVal"},
	}, msg)

	if result.Output != "transformed data" {
		t.Error("expected transformed data")
	}
	if result.Msg.ContentType != "json" {
		t.Error("expected contentType json")
	}
	if result.Msg.Transport != "fhir" {
		t.Error("expected transport fhir")
	}
}

func TestParseIntuResult_WithProtocolMeta(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	msg := message.New("", []byte("test"))

	result := p.parseIntuResult(map[string]any{
		"body": "data",
		"http": map[string]any{
			"headers": map[string]any{"X-Test": "1"},
			"method":  "POST",
		},
		"file":          map[string]any{"filename": "out.dat"},
		"ftp":           map[string]any{"filename": "ftp.dat"},
		"kafka":         map[string]any{"topic": "t1"},
		"tcp":           map[string]any{"remoteAddr": "10.0.0.1"},
		"smtp":          map[string]any{"from": "a@b.com"},
		"dicom":         map[string]any{"callingAE": "AE1"},
		"database":      map[string]any{"query": "SELECT 1"},
		"sourceCharset": "iso-8859-1",
	}, msg)

	if result.Msg.HTTP == nil {
		t.Error("expected HTTP metadata")
	}
	if result.Msg.File == nil {
		t.Error("expected File metadata")
	}
	if result.Msg.SourceCharset != "iso-8859-1" {
		t.Error("expected sourceCharset")
	}
}

func TestEngine_SetCoordinator(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)
	engine.SetCoordinator(nil)
}

func TestEngine_SetDeduplicator(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)
	engine.SetDeduplicator(nil)
}

func TestEngine_SetAlertManager(t *testing.T) {
	cfg := minimalConfig()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := NewDefaultEngine(t.TempDir(), cfg, nil, logger)
	engine.SetAlertManager(nil)
}

func TestPipeline_BuildPipelineCtx(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := &Pipeline{
		channelID: "test-ch",
		config:    &config.ChannelConfig{ID: "test-ch"},
		logger:    logger,
		maps:      NewMapVariables(),
	}

	msg := message.New("", []byte("test"))
	msg.Timestamp = time.Now()

	ctx := p.buildPipelineCtx(msg, "transformer")
	if ctx["channelId"] != "test-ch" {
		t.Error("expected channelId")
	}
	if ctx["stage"] != "transformer" {
		t.Error("expected stage")
	}
	if ctx["globalMap"] == nil {
		t.Error("expected globalMap")
	}
}

func TestPipeline_BuildTransformCtx(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := &Pipeline{
		channelID: "test-ch",
		config: &config.ChannelConfig{
			ID: "test-ch",
			DataTypes: &config.DataTypesConfig{
				Inbound:  "hl7v2",
				Outbound: "fhir_r4",
			},
		},
		logger: logger,
	}

	msg := message.New("", []byte("test"))
	ctx := p.buildTransformCtx(msg, "transformer")
	if ctx["inboundDataType"] != "hl7v2" {
		t.Error("expected inboundDataType")
	}
	if ctx["outboundDataType"] != "fhir_r4" {
		t.Error("expected outboundDataType")
	}
}

func TestPipeline_BuildDestCtx(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := &Pipeline{
		channelID: "test-ch",
		config:    &config.ChannelConfig{ID: "test-ch"},
		logger:    logger,
	}

	msg := message.New("", []byte("test"))
	srcMsg := map[string]any{"body": "orig"}
	ctx := p.buildDestCtx(msg, "dest-1", srcMsg, "destination_transformer")
	if ctx["destinationName"] != "dest-1" {
		t.Error("expected destinationName")
	}
	if ctx["sourceMessage"] == nil {
		t.Error("expected sourceMessage")
	}
}

func TestPipeline_GetResolvedDest_FromMap(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{},
		resolvedDests: map[string]config.Destination{
			"my-dest": {Type: "http", HTTP: &config.HTTPDestConfig{URL: "http://test.com"}},
		},
	}

	rd := p.getResolvedDest("my-dest", config.ChannelDestination{})
	if rd.Type != "http" {
		t.Errorf("expected 'http', got %q", rd.Type)
	}
}

func TestPipeline_GetResolvedDest_Fallback(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}

	dest := config.ChannelDestination{Type: "file"}
	rd := p.getResolvedDest("unknown", dest)
	if rd.Type != "file" {
		t.Errorf("expected 'file', got %q", rd.Type)
	}
}
