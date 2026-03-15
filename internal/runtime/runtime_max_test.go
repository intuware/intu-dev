package runtime

import (
	"context"
	"testing"

	"github.com/intuware/intu-dev/internal/connector"
	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/internal/storage"
	"github.com/intuware/intu-dev/pkg/config"
)

// ---------------------------------------------------------------------------
// Mock connector types for testing buildChannelRuntime
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// buildChannelRuntime tests
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// DeployChannel / UndeployChannel / RestartChannel
// ---------------------------------------------------------------------------

func TestDeployChannel_NotFound(t *testing.T) {
	projectDir := t.TempDir()
	cfg := minimalConfig()
	engine := NewDefaultEngine(projectDir, cfg, &mockFactory{}, discardLogger())

	err := engine.DeployChannel(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent channel")
	}
}

func TestDeployChannel_AlreadyRunning(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), &mockFactory{}, discardLogger())
	engine.channels["test-ch"] = &ChannelRuntime{ID: "test-ch"}

	err := engine.DeployChannel(context.Background(), "test-ch")
	if err != nil {
		t.Errorf("expected nil error for already-running channel, got %v", err)
	}
}

func TestUndeployChannel_NotRunning(t *testing.T) {
	engine := NewDefaultEngine("/tmp", minimalConfig(), &mockFactory{}, discardLogger())

	err := engine.UndeployChannel(context.Background(), "nonexistent")
	if err != nil {
		t.Errorf("expected nil for not-running channel, got %v", err)
	}
}

func TestRestartChannel_NotFound(t *testing.T) {
	projectDir := t.TempDir()
	cfg := minimalConfig()
	engine := NewDefaultEngine(projectDir, cfg, &mockFactory{}, discardLogger())

	err := engine.RestartChannel(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent channel restart")
	}
}

// ---------------------------------------------------------------------------
// resolveActiveDestinations
// ---------------------------------------------------------------------------

func TestResolveActiveDestinations_NoRouting(t *testing.T) {
	cr := &ChannelRuntime{
		DestConfigs: []config.ChannelDestination{
			{Name: "d1"},
			{Name: "d2"},
			{Name: "d3"},
		},
	}
	active := cr.resolveActiveDestinations(nil)
	if len(active) != 3 {
		t.Errorf("expected 3, got %d", len(active))
	}
}

func TestResolveActiveDestinations_EmptyRouting(t *testing.T) {
	cr := &ChannelRuntime{
		DestConfigs: []config.ChannelDestination{
			{Name: "d1"},
			{Name: "d2"},
		},
	}
	active := cr.resolveActiveDestinations([]string{})
	if len(active) != 2 {
		t.Errorf("expected 2 (all), got %d", len(active))
	}
}

func TestResolveActiveDestinations_Selective(t *testing.T) {
	cr := &ChannelRuntime{
		DestConfigs: []config.ChannelDestination{
			{Name: "d1"},
			{Name: "d2"},
			{Name: "d3"},
		},
	}
	active := cr.resolveActiveDestinations([]string{"d1", "d3"})
	if len(active) != 2 {
		t.Errorf("expected 2, got %d", len(active))
	}
	names := make(map[string]bool)
	for _, d := range active {
		names[d.Name] = true
	}
	if !names["d1"] || !names["d3"] {
		t.Errorf("expected d1 and d3, got %v", names)
	}
	if names["d2"] {
		t.Error("d2 should not be included")
	}
}

func TestResolveActiveDestinations_RefFallback(t *testing.T) {
	cr := &ChannelRuntime{
		DestConfigs: []config.ChannelDestination{
			{Name: "", Ref: "ref-dest"},
			{Name: "d2"},
		},
	}
	active := cr.resolveActiveDestinations([]string{"ref-dest"})
	if len(active) != 1 {
		t.Errorf("expected 1, got %d", len(active))
	}
}

// ---------------------------------------------------------------------------
// initRetryAndQueue
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Pipeline: ExecutePostprocessor and ExecuteResponseTransformer
// ---------------------------------------------------------------------------

func TestExecutePostprocessor_NoConfig(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config:    &config.ChannelConfig{},
		logger:    discardLogger(),
	}

	err := p.ExecutePostprocessor(context.Background(), message.New("ch1", []byte("data")), "output", nil)
	if err != nil {
		t.Errorf("expected nil for no postprocessor, got %v", err)
	}
}

func TestExecutePostprocessor_NilPipeline(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config:    &config.ChannelConfig{Pipeline: nil},
		logger:    discardLogger(),
	}

	err := p.ExecutePostprocessor(context.Background(), message.New("ch1", []byte("data")), "output", nil)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestExecutePostprocessor_EmptyPostprocessor(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config: &config.ChannelConfig{
			Pipeline: &config.PipelineConfig{Postprocessor: ""},
		},
		logger: discardLogger(),
	}

	err := p.ExecutePostprocessor(context.Background(), message.New("ch1", []byte("data")), "output", nil)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestExecuteResponseTransformer_NoConfig(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config:    &config.ChannelConfig{},
		logger:    discardLogger(),
	}

	dest := config.ChannelDestination{Name: "d1"}
	resp := &message.Response{StatusCode: 200, Body: []byte("ok")}
	err := p.ExecuteResponseTransformer(context.Background(), message.New("ch1", []byte("data")), dest, resp)
	if err != nil {
		t.Errorf("expected nil for no response transformer, got %v", err)
	}
}

func TestExecuteResponseTransformer_NilResponseTransformer(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config:    &config.ChannelConfig{},
		logger:    discardLogger(),
	}

	dest := config.ChannelDestination{
		Name:                "d1",
		ResponseTransformer: nil,
	}
	resp := &message.Response{StatusCode: 200}
	err := p.ExecuteResponseTransformer(context.Background(), message.New("ch1", []byte("data")), dest, resp)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestExecuteResponseTransformer_EmptyEntrypoint(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config:    &config.ChannelConfig{},
		logger:    discardLogger(),
	}

	dest := config.ChannelDestination{
		Name:                "d1",
		ResponseTransformer: &config.ScriptRef{Entrypoint: ""},
	}
	resp := &message.Response{StatusCode: 200}
	err := p.ExecuteResponseTransformer(context.Background(), message.New("ch1", []byte("data")), dest, resp)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Pipeline helpers
// ---------------------------------------------------------------------------

func TestResolveValidator_PipelineConfig(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{
			Pipeline: &config.PipelineConfig{Validator: "validator.ts"},
		},
	}
	if p.resolveValidator() != "validator.ts" {
		t.Errorf("expected 'validator.ts', got %q", p.resolveValidator())
	}
}

func TestResolveValidator_TopLevelConfig(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{
			Validator: &config.ScriptRef{Entrypoint: "top-validator.ts"},
		},
	}
	if p.resolveValidator() != "top-validator.ts" {
		t.Errorf("expected 'top-validator.ts', got %q", p.resolveValidator())
	}
}

func TestResolveValidator_None(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{},
	}
	if p.resolveValidator() != "" {
		t.Errorf("expected empty, got %q", p.resolveValidator())
	}
}

func TestResolveTransformer_PipelineConfig(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{
			Pipeline: &config.PipelineConfig{Transformer: "transformer.ts"},
		},
	}
	if p.resolveTransformer() != "transformer.ts" {
		t.Errorf("expected 'transformer.ts', got %q", p.resolveTransformer())
	}
}

func TestResolveTransformer_TopLevelConfig(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{
			Transformer: &config.ScriptRef{Entrypoint: "top-transformer.ts"},
		},
	}
	if p.resolveTransformer() != "top-transformer.ts" {
		t.Errorf("expected 'top-transformer.ts', got %q", p.resolveTransformer())
	}
}

func TestResolveTransformer_None(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{},
	}
	if p.resolveTransformer() != "" {
		t.Errorf("expected empty, got %q", p.resolveTransformer())
	}
}

func TestResolveDestTransformer_Configured(t *testing.T) {
	dest := config.ChannelDestination{
		Transformer: &config.ScriptRef{Entrypoint: "dest-transform.ts"},
	}
	if resolveDestTransformer(dest) != "dest-transform.ts" {
		t.Errorf("expected 'dest-transform.ts', got %q", resolveDestTransformer(dest))
	}
}

func TestResolveDestTransformer_Nil(t *testing.T) {
	dest := config.ChannelDestination{}
	if resolveDestTransformer(dest) != "" {
		t.Errorf("expected empty, got %q", resolveDestTransformer(dest))
	}
}

func TestResolveDestResponseTransformer_Configured(t *testing.T) {
	dest := config.ChannelDestination{
		ResponseTransformer: &config.ScriptRef{Entrypoint: "resp-transform.ts"},
	}
	if resolveDestResponseTransformer(dest) != "resp-transform.ts" {
		t.Errorf("expected 'resp-transform.ts'")
	}
}

func TestResolveDestResponseTransformer_Nil(t *testing.T) {
	dest := config.ChannelDestination{}
	if resolveDestResponseTransformer(dest) != "" {
		t.Errorf("expected empty")
	}
}

// ---------------------------------------------------------------------------
// Pipeline: resolveDestType
// ---------------------------------------------------------------------------

func TestResolveDestType_Explicit(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	result := p.resolveDestType("d1", config.ChannelDestination{Type: "http"})
	if result != "http" {
		t.Errorf("expected 'http', got %q", result)
	}
}

func TestResolveDestType_FromResolvedDests(t *testing.T) {
	p := &Pipeline{
		config: &config.ChannelConfig{},
		resolvedDests: map[string]config.Destination{
			"d1": {Type: "tcp"},
		},
	}
	result := p.resolveDestType("d1", config.ChannelDestination{})
	if result != "tcp" {
		t.Errorf("expected 'tcp', got %q", result)
	}
}

func TestResolveDestType_InferFromHTTP(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	result := p.resolveDestType("d1", config.ChannelDestination{
		HTTP: &config.HTTPDestConfig{URL: "http://example.com"},
	})
	if result != "http" {
		t.Errorf("expected 'http', got %q", result)
	}
}

func TestResolveDestType_InferFromFile(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	result := p.resolveDestType("d1", config.ChannelDestination{
		File: &config.FileDestConfig{Directory: "/tmp"},
	})
	if result != "file" {
		t.Errorf("expected 'file', got %q", result)
	}
}

func TestResolveDestType_InferFromTCP(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	result := p.resolveDestType("d1", config.ChannelDestination{
		TCP: &config.TCPDestConfig{Host: "localhost"},
	})
	if result != "tcp" {
		t.Errorf("expected 'tcp', got %q", result)
	}
}

func TestResolveDestType_NoMatch(t *testing.T) {
	p := &Pipeline{config: &config.ChannelConfig{}}
	result := p.resolveDestType("d1", config.ChannelDestination{})
	if result != "" {
		t.Errorf("expected empty, got %q", result)
	}
}

// ---------------------------------------------------------------------------
// Pipeline helpers: toBytes, toStringMap, toStringSlice
// ---------------------------------------------------------------------------

func TestToBytes_String(t *testing.T) {
	p := &Pipeline{logger: discardLogger()}
	result := p.toBytes("hello")
	if string(result) != "hello" {
		t.Errorf("expected 'hello', got %q", result)
	}
}

func TestToBytes_ByteSlice(t *testing.T) {
	p := &Pipeline{logger: discardLogger()}
	result := p.toBytes([]byte("data"))
	if string(result) != "data" {
		t.Errorf("expected 'data', got %q", result)
	}
}

func TestToBytes_Map(t *testing.T) {
	p := &Pipeline{logger: discardLogger()}
	result := p.toBytes(map[string]string{"key": "val"})
	if len(result) == 0 {
		t.Error("expected non-empty bytes")
	}
}

func TestNonNilMap_Nil(t *testing.T) {
	result := nonNilMap(nil)
	if result == nil {
		t.Error("expected non-nil map")
	}
	if len(result) != 0 {
		t.Errorf("expected empty map, got %v", result)
	}
}

func TestNonNilMap_NonNil(t *testing.T) {
	input := map[string]string{"a": "b"}
	result := nonNilMap(input)
	if result["a"] != "b" {
		t.Errorf("expected map with a=b")
	}
}

func TestNonNilStrMap_Nil(t *testing.T) {
	result := nonNilStrMap(nil)
	if result == nil {
		t.Error("expected non-nil map")
	}
}

func TestNonNilStrMap_NonNil(t *testing.T) {
	input := map[string]string{"x": "y"}
	result := nonNilStrMap(input)
	if result["x"] != "y" {
		t.Error("expected x=y")
	}
}

func TestToStringSlice_ValidArray(t *testing.T) {
	input := []any{"a", "b", "c"}
	result := toStringSlice(input)
	if len(result) != 3 || result[0] != "a" {
		t.Errorf("unexpected result: %v", result)
	}
}

func TestToStringSlice_NonArray(t *testing.T) {
	result := toStringSlice("not an array")
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestToStringMap_ValidMap(t *testing.T) {
	input := map[string]any{"key": "val", "num": 42}
	result := toStringMap(input)
	if result["key"] != "val" {
		t.Error("expected key=val")
	}
	if result["num"] != "42" {
		t.Errorf("expected num=42, got %q", result["num"])
	}
}

func TestToStringMap_NonMap(t *testing.T) {
	result := toStringMap("not a map")
	if len(result) != 0 {
		t.Errorf("expected empty map, got %v", result)
	}
}

func TestIsValidUTF8Max(t *testing.T) {
	if !isValidUTF8([]byte("hello")) {
		t.Error("expected valid UTF-8")
	}
	if isValidUTF8([]byte{0xff, 0xfe}) {
		t.Error("expected invalid UTF-8")
	}
}

func TestEnsureValidUTF8_Valid(t *testing.T) {
	input := []byte("hello")
	result := ensureValidUTF8(input)
	if string(result) != "hello" {
		t.Errorf("expected 'hello', got %q", result)
	}
}

func TestEnsureValidUTF8_Invalid(t *testing.T) {
	input := []byte{0xff, 0xfe, 0x68, 0x69}
	result := ensureValidUTF8(input)
	if len(result) == 0 {
		t.Error("expected non-empty result")
	}
}
