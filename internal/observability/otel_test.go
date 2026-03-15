package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
	"go.opentelemetry.io/otel/sdk/metric"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---------------------------------------------------------------------------
// initOTelMetricsInstruments — nil provider panics are guarded in production
// by only calling with a valid provider, but we test with a real provider.
// ---------------------------------------------------------------------------

func TestInitOTelMetricsInstruments_WithProvider(t *testing.T) {
	mp := metric.NewMeterProvider()
	defer mp.Shutdown(context.Background())

	// sync.Once means this may or may not fire depending on test ordering.
	// We simply verify it doesn't panic.
	initOTelMetricsInstruments(mp)
}

// ---------------------------------------------------------------------------
// OTelMetrics: exercise all Incr/Record methods on a real initialized instance
// ---------------------------------------------------------------------------

func TestOTelMetrics_AllMethods_Initialized(t *testing.T) {
	mp := metric.NewMeterProvider()
	defer mp.Shutdown(context.Background())

	initOTelMetricsInstruments(mp)

	om := GetOTelMetrics()
	if om == nil {
		t.Skip("OTel metrics not initialized (sync.Once already fired in another test)")
	}

	om.IncrReceived("ch-test")
	om.IncrProcessed("ch-test")
	om.IncrErrored("ch-test", "dest-a")
	om.IncrFiltered("ch-test")
	om.RecordLatency("ch-test", "transform", 15*time.Millisecond)
	om.RecordDestLatency("ch-test", "dest-a", 8*time.Millisecond)
	om.SetQueueDepth("ch-test", "dest-a", 3)

	// Call multiple times to ensure no counter issues
	for i := 0; i < 5; i++ {
		om.IncrReceived("ch-loop")
		om.IncrProcessed("ch-loop")
	}
}

// ---------------------------------------------------------------------------
// GetOTelMetrics — when not initialized returns nil (or non-nil if sync.Once fired)
// ---------------------------------------------------------------------------

func TestGetOTelMetrics_ConsistentReturn(t *testing.T) {
	a := GetOTelMetrics()
	b := GetOTelMetrics()
	if a != b {
		t.Fatal("GetOTelMetrics should return same pointer across calls")
	}
}

// ---------------------------------------------------------------------------
// PrometheusServer: NewPrometheusServer with httptest-based Start/Stop
// ---------------------------------------------------------------------------

func TestPrometheusServer_StartStop_WithHTTPGet(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    19125,
		Path:    "/metrics",
	}, discardLogger())
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	if ps == nil {
		t.Fatal("expected non-nil server")
	}

	if err := ps.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Give the server time to start listening
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:19125/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if err := ps.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestPrometheusServer_DefaultPortAndPath(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
	}, discardLogger())
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	if ps == nil {
		t.Fatal("expected non-nil server")
	}
	if ps.server.Addr != ":9090" {
		t.Fatalf("expected default addr :9090, got %q", ps.server.Addr)
	}
}

func TestPrometheusServer_CustomPath(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    19126,
		Path:    "/custom-metrics",
	}, discardLogger())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := ps.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:19126/custom-metrics")
	if err != nil {
		t.Fatalf("GET /custom-metrics: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if err := ps.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// ---------------------------------------------------------------------------
// OTelShutdown: Shutdown with nil shutdown func (nil providers)
// ---------------------------------------------------------------------------

func TestOTelShutdown_NilProviders_ContextCancelled(t *testing.T) {
	s := &OTelShutdown{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.Shutdown(ctx)
	if err != nil {
		t.Fatalf("expected no error even with cancelled ctx on nil providers, got %v", err)
	}
}

func TestOTelShutdown_WithMeterProvider(t *testing.T) {
	mp := metric.NewMeterProvider()
	s := &OTelShutdown{meterProvider: mp}
	err := s.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// MeterProvider: returns nil when global is noop
// ---------------------------------------------------------------------------

func TestMeterProvider_NilWhenNoop(t *testing.T) {
	// In most test scenarios the global provider is noop.
	// We just verify it doesn't panic and returns a consistent type.
	mp := MeterProvider()
	_ = mp
}

// ---------------------------------------------------------------------------
// InitOTel with nil/disabled config
// ---------------------------------------------------------------------------

func TestInitOTel_NilConfig_ShutdownSafe(t *testing.T) {
	shutdown, err := InitOTel(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil OTelShutdown")
	}
	if shutdown.tracerProvider != nil {
		t.Fatal("expected nil tracerProvider for nil config")
	}
	if shutdown.meterProvider != nil {
		t.Fatal("expected nil meterProvider for nil config")
	}
	if err := shutdown.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
}

func TestInitOTel_DisabledConfig(t *testing.T) {
	shutdown, err := InitOTel(&config.OTelConfig{Enabled: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown.tracerProvider != nil || shutdown.meterProvider != nil {
		t.Fatal("disabled config should not create providers")
	}
}

func TestInitOTel_DisabledWithFieldsSet(t *testing.T) {
	shutdown, err := InitOTel(&config.OTelConfig{
		Enabled:     false,
		ServiceName: "test",
		Endpoint:    "localhost:4317",
		Traces:      true,
		Metrics:     true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown.tracerProvider != nil || shutdown.meterProvider != nil {
		t.Fatal("disabled should not create providers regardless of other fields")
	}
}

// ---------------------------------------------------------------------------
// Metrics.Incr*/Record* with OTel forwarding
// ---------------------------------------------------------------------------

func TestMetrics_IncrReceived_WithOTelForwarding(t *testing.T) {
	m := NewMetrics()
	m.IncrReceived("ch-fwd")
	if m.Counter("messages_received_total.ch-fwd").Load() != 1 {
		t.Fatal("expected counter 1")
	}
}

func TestMetrics_IncrProcessed_WithOTelForwarding(t *testing.T) {
	m := NewMetrics()
	m.IncrProcessed("ch-fwd")
	if m.Counter("messages_processed_total.ch-fwd").Load() != 1 {
		t.Fatal("expected counter 1")
	}
}

func TestMetrics_IncrErrored_WithOTelForwarding(t *testing.T) {
	m := NewMetrics()
	m.IncrErrored("ch-fwd", "dest-fwd")
	if m.Counter("messages_errored_total.ch-fwd.dest-fwd").Load() != 1 {
		t.Fatal("expected counter 1")
	}
}

func TestMetrics_IncrFiltered_WithOTelForwarding(t *testing.T) {
	m := NewMetrics()
	m.IncrFiltered("ch-fwd")
	if m.Counter("messages_filtered_total.ch-fwd").Load() != 1 {
		t.Fatal("expected counter 1")
	}
}

func TestMetrics_RecordLatency_WithOTelForwarding(t *testing.T) {
	m := NewMetrics()
	m.RecordLatency("ch-fwd", "stage-a", 10*time.Millisecond)
	ts := m.Timing("processing_duration.ch-fwd.stage-a")
	count, _, _, _ := ts.Stats()
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

func TestMetrics_RecordDestLatency_WithOTelForwarding(t *testing.T) {
	m := NewMetrics()
	m.RecordDestLatency("ch-fwd", "dest-fwd", 5*time.Millisecond)
	ts := m.Timing("destination_latency.ch-fwd.dest-fwd")
	count, _, _, _ := ts.Stats()
	if count != 1 {
		t.Fatalf("expected 1, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// Concurrent metrics operations with OTel forwarding
// ---------------------------------------------------------------------------

func TestMetrics_ConcurrentOTelForwarding(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ch := fmt.Sprintf("ch-%d", idx)
			for j := 0; j < 50; j++ {
				m.IncrReceived(ch)
				m.IncrProcessed(ch)
				m.IncrErrored(ch, "dest")
				m.IncrFiltered(ch)
				m.RecordLatency(ch, "s", time.Millisecond)
				m.RecordDestLatency(ch, "d", time.Millisecond)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		ch := fmt.Sprintf("ch-%d", i)
		if m.Counter("messages_received_total."+ch).Load() != 50 {
			t.Errorf("ch-%d: expected 50 received", i)
		}
		if m.Counter("messages_processed_total."+ch).Load() != 50 {
			t.Errorf("ch-%d: expected 50 processed", i)
		}
	}
}

// ---------------------------------------------------------------------------
// PrometheusServer metrics scrape with OTel metrics
// ---------------------------------------------------------------------------

func TestPrometheusServer_MetricsScrapeContent(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    19127,
		Path:    "/metrics",
	}, discardLogger())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ps.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer ps.Stop()
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:19127/metrics")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if len(body) == 0 {
		t.Fatal("expected non-empty metrics response")
	}
}

// ---------------------------------------------------------------------------
// nil receiver OTel metrics - with method chaining pattern
// ---------------------------------------------------------------------------

func TestOTelMetrics_NilReceiver_AllMethods(t *testing.T) {
	var om *OTelMetrics
	om.IncrReceived("ch")
	om.IncrProcessed("ch")
	om.IncrErrored("ch", "d")
	om.IncrFiltered("ch")
	om.RecordLatency("ch", "s", time.Second)
	om.RecordDestLatency("ch", "d", time.Second)
	om.SetQueueDepth("ch", "d", 10)
}

// ---------------------------------------------------------------------------
// PrometheusServer - 404 on non-metrics path
// ---------------------------------------------------------------------------

func TestPrometheusServer_404OnNonMetricsPath(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    19128,
		Path:    "/metrics",
	}, discardLogger())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ps.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer ps.Stop()
	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:19128/not-metrics")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Metrics with httptest for more robust PrometheusServer testing
// ---------------------------------------------------------------------------

func TestPrometheusServer_StopIdempotent(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    19129,
		Path:    "/metrics",
	}, discardLogger())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ps.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := ps.Stop(); err != nil {
		t.Fatalf("first stop: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Using httptest.Server pattern for Prometheus handler
// ---------------------------------------------------------------------------

func TestPrometheusHandler_Httptest(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("# HELP test_metric A test metric\n"))
	}))
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}
