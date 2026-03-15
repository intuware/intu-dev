package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
	"go.opentelemetry.io/otel/sdk/metric"
)

// ===================================================================
// PrometheusServer — Start/Stop
// ===================================================================

func TestPush_PrometheusServer_StartStop(t *testing.T) {
	cfg := &config.PrometheusConfig{Enabled: true, Port: 0, Path: "/metrics"}

	ps, err := NewPrometheusServer(cfg, testPushLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if ps == nil {
		t.Fatal("expected non-nil server")
	}

	if err := ps.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := ps.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestPush_PrometheusServer_NilConfig(t *testing.T) {
	ps, err := NewPrometheusServer(nil, testPushLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps != nil {
		t.Fatal("expected nil for nil config")
	}
}

func TestPush_PrometheusServer_DisabledConfig(t *testing.T) {
	cfg := &config.PrometheusConfig{Enabled: false}
	ps, err := NewPrometheusServer(cfg, testPushLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps != nil {
		t.Fatal("expected nil for disabled config")
	}
}

func TestPush_PrometheusServer_DefaultPortAndPath(t *testing.T) {
	cfg := &config.PrometheusConfig{Enabled: true}
	ps, err := NewPrometheusServer(cfg, testPushLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if ps == nil {
		t.Fatal("expected non-nil")
	}
	if ps.server.Addr != ":9090" {
		t.Fatalf("expected :9090, got %s", ps.server.Addr)
	}
}

func TestPush_PrometheusServer_CustomPath(t *testing.T) {
	cfg := &config.PrometheusConfig{Enabled: true, Port: 0, Path: "/custom-metrics"}
	ps, err := NewPrometheusServer(cfg, testPushLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if ps == nil {
		t.Fatal("expected non-nil")
	}
}

func TestPush_PrometheusServer_ScrapeContent(t *testing.T) {
	cfg := &config.PrometheusConfig{Enabled: true, Port: 19091, Path: "/metrics"}
	ps, err := NewPrometheusServer(cfg, testPushLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := ps.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ps.Stop()

	time.Sleep(100 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:19091/metrics")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "go_") {
		t.Fatal("expected Go metrics in output")
	}
}

// ===================================================================
// OTel — init and shutdown
// ===================================================================

func TestPush_InitOTel_NilConfig(t *testing.T) {
	shutdown, err := InitOTel(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown")
	}
	if err := shutdown.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestPush_InitOTel_DisabledConfig(t *testing.T) {
	cfg := &config.OTelConfig{Enabled: false}
	shutdown, err := InitOTel(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := shutdown.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestPush_OTelShutdown_NilProviders(t *testing.T) {
	s := &OTelShutdown{}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestPush_OTelShutdown_WithMeterProvider(t *testing.T) {
	mp := metric.NewMeterProvider()
	s := &OTelShutdown{meterProvider: mp}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestPush_MeterProvider_NilWhenNoop(t *testing.T) {
	mp := MeterProvider()
	_ = mp
}

// ===================================================================
// OTelMetrics — nil receiver safety
// ===================================================================

func TestPush_OTelMetrics_NilReceiver_AllMethods(t *testing.T) {
	var om *OTelMetrics
	om.IncrReceived("ch1")
	om.IncrProcessed("ch1")
	om.IncrErrored("ch1", "d1")
	om.IncrFiltered("ch1")
	om.RecordLatency("ch1", "total", 100*time.Millisecond)
	om.RecordDestLatency("ch1", "d1", 50*time.Millisecond)
	om.SetQueueDepth("ch1", "d1", 5)
}

// ===================================================================
// Metrics — comprehensive API coverage
// ===================================================================

func TestPush_Metrics_AllMethods(t *testing.T) {
	m := NewMetrics()

	m.IncrReceived("ch1")
	m.IncrReceived("ch1")
	m.IncrProcessed("ch1")
	m.IncrErrored("ch1", "d1")
	m.IncrFiltered("ch1")
	m.RecordLatency("ch1", "total", 100*time.Millisecond)
	m.RecordDestLatency("ch1", "d1", 50*time.Millisecond)

	snap := m.Snapshot()
	counters := snap["counters"].(map[string]int64)
	if counters["messages_received_total.ch1"] != 2 {
		t.Fatalf("expected 2 received, got %d", counters["messages_received_total.ch1"])
	}
	if counters["messages_processed_total.ch1"] != 1 {
		t.Fatalf("expected 1 processed, got %d", counters["messages_processed_total.ch1"])
	}
	if counters["messages_errored_total.ch1.d1"] != 1 {
		t.Fatalf("expected 1 errored, got %d", counters["messages_errored_total.ch1.d1"])
	}
	if counters["messages_filtered_total.ch1"] != 1 {
		t.Fatalf("expected 1 filtered, got %d", counters["messages_filtered_total.ch1"])
	}

	timings := snap["timings"].(map[string]map[string]any)
	if timings["processing_duration.ch1.total"]["count"].(int64) != 1 {
		t.Fatal("expected 1 timing record")
	}
}

func TestPush_Metrics_Gauge(t *testing.T) {
	m := NewMetrics()
	g := m.Gauge("queue_depth")
	g.Store(10)
	if g.Load() != 10 {
		t.Fatalf("expected 10, got %d", g.Load())
	}
	g2 := m.Gauge("queue_depth")
	if g2.Load() != 10 {
		t.Fatal("expected same gauge instance")
	}
}

func TestPush_Metrics_Timing_Record(t *testing.T) {
	m := NewMetrics()
	ts := m.Timing("test-timing")
	ts.Record(10 * time.Millisecond)
	ts.Record(20 * time.Millisecond)
	ts.Record(5 * time.Millisecond)

	count, avg, min, max := ts.Stats()
	if count != 3 {
		t.Fatalf("expected 3, got %d", count)
	}
	if min != 5*time.Millisecond {
		t.Fatalf("expected min=5ms, got %v", min)
	}
	if max != 20*time.Millisecond {
		t.Fatalf("expected max=20ms, got %v", max)
	}
	_ = avg
}

func TestPush_Metrics_Timing_Empty(t *testing.T) {
	ts := &TimingStat{}
	count, avg, min, max := ts.Stats()
	if count != 0 || avg != 0 || min != 0 || max != 0 {
		t.Fatal("expected all zeros for empty timing")
	}
}

func TestPush_Metrics_ConcurrentAccess(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ch := fmt.Sprintf("ch-%d", i%5)
			m.IncrReceived(ch)
			m.IncrProcessed(ch)
			m.IncrErrored(ch, "d1")
			m.IncrFiltered(ch)
			m.RecordLatency(ch, "total", time.Duration(i)*time.Millisecond)
			m.RecordDestLatency(ch, "d1", time.Duration(i)*time.Millisecond)
		}(i)
	}
	wg.Wait()

	snap := m.Snapshot()
	counters := snap["counters"].(map[string]int64)
	var total int64
	for k, v := range counters {
		if strings.HasPrefix(k, "messages_received_total.") {
			total += v
		}
	}
	if total != 50 {
		t.Fatalf("expected 50 total received, got %d", total)
	}
}

func TestPush_Global(t *testing.T) {
	g := Global()
	if g == nil {
		t.Fatal("expected non-nil global")
	}
}

// ===================================================================
// ChannelLogger — various levels and options
// ===================================================================

func TestPush_ChannelLogger_NilConfig(t *testing.T) {
	cl := NewChannelLogger("ch1", nil, "info")
	if cl == nil {
		t.Fatal("expected non-nil")
	}
}

func TestPush_ChannelLogger_DefaultLevel(t *testing.T) {
	cl := NewChannelLogger("ch1", nil, "")
	if cl == nil {
		t.Fatal("expected non-nil")
	}
}

func TestPush_ChannelLogger_DebugLevel(t *testing.T) {
	logCfg := &config.ChannelLogging{Level: "debug"}
	cl := NewChannelLogger("ch1", logCfg, "info")
	if cl == nil {
		t.Fatal("expected non-nil")
	}
}

func TestPush_ChannelLogger_WarnLevel(t *testing.T) {
	logCfg := &config.ChannelLogging{Level: "warn"}
	cl := NewChannelLogger("ch1", logCfg, "info")
	if cl == nil {
		t.Fatal("expected non-nil")
	}
}

func TestPush_ChannelLogger_ErrorLevel(t *testing.T) {
	logCfg := &config.ChannelLogging{Level: "error"}
	cl := NewChannelLogger("ch1", logCfg, "info")
	if cl == nil {
		t.Fatal("expected non-nil")
	}
}

func TestPush_ChannelLogger_SilentLevel(t *testing.T) {
	logCfg := &config.ChannelLogging{Level: "silent"}
	cl := NewChannelLogger("ch1", logCfg, "info")
	if cl == nil {
		t.Fatal("expected non-nil")
	}
}

func TestPush_ChannelLogger_WithWriter(t *testing.T) {
	cl := NewChannelLoggerWithWriter("ch1", nil, "info", io.Discard)
	if cl == nil {
		t.Fatal("expected non-nil")
	}
}

func testPushLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
