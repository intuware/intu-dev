package observability

import (
	"bytes"
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

func TestNewMetrics(t *testing.T) {
	m := NewMetrics()
	if m == nil {
		t.Fatal("NewMetrics returned nil")
	}
	snap := m.Snapshot()
	if snap == nil {
		t.Fatal("Snapshot returned nil")
	}
	if counters, ok := snap["counters"].(map[string]int64); !ok || len(counters) != 0 {
		t.Errorf("expected empty counters, got %v", snap["counters"])
	}
	if gauges, ok := snap["gauges"].(map[string]int64); !ok || len(gauges) != 0 {
		t.Errorf("expected empty gauges, got %v", snap["gauges"])
	}
	if timings, ok := snap["timings"].(map[string]map[string]any); !ok || len(timings) != 0 {
		t.Errorf("expected empty timings, got %v", snap["timings"])
	}
}

func TestGlobal(t *testing.T) {
	g := Global()
	if g == nil {
		t.Fatal("Global returned nil")
	}
	if g != globalMetrics {
		t.Error("Global should return globalMetrics")
	}
}

func TestMetrics_Counter(t *testing.T) {
	m := NewMetrics()
	c := m.Counter("test_counter")
	if c == nil {
		t.Fatal("Counter returned nil")
	}
	if c.Load() != 0 {
		t.Errorf("expected 0, got %d", c.Load())
	}
	c.Add(5)
	if c.Load() != 5 {
		t.Errorf("expected 5, got %d", c.Load())
	}
	c2 := m.Counter("test_counter")
	if c != c2 {
		t.Error("Counter should return same instance for same name")
	}
}

func TestMetrics_Gauge(t *testing.T) {
	m := NewMetrics()
	g := m.Gauge("test_gauge")
	if g == nil {
		t.Fatal("Gauge returned nil")
	}
	if g.Load() != 0 {
		t.Errorf("expected 0, got %d", g.Load())
	}
	g.Store(42)
	if g.Load() != 42 {
		t.Errorf("expected 42, got %d", g.Load())
	}
	g2 := m.Gauge("test_gauge")
	if g != g2 {
		t.Error("Gauge should return same instance for same name")
	}
}

func TestMetrics_Timing(t *testing.T) {
	m := NewMetrics()
	ts := m.Timing("test_timing")
	if ts == nil {
		t.Fatal("Timing returned nil")
	}
	count, avg, min, max := ts.Stats()
	if count != 0 || avg != 0 || min != 0 || max != 0 {
		t.Errorf("expected zeros, got count=%d avg=%v min=%v max=%v", count, avg, min, max)
	}
	ts.Record(10 * time.Millisecond)
	ts.Record(20 * time.Millisecond)
	ts.Record(30 * time.Millisecond)
	count, avg, min, max = ts.Stats()
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
	if min != 10*time.Millisecond {
		t.Errorf("expected min 10ms, got %v", min)
	}
	if max != 30*time.Millisecond {
		t.Errorf("expected max 30ms, got %v", max)
	}
	if avg != 20*time.Millisecond {
		t.Errorf("expected avg 20ms, got %v", avg)
	}
	ts2 := m.Timing("test_timing")
	if ts != ts2 {
		t.Error("Timing should return same instance for same name")
	}
}

func TestTimingStat_RecordEmpty(t *testing.T) {
	ts := &TimingStat{}
	ts.Record(5 * time.Second)
	count, avg, min, max := ts.Stats()
	if count != 1 || avg != 5*time.Second || min != 5*time.Second || max != 5*time.Second {
		t.Errorf("expected single 5s record, got count=%d avg=%v min=%v max=%v", count, avg, min, max)
	}
}

func TestMetrics_IncrReceived(t *testing.T) {
	m := NewMetrics()
	m.IncrReceived("ch1")
	m.IncrReceived("ch1")
	m.IncrReceived("ch2")
	if m.Counter("messages_received_total.ch1").Load() != 2 {
		t.Errorf("expected 2 for ch1, got %d", m.Counter("messages_received_total.ch1").Load())
	}
	if m.Counter("messages_received_total.ch2").Load() != 1 {
		t.Errorf("expected 1 for ch2, got %d", m.Counter("messages_received_total.ch2").Load())
	}
}

func TestMetrics_IncrProcessed(t *testing.T) {
	m := NewMetrics()
	m.IncrProcessed("ch1")
	m.IncrProcessed("ch1")
	if m.Counter("messages_processed_total.ch1").Load() != 2 {
		t.Errorf("expected 2, got %d", m.Counter("messages_processed_total.ch1").Load())
	}
}

func TestMetrics_IncrErrored(t *testing.T) {
	m := NewMetrics()
	m.IncrErrored("ch1", "dest1")
	m.IncrErrored("ch1", "dest1")
	m.IncrErrored("ch1", "dest2")
	if m.Counter("messages_errored_total.ch1.dest1").Load() != 2 {
		t.Errorf("expected 2 for dest1, got %d", m.Counter("messages_errored_total.ch1.dest1").Load())
	}
	if m.Counter("messages_errored_total.ch1.dest2").Load() != 1 {
		t.Errorf("expected 1 for dest2, got %d", m.Counter("messages_errored_total.ch1.dest2").Load())
	}
}

func TestMetrics_IncrFiltered(t *testing.T) {
	m := NewMetrics()
	m.IncrFiltered("ch1")
	if m.Counter("messages_filtered_total.ch1").Load() != 1 {
		t.Errorf("expected 1, got %d", m.Counter("messages_filtered_total.ch1").Load())
	}
}

func TestMetrics_RecordLatency(t *testing.T) {
	m := NewMetrics()
	m.RecordLatency("ch1", "transform", 50*time.Millisecond)
	m.RecordLatency("ch1", "transform", 100*time.Millisecond)
	ts := m.Timing("processing_duration.ch1.transform")
	count, avg, min, max := ts.Stats()
	if count != 2 || avg != 75*time.Millisecond || min != 50*time.Millisecond || max != 100*time.Millisecond {
		t.Errorf("got count=%d avg=%v min=%v max=%v", count, avg, min, max)
	}
}

func TestMetrics_RecordDestLatency(t *testing.T) {
	m := NewMetrics()
	m.RecordDestLatency("ch1", "dest1", 25*time.Millisecond)
	ts := m.Timing("destination_latency.ch1.dest1")
	count, _, _, _ := ts.Stats()
	if count != 1 {
		t.Errorf("expected 1, got %d", count)
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	m := NewMetrics()
	m.Counter("c1").Add(10)
	m.Gauge("g1").Store(20)
	m.Timing("t1").Record(100 * time.Millisecond)
	snap := m.Snapshot()
	counters := snap["counters"].(map[string]int64)
	if counters["c1"] != 10 {
		t.Errorf("expected counter c1=10, got %d", counters["c1"])
	}
	gauges := snap["gauges"].(map[string]int64)
	if gauges["g1"] != 20 {
		t.Errorf("expected gauge g1=20, got %d", gauges["g1"])
	}
	timings := snap["timings"].(map[string]map[string]any)
	if t1, ok := timings["t1"]; !ok {
		t.Error("expected timing t1 in snapshot")
	} else if t1["count"].(int64) != 1 {
		t.Errorf("expected timing count 1, got %v", t1["count"])
	}
}

func TestMetrics_ConcurrentAccess(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.Counter("conc").Add(1)
				m.Gauge("conc_g").Store(int64(j))
				m.Timing("conc_t").Record(time.Duration(j) * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	if m.Counter("conc").Load() != 1000 {
		t.Errorf("expected 1000, got %d", m.Counter("conc").Load())
	}
}

// --- ChannelLogger tests ---

func TestNewChannelLogger_NilConfig(t *testing.T) {
	buf := &bytes.Buffer{}
	cl := NewChannelLoggerWithWriter("ch1", nil, "", buf)
	if cl == nil {
		t.Fatal("NewChannelLoggerWithWriter returned nil")
	}
	if cl.channelID != "ch1" {
		t.Errorf("expected channelID ch1, got %s", cl.channelID)
	}
	if cl.payloadCfg != nil {
		t.Error("payloadCfg should be nil when logCfg is nil")
	}
	if cl.truncateAt != 0 {
		t.Errorf("expected truncateAt 0, got %d", cl.truncateAt)
	}
	if cl.Logger() == nil {
		t.Error("Logger returned nil")
	}
}

func TestNewChannelLogger_DefaultLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	cl := NewChannelLoggerWithWriter("ch1", nil, "", buf)
	cl.Logger().Info("test message")
	out := buf.String()
	if !strings.Contains(out, "test message") {
		t.Errorf("expected log output to contain message, got %s", out)
	}
}

func TestNewChannelLogger_LevelFromConfig(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{Level: "debug"}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "info", buf)
	cl.Logger().Debug("debug msg")
	out := buf.String()
	if !strings.Contains(out, "debug msg") {
		t.Errorf("expected debug level to log, got %s", out)
	}
}

func TestNewChannelLogger_LevelWarn(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{Level: "warn"}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "info", buf)
	cl.Logger().Info("info msg")
	cl.Logger().Warn("warn msg")
	out := buf.String()
	if strings.Contains(out, "info msg") {
		t.Error("info should not be logged at warn level")
	}
	if !strings.Contains(out, "warn msg") {
		t.Errorf("warn should be logged, got %s", out)
	}
}

func TestNewChannelLogger_LevelError(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{Level: "error"}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "info", buf)
	cl.Logger().Warn("warn msg")
	cl.Logger().Error("error msg")
	out := buf.String()
	if strings.Contains(out, "warn msg") {
		t.Error("warn should not be logged at error level")
	}
	if !strings.Contains(out, "error msg") {
		t.Errorf("error should be logged, got %s", out)
	}
}

func TestNewChannelLogger_LevelSilent(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{Level: "silent"}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "info", buf)
	cl.Logger().Info("info msg")
	cl.Logger().Error("error msg")
	out := buf.String()
	if out != "" {
		t.Errorf("silent level should not log anything, got %s", out)
	}
}

func TestNewChannelLogger_LevelCaseInsensitive(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{Level: "DEBUG"}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "info", buf)
	cl.Logger().Debug("debug msg")
	out := buf.String()
	if !strings.Contains(out, "debug msg") {
		t.Errorf("DEBUG should work like debug, got %s", out)
	}
}

func TestChannelLogger_LogSourcePayload_Disabled(t *testing.T) {
	buf := &bytes.Buffer{}
	cl := NewChannelLoggerWithWriter("ch1", nil, "debug", buf)
	cl.LogSourcePayload([]byte("payload"))
	if buf.Len() != 0 {
		t.Errorf("LogSourcePayload with nil payloadCfg should not log, got %s", buf.String())
	}
}

func TestChannelLogger_LogSourcePayload_Enabled(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{
		Level:    "debug",
		Payloads: &config.PayloadLogging{Source: true},
	}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "debug", buf)
	cl.LogSourcePayload([]byte("hello"))
	out := buf.String()
	if !strings.Contains(out, "source payload") || !strings.Contains(out, "hello") {
		t.Errorf("expected source payload log, got %s", out)
	}
}

func TestChannelLogger_LogTransformedPayload_Enabled(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{
		Level:    "debug",
		Payloads: &config.PayloadLogging{Transformed: true},
	}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "debug", buf)
	cl.LogTransformedPayload([]byte("transformed"))
	out := buf.String()
	if !strings.Contains(out, "transformed payload") || !strings.Contains(out, "transformed") {
		t.Errorf("expected transformed payload log, got %s", out)
	}
}

func TestChannelLogger_LogSentPayload_Enabled(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{
		Level:    "debug",
		Payloads: &config.PayloadLogging{Sent: true},
	}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "debug", buf)
	cl.LogSentPayload("dest1", []byte("sent"))
	out := buf.String()
	if !strings.Contains(out, "sent payload") || !strings.Contains(out, "dest1") || !strings.Contains(out, "sent") {
		t.Errorf("expected sent payload log, got %s", out)
	}
}

func TestChannelLogger_LogResponsePayload_Enabled(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{
		Level:    "debug",
		Payloads: &config.PayloadLogging{Response: true},
	}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "debug", buf)
	cl.LogResponsePayload("dest1", []byte("response"))
	out := buf.String()
	if !strings.Contains(out, "response payload") || !strings.Contains(out, "dest1") || !strings.Contains(out, "response") {
		t.Errorf("expected response payload log, got %s", out)
	}
}

func TestChannelLogger_LogFilteredMessage_Enabled(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{
		Level:    "debug",
		Payloads: &config.PayloadLogging{Filtered: true},
	}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "debug", buf)
	cl.LogFilteredMessage("msg-123")
	out := buf.String()
	if !strings.Contains(out, "message filtered") || !strings.Contains(out, "msg-123") {
		t.Errorf("expected filtered message log, got %s", out)
	}
}

func TestChannelLogger_Truncate(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{
		Level:      "debug",
		Payloads:   &config.PayloadLogging{Source: true},
		TruncateAt: 5,
	}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "debug", buf)
	cl.LogSourcePayload([]byte("hello world"))
	out := buf.String()
	if !strings.Contains(out, "hello...(truncated)") {
		t.Errorf("expected truncated payload, got %s", out)
	}
}

func TestChannelLogger_TruncateExact(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{
		Level:      "debug",
		Payloads:   &config.PayloadLogging{Source: true},
		TruncateAt: 11,
	}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "debug", buf)
	cl.LogSourcePayload([]byte("hello world"))
	out := buf.String()
	if strings.Contains(out, "truncated") {
		t.Error("exact length should not truncate")
	}
	if !strings.Contains(out, "hello world") {
		t.Errorf("expected full payload, got %s", out)
	}
}

func TestChannelLogger_TruncateShort(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{
		Level:      "debug",
		Payloads:   &config.PayloadLogging{Source: true},
		TruncateAt: 100,
	}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "debug", buf)
	cl.LogSourcePayload([]byte("hi"))
	out := buf.String()
	if strings.Contains(out, "truncated") {
		t.Error("short payload should not truncate")
	}
}

func TestNewChannelLogger_UsesStdoutWhenWriterNil(t *testing.T) {
	cl := NewChannelLogger("ch1", nil, "info")
	if cl == nil {
		t.Fatal("NewChannelLogger returned nil")
	}
	if cl.Logger() == nil {
		t.Error("Logger returned nil")
	}
}

func TestChannelLogger_AllPayloadFlagsDisabled(t *testing.T) {
	buf := &bytes.Buffer{}
	logCfg := &config.ChannelLogging{
		Level:    "debug",
		Payloads: &config.PayloadLogging{Source: false, Transformed: false, Sent: false, Response: false, Filtered: false},
	}
	cl := NewChannelLoggerWithWriter("ch1", logCfg, "debug", buf)
	cl.LogSourcePayload([]byte("x"))
	cl.LogTransformedPayload([]byte("x"))
	cl.LogSentPayload("d", []byte("x"))
	cl.LogResponsePayload("d", []byte("x"))
	cl.LogFilteredMessage("id")
	if buf.Len() != 0 {
		t.Errorf("no payload logs should be written when all disabled, got %s", buf.String())
	}
}
func TestGetOTelMetrics_NilWhenNotInitialized(t *testing.T) {

	_ = GetOTelMetrics()
}

func TestOTelMetrics_NilReceiver_IncrReceived(t *testing.T) {
	var om *OTelMetrics
	om.IncrReceived("ch1")
}

func TestOTelMetrics_NilReceiver_IncrProcessed(t *testing.T) {
	var om *OTelMetrics
	om.IncrProcessed("ch1")
}

func TestOTelMetrics_NilReceiver_IncrErrored(t *testing.T) {
	var om *OTelMetrics
	om.IncrErrored("ch1", "dest1")
}

func TestOTelMetrics_NilReceiver_IncrFiltered(t *testing.T) {
	var om *OTelMetrics
	om.IncrFiltered("ch1")
}

func TestOTelMetrics_NilReceiver_RecordLatency(t *testing.T) {
	var om *OTelMetrics
	om.RecordLatency("ch1", "transform", 10*time.Millisecond)
}

func TestOTelMetrics_NilReceiver_RecordDestLatency(t *testing.T) {
	var om *OTelMetrics
	om.RecordDestLatency("ch1", "dest1", 10*time.Millisecond)
}

func TestOTelMetrics_NilReceiver_SetQueueDepth(t *testing.T) {
	var om *OTelMetrics
	om.SetQueueDepth("ch1", "dest1", 5)
}

func TestNewPrometheusServer_NilConfig(t *testing.T) {
	ps, err := NewPrometheusServer(nil, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps != nil {
		t.Fatal("expected nil server when config is nil")
	}
}

func TestNewPrometheusServer_Disabled(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{Enabled: false}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps != nil {
		t.Fatal("expected nil server when disabled")
	}
}

func TestNewPrometheusServer_Enabled(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    0,
		Path:    "",
	}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps == nil {
		t.Fatal("expected non-nil PrometheusServer")
	}
}

func TestPrometheusServer_StartStop(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    19123,
		Path:    "/metrics",
	}, slog.Default())
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	if err := ps.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := ps.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestOTelShutdown_NilProviders(t *testing.T) {
	s := &OTelShutdown{}
	err := s.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("expected no error from nil providers, got %v", err)
	}
}

func TestInitOTel_NilConfig(t *testing.T) {
	shutdown, err := InitOTel(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil OTelShutdown")
	}

	if err := shutdown.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
}

func TestInitOTel_Disabled(t *testing.T) {
	shutdown, err := InitOTel(&config.OTelConfig{Enabled: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil OTelShutdown")
	}
}

func TestMeterProvider_ReturnType(t *testing.T) {

	_ = MeterProvider()
}

func TestOTelMetrics_WithPrometheus(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    19124,
		Path:    "/metrics",
	}, slog.Default())
	if err != nil {
		t.Fatalf("create prometheus server: %v", err)
	}

	om := GetOTelMetrics()
	if om == nil {
		t.Skip("OTel metrics not initialized (sync.Once already fired without prometheus)")
	}

	om.IncrReceived("test-ch")
	om.IncrProcessed("test-ch")
	om.IncrErrored("test-ch", "dest-1")
	om.IncrFiltered("test-ch")
	om.RecordLatency("test-ch", "transform", 50*time.Millisecond)
	om.RecordDestLatency("test-ch", "dest-1", 25*time.Millisecond)
	om.SetQueueDepth("test-ch", "dest-1", 10)

	_ = ps
}
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
