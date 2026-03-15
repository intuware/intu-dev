package observability

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

func TestPrometheusServer_NewAndStartStop(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    0,
		Path:    "/metrics",
	}, logger)
	if err != nil {
		t.Fatalf("NewPrometheusServer: %v", err)
	}
	if ps == nil {
		t.Fatal("expected non-nil PrometheusServer")
	}

	if err := ps.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := ps.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestPrometheusServer_Disabled(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ps, err := NewPrometheusServer(&config.PrometheusConfig{Enabled: false}, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps != nil {
		t.Fatal("expected nil for disabled Prometheus")
	}
}

func TestPrometheusServer_NilConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ps, err := NewPrometheusServer(nil, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps != nil {
		t.Fatal("expected nil for nil config")
	}
}

func TestPrometheusServer_DefaultPortAndPath(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
	}, logger)
	if err != nil {
		t.Fatalf("NewPrometheusServer: %v", err)
	}
	if ps == nil {
		t.Fatal("expected non-nil PrometheusServer with defaults")
	}
	if ps.server.Addr != ":9090" {
		t.Errorf("expected default port :9090, got %q", ps.server.Addr)
	}
}

func TestMetrics_CounterAndGauge(t *testing.T) {
	m := NewMetrics()

	c := m.Counter("test_counter")
	c.Add(5)
	if c.Load() != 5 {
		t.Errorf("expected 5, got %d", c.Load())
	}

	c2 := m.Counter("test_counter")
	if c2.Load() != 5 {
		t.Error("expected same counter returned")
	}

	g := m.Gauge("test_gauge")
	g.Store(42)
	if g.Load() != 42 {
		t.Errorf("expected 42, got %d", g.Load())
	}

	g2 := m.Gauge("test_gauge")
	if g2.Load() != 42 {
		t.Error("expected same gauge returned")
	}
}

func TestMetrics_Timing(t *testing.T) {
	m := NewMetrics()

	ts := m.Timing("test_timing")
	ts.Record(10 * time.Millisecond)
	ts.Record(20 * time.Millisecond)
	ts.Record(30 * time.Millisecond)

	count, avg, min, max := ts.Stats()
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
	if avg != 20*time.Millisecond {
		t.Errorf("expected avg 20ms, got %v", avg)
	}
	if min != 10*time.Millisecond {
		t.Errorf("expected min 10ms, got %v", min)
	}
	if max != 30*time.Millisecond {
		t.Errorf("expected max 30ms, got %v", max)
	}

	ts2 := m.Timing("test_timing")
	count2, _, _, _ := ts2.Stats()
	if count2 != 3 {
		t.Error("expected same timing returned")
	}
}

func TestTimingStat_Empty(t *testing.T) {
	ts := &TimingStat{}
	count, avg, min, max := ts.Stats()
	if count != 0 || avg != 0 || min != 0 || max != 0 {
		t.Error("expected all zeros for empty timing")
	}
}

func TestMetrics_IncrReceived(t *testing.T) {
	m := NewMetrics()
	m.IncrReceived("ch1")
	m.IncrReceived("ch1")
	m.IncrReceived("ch2")

	snap := m.Snapshot()
	counters := snap["counters"].(map[string]int64)
	if counters["messages_received_total.ch1"] != 2 {
		t.Errorf("expected 2, got %d", counters["messages_received_total.ch1"])
	}
	if counters["messages_received_total.ch2"] != 1 {
		t.Errorf("expected 1, got %d", counters["messages_received_total.ch2"])
	}
}

func TestMetrics_IncrProcessed(t *testing.T) {
	m := NewMetrics()
	m.IncrProcessed("ch1")
	snap := m.Snapshot()
	counters := snap["counters"].(map[string]int64)
	if counters["messages_processed_total.ch1"] != 1 {
		t.Errorf("expected 1, got %d", counters["messages_processed_total.ch1"])
	}
}

func TestMetrics_IncrErrored(t *testing.T) {
	m := NewMetrics()
	m.IncrErrored("ch1", "dest1")
	snap := m.Snapshot()
	counters := snap["counters"].(map[string]int64)
	if counters["messages_errored_total.ch1.dest1"] != 1 {
		t.Errorf("expected 1, got %d", counters["messages_errored_total.ch1.dest1"])
	}
}

func TestMetrics_IncrFiltered(t *testing.T) {
	m := NewMetrics()
	m.IncrFiltered("ch1")
	snap := m.Snapshot()
	counters := snap["counters"].(map[string]int64)
	if counters["messages_filtered_total.ch1"] != 1 {
		t.Errorf("expected 1, got %d", counters["messages_filtered_total.ch1"])
	}
}

func TestMetrics_RecordLatency(t *testing.T) {
	m := NewMetrics()
	m.RecordLatency("ch1", "total", 10*time.Millisecond)
	m.RecordLatency("ch1", "total", 20*time.Millisecond)

	snap := m.Snapshot()
	timings := snap["timings"].(map[string]map[string]any)
	if timings["processing_duration.ch1.total"]["count"].(int64) != 2 {
		t.Error("expected 2 timing entries")
	}
}

func TestMetrics_RecordDestLatency(t *testing.T) {
	m := NewMetrics()
	m.RecordDestLatency("ch1", "dest1", 5*time.Millisecond)

	snap := m.Snapshot()
	timings := snap["timings"].(map[string]map[string]any)
	if timings["destination_latency.ch1.dest1"]["count"].(int64) != 1 {
		t.Error("expected 1 timing entry")
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	m := NewMetrics()
	m.IncrReceived("ch1")
	m.Gauge("active_channels").Store(5)
	m.RecordLatency("ch1", "total", time.Millisecond)

	snap := m.Snapshot()
	if snap["counters"] == nil {
		t.Error("expected counters in snapshot")
	}
	if snap["gauges"] == nil {
		t.Error("expected gauges in snapshot")
	}
	if snap["timings"] == nil {
		t.Error("expected timings in snapshot")
	}

	gauges := snap["gauges"].(map[string]int64)
	if gauges["active_channels"] != 5 {
		t.Errorf("expected gauge=5, got %d", gauges["active_channels"])
	}
}

func TestGlobal(t *testing.T) {
	m := Global()
	if m == nil {
		t.Fatal("expected non-nil global metrics")
	}
}

func TestOTelMetrics_NilSafe(t *testing.T) {
	var om *OTelMetrics
	om.IncrReceived("ch1")
	om.IncrProcessed("ch1")
	om.IncrErrored("ch1", "dest1")
	om.IncrFiltered("ch1")
	om.RecordLatency("ch1", "total", time.Millisecond)
	om.RecordDestLatency("ch1", "dest1", time.Millisecond)
	om.SetQueueDepth("ch1", "dest1", 5)
}

func TestNewChannelLogger_DefaultLevel(t *testing.T) {
	cl := NewChannelLogger("test-ch", nil, "info")
	if cl == nil {
		t.Fatal("expected non-nil ChannelLogger")
	}
	if cl.Logger() == nil {
		t.Fatal("expected non-nil logger")
	}
	if cl.channelID != "test-ch" {
		t.Errorf("expected 'test-ch', got %q", cl.channelID)
	}
}

func TestNewChannelLogger_DebugLevel(t *testing.T) {
	cl := NewChannelLogger("test-ch", &config.ChannelLogging{Level: "debug"}, "info")
	if cl == nil {
		t.Fatal("expected non-nil ChannelLogger")
	}
}

func TestNewChannelLogger_WarnLevel(t *testing.T) {
	cl := NewChannelLogger("test-ch", &config.ChannelLogging{Level: "warn"}, "info")
	if cl == nil {
		t.Fatal("expected non-nil ChannelLogger")
	}
}

func TestNewChannelLogger_ErrorLevel(t *testing.T) {
	cl := NewChannelLogger("test-ch", &config.ChannelLogging{Level: "error"}, "info")
	if cl == nil {
		t.Fatal("expected non-nil ChannelLogger")
	}
}

func TestNewChannelLogger_SilentLevel(t *testing.T) {
	cl := NewChannelLogger("test-ch", &config.ChannelLogging{Level: "silent"}, "info")
	if cl == nil {
		t.Fatal("expected non-nil ChannelLogger")
	}
}

func TestNewChannelLoggerWithWriter(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{Level: "debug"}, "info", &buf)
	if cl == nil {
		t.Fatal("expected non-nil ChannelLogger")
	}
	cl.Logger().Debug("test message")
	if !strings.Contains(buf.String(), "test message") {
		t.Errorf("expected 'test message' in output, got %q", buf.String())
	}
}

func TestChannelLogger_LogSourcePayload(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{
		Level:   "debug",
		Payloads: &config.PayloadLogging{Source: true},
	}, "info", &buf)

	cl.LogSourcePayload([]byte("source data"))
	if !strings.Contains(buf.String(), "source payload") {
		t.Error("expected 'source payload' in log")
	}
}

func TestChannelLogger_LogSourcePayload_Disabled(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{
		Level:   "debug",
		Payloads: &config.PayloadLogging{Source: false},
	}, "info", &buf)

	cl.LogSourcePayload([]byte("source data"))
	if strings.Contains(buf.String(), "source payload") {
		t.Error("expected no 'source payload' in log when disabled")
	}
}

func TestChannelLogger_LogSourcePayload_NilPayloadCfg(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", nil, "debug", &buf)
	cl.LogSourcePayload([]byte("data"))
	if strings.Contains(buf.String(), "source") {
		t.Error("expected no log when payloads config is nil")
	}
}

func TestChannelLogger_LogTransformedPayload(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{
		Level:   "debug",
		Payloads: &config.PayloadLogging{Transformed: true},
	}, "info", &buf)

	cl.LogTransformedPayload([]byte("transformed data"))
	if !strings.Contains(buf.String(), "transformed payload") {
		t.Error("expected 'transformed payload' in log")
	}
}

func TestChannelLogger_LogSentPayload(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{
		Level:   "debug",
		Payloads: &config.PayloadLogging{Sent: true},
	}, "info", &buf)

	cl.LogSentPayload("dest1", []byte("sent data"))
	if !strings.Contains(buf.String(), "sent payload") {
		t.Error("expected 'sent payload' in log")
	}
}

func TestChannelLogger_LogResponsePayload(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{
		Level:   "debug",
		Payloads: &config.PayloadLogging{Response: true},
	}, "info", &buf)

	cl.LogResponsePayload("dest1", []byte("response data"))
	if !strings.Contains(buf.String(), "response payload") {
		t.Error("expected 'response payload' in log")
	}
}

func TestChannelLogger_LogFilteredMessage(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{
		Level:   "debug",
		Payloads: &config.PayloadLogging{Filtered: true},
	}, "info", &buf)

	cl.LogFilteredMessage("msg-123")
	if !strings.Contains(buf.String(), "message filtered") {
		t.Error("expected 'message filtered' in log")
	}
}

func TestChannelLogger_LogFilteredMessage_Disabled(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{
		Level:   "debug",
		Payloads: &config.PayloadLogging{Filtered: false},
	}, "info", &buf)

	cl.LogFilteredMessage("msg-123")
	if strings.Contains(buf.String(), "filtered") {
		t.Error("expected no log when filtered is disabled")
	}
}

func TestChannelLogger_Truncate(t *testing.T) {
	cl := &ChannelLogger{
		truncateAt: 10,
	}

	short := cl.truncate("hello")
	if short != "hello" {
		t.Errorf("expected 'hello', got %q", short)
	}

	long := cl.truncate("this is a very long string that should be truncated")
	if !strings.HasSuffix(long, "...(truncated)") {
		t.Errorf("expected truncated suffix, got %q", long)
	}
	if len(long) != 10+len("...(truncated)") {
		t.Errorf("unexpected truncated length: %d", len(long))
	}
}

func TestChannelLogger_Truncate_ZeroLimit(t *testing.T) {
	cl := &ChannelLogger{
		truncateAt: 0,
	}

	long := cl.truncate("this is a very long string")
	if long != "this is a very long string" {
		t.Error("expected no truncation with zero limit")
	}
}

func TestChannelLogger_LogTransformedPayload_Disabled(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{
		Level:   "debug",
		Payloads: &config.PayloadLogging{Transformed: false},
	}, "info", &buf)

	cl.LogTransformedPayload([]byte("data"))
	if strings.Contains(buf.String(), "transformed") {
		t.Error("expected no log when disabled")
	}
}

func TestChannelLogger_LogSentPayload_Disabled(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{
		Level:   "debug",
		Payloads: &config.PayloadLogging{Sent: false},
	}, "info", &buf)

	cl.LogSentPayload("dest1", []byte("data"))
	if strings.Contains(buf.String(), "sent") {
		t.Error("expected no log when disabled")
	}
}

func TestChannelLogger_LogResponsePayload_Disabled(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{
		Level:   "debug",
		Payloads: &config.PayloadLogging{Response: false},
	}, "info", &buf)

	cl.LogResponsePayload("dest1", []byte("data"))
	if strings.Contains(buf.String(), "response") {
		t.Error("expected no log when disabled")
	}
}

func TestChannelLogger_WithTruncation(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{
		Level:      "debug",
		TruncateAt: 5,
		Payloads:   &config.PayloadLogging{Source: true},
	}, "info", &buf)

	cl.LogSourcePayload([]byte("this is very long data that should be truncated"))
	output := buf.String()
	if !strings.Contains(output, "truncated") {
		t.Error("expected truncated marker in output")
	}
}

func TestChannelLogger_GlobalLevelUsedWhenNoOverride(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", nil, "debug", &buf)
	cl.Logger().Debug("debug message from global level")
	if !strings.Contains(buf.String(), "debug message") {
		t.Error("expected debug message when global level is debug")
	}
}

func TestChannelLogger_ChannelLevelOverridesGlobal(t *testing.T) {
	var buf bytes.Buffer
	cl := NewChannelLoggerWithWriter("test-ch", &config.ChannelLogging{Level: "error"}, "debug", &buf)
	cl.Logger().Debug("should not appear")
	if strings.Contains(buf.String(), "should not appear") {
		t.Error("expected channel error level to suppress debug")
	}
}
