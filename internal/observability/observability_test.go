package observability

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
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
