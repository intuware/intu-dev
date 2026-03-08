package logging

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu/pkg/config"
)

func TestStdoutTransportWrite(t *testing.T) {
	s := &stdoutTransport{}
	n, err := s.Write([]byte("hello\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 6 {
		t.Fatalf("expected 6 bytes written, got %d", n)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
}

type mockTransport struct {
	mu   sync.Mutex
	data []byte
}

func (m *mockTransport) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append(m.data, p...)
	return len(p), nil
}

func (m *mockTransport) Close() error { return nil }

func (m *mockTransport) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return string(m.data)
}

func TestMultiTransportFansOut(t *testing.T) {
	t1 := &mockTransport{}
	t2 := &mockTransport{}
	multi := NewMultiTransport(t1, t2)

	msg := []byte(`{"level":"info","msg":"test"}` + "\n")
	n, err := multi.Write(msg)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	if n != len(msg) {
		t.Fatalf("expected %d bytes, got %d", len(msg), n)
	}

	if t1.String() != string(msg) {
		t.Fatalf("transport 1 got: %q", t1.String())
	}
	if t2.String() != string(msg) {
		t.Fatalf("transport 2 got: %q", t2.String())
	}

	if err := multi.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
}

func TestNewTransportFromConfigNilReturnsStdout(t *testing.T) {
	transport, err := NewTransportFromConfig(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := transport.(*stdoutTransport); !ok {
		t.Fatalf("expected stdoutTransport, got %T", transport)
	}
}

func TestNewTransportFromConfigEmptyReturnsStdout(t *testing.T) {
	cfg := &config.LoggingConfig{}
	transport, err := NewTransportFromConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := transport.(*stdoutTransport); !ok {
		t.Fatalf("expected stdoutTransport, got %T", transport)
	}
}

func TestNewTransportFromConfigStdoutOnly(t *testing.T) {
	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "stdout"},
		},
	}
	transport, err := NewTransportFromConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := transport.(*stdoutTransport); !ok {
		t.Fatalf("expected stdoutTransport, got %T", transport)
	}
}

func TestNewTransportFromConfigMultipleReturnsMulti(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")
	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "stdout"},
			{Type: "file", File: &config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3}},
		},
	}
	transport, err := NewTransportFromConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := transport.(*MultiTransport); !ok {
		t.Fatalf("expected MultiTransport, got %T", transport)
	}
	transport.Close()
}

func TestNewTransportFromConfigUnknownTypeErrors(t *testing.T) {
	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "unknown_service"},
		},
	}
	_, err := NewTransportFromConfig(cfg)
	if err == nil {
		t.Fatal("expected error for unknown transport type")
	}
	if !strings.Contains(err.Error(), "unknown log transport type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewTransportFromConfigMissingSubConfigErrors(t *testing.T) {
	cases := []string{"cloudwatch", "datadog", "sumologic", "elasticsearch", "file"}
	for _, tc := range cases {
		cfg := &config.LoggingConfig{
			Transports: []config.LogTransportConfig{{Type: tc}},
		}
		_, err := NewTransportFromConfig(cfg)
		if err == nil {
			t.Fatalf("expected error for %s without sub-config", tc)
		}
	}
}

func TestLoggerNewWithNilConfigUsesStdout(t *testing.T) {
	logger := New("info", nil)
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestLoggerNewWithFileTransport(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")

	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "file", File: &config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3}},
		},
	}

	logger := New("info", cfg)
	logger.Info("test message", "key", "value")

	time.Sleep(100 * time.Millisecond)

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected log data in file")
	}

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimRight(data, "\n"), &entry); err != nil {
		t.Fatalf("unmarshal log entry: %v", err)
	}
	if entry["msg"] != "test message" {
		t.Fatalf("expected msg 'test message', got %v", entry["msg"])
	}
}

func TestWriterFromConfigNilReturnsStdout(t *testing.T) {
	w := WriterFromConfig(nil)
	if w != os.Stdout {
		t.Fatal("expected os.Stdout")
	}
}

func TestFileTransportCreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "subdir", "nested", "app.log")

	ft, err := NewFileTransport(&config.FileLogConfig{
		Path:      logFile,
		MaxSizeMB: 10,
		MaxFiles:  3,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ft.Close()

	n, err := ft.Write([]byte("hello\n"))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	if n != 6 {
		t.Fatalf("expected 6 bytes, got %d", n)
	}

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "hello\n" {
		t.Fatalf("got %q", string(data))
	}
}

func TestFileTransportRotation(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")

	ft, err := NewFileTransport(&config.FileLogConfig{
		Path:      logFile,
		MaxSizeMB: 0,
		MaxFiles:  2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ft.maxBytes = 100

	for i := 0; i < 5; i++ {
		line := strings.Repeat("x", 30) + "\n"
		ft.Write([]byte(line))
	}

	ft.Close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected at least 2 files (current + rotated), got %d", len(entries))
	}
}

func TestDatadogTransportMissingAPIKeyErrors(t *testing.T) {
	_, err := NewDatadogTransport(&config.DatadogLogConfig{})
	if err == nil {
		t.Fatal("expected error for missing API key")
	}
}

func TestDatadogTransportSendsToHTTP(t *testing.T) {
	var received []byte
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		received = append(received, body...)
		if r.Header.Get("DD-API-KEY") != "test-key" {
			t.Errorf("expected DD-API-KEY header 'test-key', got %q", r.Header.Get("DD-API-KEY"))
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dd := &DatadogTransport{
		apiKey:  "test-key",
		url:     srv.URL,
		service: "intu",
		source:  "intu",
		tags:    "env:test",
		client:  srv.Client(),
	}
	dd.batch = newBatchBuffer(100, 5242880, 50*time.Millisecond, dd.flushBatch)

	logLine := `{"level":"info","msg":"hello"}` + "\n"
	dd.Write([]byte(logLine))

	time.Sleep(200 * time.Millisecond)
	dd.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(received) == 0 {
		t.Fatal("expected data to be sent to datadog")
	}

	var entries []map[string]any
	if err := json.Unmarshal(received, &entries); err != nil {
		t.Fatalf("unmarshal received: %v (raw: %s)", err, string(received))
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one entry")
	}
	if entries[0]["ddsource"] != "intu" {
		t.Fatalf("expected ddsource 'intu', got %v", entries[0]["ddsource"])
	}
	if entries[0]["service"] != "intu" {
		t.Fatalf("expected service 'intu', got %v", entries[0]["service"])
	}
}

func TestSumoLogicTransportMissingEndpointErrors(t *testing.T) {
	_, err := NewSumoLogicTransport(&config.SumoLogicLogConfig{})
	if err == nil {
		t.Fatal("expected error for missing endpoint")
	}
}

func TestSumoLogicTransportSendsToHTTP(t *testing.T) {
	var received []byte
	var mu sync.Mutex
	var headers http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		received = append(received, body...)
		headers = r.Header
		w.WriteHeader(200)
	}))
	defer srv.Close()

	sl, err := NewSumoLogicTransport(&config.SumoLogicLogConfig{
		Endpoint:       srv.URL,
		SourceCategory: "test/intu",
		SourceName:     "test-instance",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sl.client = srv.Client()
	sl.batch.Close()
	sl.batch = newBatchBuffer(100, 1048576, 50*time.Millisecond, sl.flushBatch)

	logLine := `{"level":"info","msg":"hello"}` + "\n"
	sl.Write([]byte(logLine))

	time.Sleep(200 * time.Millisecond)
	sl.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(received) == 0 {
		t.Fatal("expected data to be sent")
	}
	if headers.Get("X-Sumo-Category") != "test/intu" {
		t.Fatalf("expected X-Sumo-Category header, got %q", headers.Get("X-Sumo-Category"))
	}
	if headers.Get("X-Sumo-Name") != "test-instance" {
		t.Fatalf("expected X-Sumo-Name header, got %q", headers.Get("X-Sumo-Name"))
	}
}

func TestElasticsearchTransportMissingURLsErrors(t *testing.T) {
	_, err := NewElasticsearchTransport(&config.ElasticsearchLogConfig{})
	if err == nil {
		t.Fatal("expected error for missing URLs")
	}
}

func TestElasticsearchTransportSendsNDJSON(t *testing.T) {
	var received []byte
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		received = append(received, body...)
		if r.Header.Get("Content-Type") != "application/x-ndjson" {
			t.Errorf("expected ndjson content type, got %q", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	es, err := NewElasticsearchTransport(&config.ElasticsearchLogConfig{
		URLs:  []string{srv.URL},
		Index: "intu-logs-{year}.{month}",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	es.client = srv.Client()
	es.batch.Close()
	es.batch = newBatchBuffer(100, 5242880, 50*time.Millisecond, es.flushBatch)

	logLine := `{"level":"info","msg":"hello"}` + "\n"
	es.Write([]byte(logLine))

	time.Sleep(200 * time.Millisecond)
	es.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(received) == 0 {
		t.Fatal("expected data to be sent")
	}

	lines := strings.Split(strings.TrimSpace(string(received)), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected at least 2 lines (action + doc), got %d: %q", len(lines), string(received))
	}

	var action map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &action); err != nil {
		t.Fatalf("unmarshal action: %v", err)
	}
	indexAction, ok := action["index"].(map[string]any)
	if !ok {
		t.Fatal("expected index action")
	}
	idx, _ := indexAction["_index"].(string)
	if !strings.HasPrefix(idx, "intu-logs-") {
		t.Fatalf("expected index starting with 'intu-logs-', got %q", idx)
	}
}

func TestElasticsearchTransportBasicAuth(t *testing.T) {
	var authHeader string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authHeader = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer srv.Close()

	es, err := NewElasticsearchTransport(&config.ElasticsearchLogConfig{
		URLs:     []string{srv.URL},
		Username: "elastic",
		Password: "secret",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	es.client = srv.Client()
	es.batch.Close()
	es.batch = newBatchBuffer(100, 5242880, 50*time.Millisecond, es.flushBatch)

	es.Write([]byte(`{"msg":"test"}` + "\n"))

	time.Sleep(200 * time.Millisecond)
	es.Close()

	mu.Lock()
	defer mu.Unlock()
	if !strings.HasPrefix(authHeader, "Basic ") {
		t.Fatalf("expected Basic auth header, got %q", authHeader)
	}
}

func TestBatchBufferFlushesOnCount(t *testing.T) {
	var flushed [][]byte
	var mu sync.Mutex
	flush := func(batch [][]byte) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = append(flushed, batch...)
		return nil
	}

	buf := newBatchBuffer(3, 1048576, 10*time.Second, flush)
	buf.Add([]byte("a"))
	buf.Add([]byte("b"))

	mu.Lock()
	count := len(flushed)
	mu.Unlock()
	if count != 0 {
		t.Fatalf("expected no flush yet, got %d items", count)
	}

	buf.Add([]byte("c"))

	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	count = len(flushed)
	mu.Unlock()
	if count != 3 {
		t.Fatalf("expected 3 flushed items, got %d", count)
	}

	buf.Close()
}

func TestBatchBufferFlushesOnClose(t *testing.T) {
	var flushed [][]byte
	var mu sync.Mutex
	flush := func(batch [][]byte) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = append(flushed, batch...)
		return nil
	}

	buf := newBatchBuffer(1000, 10485760, 10*time.Second, flush)
	buf.Add([]byte("a"))
	buf.Add([]byte("b"))

	buf.Close()

	mu.Lock()
	count := len(flushed)
	mu.Unlock()
	if count != 2 {
		t.Fatalf("expected 2 flushed items on close, got %d", count)
	}
}

func TestLoggerIntegrationWithMultiTransport(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "multi.log")

	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "file", File: &config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3}},
		},
	}

	logger := New("debug", cfg)
	logger.Info("integration test", "component", "transport")
	logger.Debug("debug message", "detail", "verbose")

	time.Sleep(100 * time.Millisecond)

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 log lines, got %d: %s", len(lines), string(data))
	}

	for _, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if _, ok := entry["time"]; !ok {
			t.Fatal("expected time field in log entry")
		}
		if _, ok := entry["level"]; !ok {
			t.Fatal("expected level field in log entry")
		}
	}
}

func TestLoggerWithSlogHandlerRoutesThroughTransport(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "slog.log")

	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "file", File: &config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3}},
		},
	}

	logger := New("info", cfg)

	logger.Info("message one", "key1", "val1")
	logger.Warn("message two", "key2", "val2")
	logger.With("channel", "test-ch").Info("channel message")

	time.Sleep(100 * time.Millisecond)

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 log lines, got %d", len(lines))
	}

	var lastEntry map[string]any
	json.Unmarshal([]byte(lines[2]), &lastEntry)
	if lastEntry["channel"] != "test-ch" {
		t.Fatalf("expected channel attribute, got %v", lastEntry)
	}
}

func TestNewLoggerFallsBackOnBadConfig(t *testing.T) {
	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "datadog"},
		},
	}

	logger := New("info", cfg)
	if logger == nil {
		t.Fatal("expected fallback logger, got nil")
	}

	logger.Info("this should not panic")
}

func TestFileTransportMissingPathErrors(t *testing.T) {
	_, err := NewFileTransport(&config.FileLogConfig{})
	if err == nil {
		t.Fatal("expected error for missing path")
	}
}

func TestChannelLoggerWithWriter(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "channel.log")

	f, err := os.Create(logFile)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()

	handler := slog.NewJSONHandler(f, &slog.HandlerOptions{Level: slog.LevelDebug})
	_ = handler

	ft, err := NewFileTransport(&config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3})
	if err != nil {
		t.Fatalf("new file transport: %v", err)
	}
	defer ft.Close()

	n, _ := ft.Write([]byte(`{"msg":"test from writer"}` + "\n"))
	if n == 0 {
		t.Fatal("expected bytes written")
	}

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(data), "test from writer") {
		t.Fatalf("expected 'test from writer' in log file, got: %s", string(data))
	}
}
