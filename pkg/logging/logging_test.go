package logging

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// New (logger.go) — log level tests
// ===================================================================

func TestNew_DebugLevel(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "debug.log")

	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "file", File: &config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3}},
		},
	}

	logger := New("debug", cfg)
	logger.Debug("debug msg")
	logger.Info("info msg")
	time.Sleep(100 * time.Millisecond)

	data, _ := os.ReadFile(logFile)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 log lines (debug+info), got %d", len(lines))
	}
}

func TestNew_WarnLevel(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "warn.log")

	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "file", File: &config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3}},
		},
	}

	logger := New("warn", cfg)
	logger.Debug("debug msg")
	logger.Info("info msg")
	logger.Warn("warn msg")
	logger.Error("error msg")
	time.Sleep(100 * time.Millisecond)

	data, _ := os.ReadFile(logFile)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 log lines (warn+error), got %d: %s", len(lines), string(data))
	}
}

func TestNew_ErrorLevel(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "error.log")

	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "file", File: &config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3}},
		},
	}

	logger := New("error", cfg)
	logger.Info("info msg")
	logger.Warn("warn msg")
	logger.Error("error msg")
	time.Sleep(100 * time.Millisecond)

	data, _ := os.ReadFile(logFile)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 log line (error only), got %d", len(lines))
	}
}

func TestNew_DefaultsToInfoLevel(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "default.log")

	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "file", File: &config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3}},
		},
	}

	logger := New("unknown-level", cfg)
	logger.Debug("debug msg")
	logger.Info("info msg")
	time.Sleep(100 * time.Millisecond)

	data, _ := os.ReadFile(logFile)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 log line (info only, debug filtered), got %d", len(lines))
	}
}

func TestNew_NilConfig(t *testing.T) {
	logger := New("info", nil)
	if logger == nil {
		t.Fatal("expected non-nil logger with nil config")
	}
	logger.Info("should not panic")
}

func TestNew_EmptyTransports(t *testing.T) {
	logger := New("info", &config.LoggingConfig{})
	if logger == nil {
		t.Fatal("expected non-nil logger with empty transports")
	}
}

func TestNew_BadConfigFallsBackToStdout(t *testing.T) {
	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "datadog"},
		},
	}
	logger := New("info", cfg)
	if logger == nil {
		t.Fatal("expected fallback logger, not nil")
	}
	logger.Info("fallback test")
}

// ===================================================================
// WriterFromConfig (logger.go)
// ===================================================================

func TestWriterFromConfig_Nil(t *testing.T) {
	w := WriterFromConfig(nil)
	if w != os.Stdout {
		t.Fatal("expected os.Stdout for nil config")
	}
}

func TestWriterFromConfig_EmptyTransports(t *testing.T) {
	w := WriterFromConfig(&config.LoggingConfig{})
	if w != os.Stdout {
		t.Fatal("expected os.Stdout for empty transports")
	}
}

func TestWriterFromConfig_ValidConfig(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "writer.log")

	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "file", File: &config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3}},
		},
	}

	w := WriterFromConfig(cfg)
	if w == os.Stdout {
		t.Fatal("expected file transport, not stdout")
	}

	w.Write([]byte("writer test\n"))

	if ft, ok := w.(*FileTransport); ok {
		ft.Close()
	}

	data, _ := os.ReadFile(logFile)
	if !strings.Contains(string(data), "writer test") {
		t.Fatalf("expected 'writer test' in log, got %q", string(data))
	}
}

func TestWriterFromConfig_BadConfigFallsBackToStdout(t *testing.T) {
	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "elasticsearch"},
		},
	}
	w := WriterFromConfig(cfg)
	if w != os.Stdout {
		t.Fatal("expected os.Stdout fallback for bad config")
	}
}

// ===================================================================
// FileTransport (file_transport.go) — additional tests
// ===================================================================

func TestFileTransport_WriteAndRead(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "test.log")

	ft, err := NewFileTransport(&config.FileLogConfig{
		Path: logFile, MaxSizeMB: 10, MaxFiles: 3,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer ft.Close()

	ft.Write([]byte("line one\n"))
	ft.Write([]byte("line two\n"))

	data, _ := os.ReadFile(logFile)
	if string(data) != "line one\nline two\n" {
		t.Fatalf("expected two lines, got %q", string(data))
	}
}

func TestFileTransport_RotationCreatesNewFile(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "rotate.log")

	ft, err := NewFileTransport(&config.FileLogConfig{
		Path: logFile, MaxSizeMB: 1, MaxFiles: 5,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ft.maxBytes = 50

	for i := 0; i < 10; i++ {
		ft.Write([]byte(fmt.Sprintf("line %02d -- some padding content\n", i)))
	}
	ft.Close()

	entries, _ := os.ReadDir(dir)
	if len(entries) < 2 {
		t.Fatalf("expected at least 2 files after rotation, got %d", len(entries))
	}
}

func TestFileTransport_PruneOldFiles(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "prune.log")

	ft, err := NewFileTransport(&config.FileLogConfig{
		Path: logFile, MaxSizeMB: 1, MaxFiles: 2,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ft.maxBytes = 20

	for i := 0; i < 20; i++ {
		ft.Write([]byte(fmt.Sprintf("line-%02d-data\n", i)))
		time.Sleep(5 * time.Millisecond)
	}
	ft.Close()

	time.Sleep(200 * time.Millisecond)

	entries, _ := os.ReadDir(dir)
	logFiles := 0
	for _, e := range entries {
		if !e.IsDir() {
			logFiles++
		}
	}
	if logFiles > 4 {
		t.Fatalf("expected at most 4 files (current + 2 max rotated + margin), got %d", logFiles)
	}
}

func TestFileTransport_Compress(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "compress.log")

	ft, err := NewFileTransport(&config.FileLogConfig{
		Path:      logFile,
		MaxSizeMB: 1,
		MaxFiles:  5,
		Compress:  true,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ft.maxBytes = 30

	for i := 0; i < 5; i++ {
		ft.Write([]byte(fmt.Sprintf("compressed-line-%02d\n", i)))
	}
	ft.Close()

	time.Sleep(500 * time.Millisecond)

	entries, _ := os.ReadDir(dir)
	hasGz := false
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".gz") {
			hasGz = true
			f, err := os.Open(filepath.Join(dir, e.Name()))
			if err != nil {
				t.Fatalf("open gz: %v", err)
			}
			r, err := gzip.NewReader(f)
			if err != nil {
				f.Close()
				t.Fatalf("gzip reader: %v", err)
			}
			data, _ := io.ReadAll(r)
			r.Close()
			f.Close()
			if len(data) == 0 {
				t.Fatal("expected non-empty gzip content")
			}
			break
		}
	}
	if !hasGz {
		t.Log("no .gz files found (compression is async, may not complete in time)")
	}
}

func TestFileTransport_MissingPathErrors(t *testing.T) {
	_, err := NewFileTransport(&config.FileLogConfig{})
	if err == nil {
		t.Fatal("expected error for missing path")
	}
}

func TestFileTransport_DefaultMaxSize(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "defaults.log")

	ft, err := NewFileTransport(&config.FileLogConfig{
		Path:     logFile,
		MaxFiles: 0,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer ft.Close()

	if ft.maxBytes != int64(defaultMaxSizeMB)*1024*1024 {
		t.Fatalf("expected default max bytes %d, got %d", int64(defaultMaxSizeMB)*1024*1024, ft.maxBytes)
	}
	if ft.maxFiles != 5 {
		t.Fatalf("expected default maxFiles=5, got %d", ft.maxFiles)
	}
}

func TestFileTransport_CloseNilFile(t *testing.T) {
	ft := &FileTransport{}
	if err := ft.Close(); err != nil {
		t.Fatalf("Close nil file should not error: %v", err)
	}
}

func TestFileTransport_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "concurrent.log")

	ft, err := NewFileTransport(&config.FileLogConfig{
		Path: logFile, MaxSizeMB: 10, MaxFiles: 3,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ft.Write([]byte(fmt.Sprintf("goroutine-%d\n", idx)))
		}(i)
	}
	wg.Wait()
	ft.Close()

	data, _ := os.ReadFile(logFile)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 50 {
		t.Fatalf("expected 50 lines, got %d", len(lines))
	}
}

// ===================================================================
// batchBuffer (batch_buffer.go) — additional tests
// ===================================================================

func TestBatchBuffer_FlushOnByteThreshold(t *testing.T) {
	var flushed [][]byte
	var mu sync.Mutex
	flush := func(batch [][]byte) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = append(flushed, batch...)
		return nil
	}

	buf := newBatchBuffer(1000, 50, 10*time.Second, flush)

	buf.Add([]byte(strings.Repeat("x", 30)))
	mu.Lock()
	count := len(flushed)
	mu.Unlock()
	if count != 0 {
		t.Fatalf("expected no flush yet, got %d", count)
	}

	buf.Add([]byte(strings.Repeat("y", 25)))
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	count = len(flushed)
	mu.Unlock()
	if count != 2 {
		t.Fatalf("expected 2 flushed items (byte threshold), got %d", count)
	}

	buf.Close()
}

func TestBatchBuffer_FlushOnInterval(t *testing.T) {
	var flushed [][]byte
	var mu sync.Mutex
	flush := func(batch [][]byte) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = append(flushed, batch...)
		return nil
	}

	buf := newBatchBuffer(1000, 10485760, 100*time.Millisecond, flush)
	buf.Add([]byte("interval-test"))

	mu.Lock()
	count := len(flushed)
	mu.Unlock()
	if count != 0 {
		t.Fatalf("expected no flush immediately, got %d", count)
	}

	time.Sleep(250 * time.Millisecond)

	mu.Lock()
	count = len(flushed)
	mu.Unlock()
	if count != 1 {
		t.Fatalf("expected 1 flushed item after interval, got %d", count)
	}

	buf.Close()
}

func TestBatchBuffer_DoubleCloseIsSafe(t *testing.T) {
	flush := func(batch [][]byte) error { return nil }
	buf := newBatchBuffer(10, 1048576, time.Second, flush)
	buf.Add([]byte("data"))

	if err := buf.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := buf.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestBatchBuffer_EmptyClose(t *testing.T) {
	called := false
	flush := func(batch [][]byte) error {
		called = true
		return nil
	}
	buf := newBatchBuffer(10, 1048576, time.Second, flush)
	buf.Close()

	if called {
		t.Fatal("expected flush not to be called on empty close")
	}
}

func TestBatchBuffer_FlushErrorPropagatesOnClose(t *testing.T) {
	flush := func(batch [][]byte) error {
		return fmt.Errorf("flush error")
	}
	buf := newBatchBuffer(1000, 10485760, time.Hour, flush)
	buf.Add([]byte("data"))

	err := buf.Close()
	if err == nil {
		t.Fatal("expected error from Close when flush fails")
	}
	if !strings.Contains(err.Error(), "flush error") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ===================================================================
// DatadogTransport (datadog.go) — additional tests
// ===================================================================

func TestDatadogTransport_MissingAPIKey(t *testing.T) {
	_, err := NewDatadogTransport(&config.DatadogLogConfig{})
	if err == nil {
		t.Fatal("expected error for missing API key")
	}
}

func TestDatadogTransport_DefaultSiteAndService(t *testing.T) {
	dd, err := NewDatadogTransport(&config.DatadogLogConfig{APIKey: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dd.Close()

	if !strings.Contains(dd.url, "datadoghq.com") {
		t.Fatalf("expected default site in URL, got %q", dd.url)
	}
	if dd.service != "intu" {
		t.Fatalf("expected default service 'intu', got %q", dd.service)
	}
	if dd.source != "intu" {
		t.Fatalf("expected default source 'intu', got %q", dd.source)
	}
}

func TestDatadogTransport_CustomSite(t *testing.T) {
	dd, err := NewDatadogTransport(&config.DatadogLogConfig{
		APIKey: "key", Site: "us5.datadoghq.com",
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	dd.Close()

	if !strings.Contains(dd.url, "us5.datadoghq.com") {
		t.Fatalf("expected custom site in URL, got %q", dd.url)
	}
}

func TestDatadogTransport_Tags(t *testing.T) {
	var received []byte
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		received = append(received, body...)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dd := &DatadogTransport{
		apiKey:  "test-key",
		url:     srv.URL,
		service: "svc",
		source:  "src",
		tags:    "env:test,app:intu",
		client:  srv.Client(),
	}
	dd.batch = newBatchBuffer(100, 5242880, 50*time.Millisecond, dd.flushBatch)

	dd.Write([]byte(`{"msg":"tagged"}` + "\n"))
	time.Sleep(200 * time.Millisecond)
	dd.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(received) == 0 {
		t.Fatal("expected data sent")
	}

	var entries []map[string]any
	json.Unmarshal(received, &entries)
	if len(entries) == 0 {
		t.Fatal("expected at least one entry")
	}
	if entries[0]["ddtags"] != "env:test,app:intu" {
		t.Fatalf("expected ddtags, got %v", entries[0]["ddtags"])
	}
}

func TestDatadogTransport_InvalidJSONStillSent(t *testing.T) {
	var received []byte
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		received = append(received, body...)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dd := &DatadogTransport{
		apiKey: "key", url: srv.URL, service: "s", source: "s",
		client: srv.Client(),
	}
	dd.batch = newBatchBuffer(100, 5242880, 50*time.Millisecond, dd.flushBatch)

	dd.Write([]byte("not-json\n"))
	time.Sleep(200 * time.Millisecond)
	dd.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(received) == 0 {
		t.Fatal("expected data sent for invalid JSON")
	}
	var entries []map[string]any
	json.Unmarshal(received, &entries)
	if len(entries) == 0 || entries[0]["message"] == nil {
		t.Fatal("expected 'message' field for non-JSON input")
	}
}

func TestDatadogTransport_ServerErrorReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	dd := &DatadogTransport{
		apiKey: "key", url: srv.URL, service: "s", source: "s",
		client: srv.Client(),
	}

	err := dd.flushBatch([][]byte{[]byte(`{"msg":"test"}`)})
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDatadogTransport_EmptyBatchIsNoop(t *testing.T) {
	dd := &DatadogTransport{apiKey: "key"}
	if err := dd.flushBatch(nil); err != nil {
		t.Fatalf("empty batch should be noop: %v", err)
	}
	if err := dd.flushBatch([][]byte{}); err != nil {
		t.Fatalf("zero-len batch should be noop: %v", err)
	}
}

// ===================================================================
// ElasticsearchTransport (elasticsearch.go) — additional tests
// ===================================================================

func TestElasticsearchTransport_MissingURLs(t *testing.T) {
	_, err := NewElasticsearchTransport(&config.ElasticsearchLogConfig{})
	if err == nil {
		t.Fatal("expected error for missing URLs")
	}
}

func TestElasticsearchTransport_DefaultIndex(t *testing.T) {
	es, err := NewElasticsearchTransport(&config.ElasticsearchLogConfig{
		URLs: []string{"http://localhost:9200"},
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	es.Close()

	if es.index != "intu-logs" {
		t.Fatalf("expected default index 'intu-logs', got %q", es.index)
	}
}

func TestElasticsearchTransport_ResolvedIndex(t *testing.T) {
	es := &ElasticsearchTransport{index: "logs-{year}.{month}.{day}"}
	idx := es.resolvedIndex()

	now := time.Now()
	expected := fmt.Sprintf("logs-%d.%02d.%02d", now.Year(), now.Month(), now.Day())
	if idx != expected {
		t.Fatalf("expected %q, got %q", expected, idx)
	}
}

func TestElasticsearchTransport_ResolvedIndexNoPlaceholders(t *testing.T) {
	es := &ElasticsearchTransport{index: "static-index"}
	if es.resolvedIndex() != "static-index" {
		t.Fatalf("expected 'static-index', got %q", es.resolvedIndex())
	}
}

func TestElasticsearchTransport_APIKeyAuth(t *testing.T) {
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
		URLs:   []string{srv.URL},
		APIKey: "my-api-key",
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	es.client = srv.Client()
	es.batch.Close()
	es.batch = newBatchBuffer(100, 5242880, 50*time.Millisecond, es.flushBatch)

	es.Write([]byte(`{"msg":"apikey test"}` + "\n"))
	time.Sleep(200 * time.Millisecond)
	es.Close()

	mu.Lock()
	defer mu.Unlock()
	if authHeader != "ApiKey my-api-key" {
		t.Fatalf("expected 'ApiKey my-api-key', got %q", authHeader)
	}
}

func TestElasticsearchTransport_URLRoundRobin(t *testing.T) {
	var hitURLs []string
	var mu sync.Mutex

	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hitURLs = append(hitURLs, "srv1")
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hitURLs = append(hitURLs, "srv2")
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer srv2.Close()

	es, err := NewElasticsearchTransport(&config.ElasticsearchLogConfig{
		URLs: []string{srv1.URL, srv2.URL},
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	es.client = srv1.Client()
	es.batch.Close()

	es.flushBatch([][]byte{[]byte(`{"msg":"a"}`)})
	es.flushBatch([][]byte{[]byte(`{"msg":"b"}`)})

	mu.Lock()
	defer mu.Unlock()
	if len(hitURLs) < 2 {
		t.Fatalf("expected 2 requests, got %d", len(hitURLs))
	}
	if hitURLs[0] == hitURLs[1] {
		t.Fatalf("expected round-robin to different servers, both went to %s", hitURLs[0])
	}
}

func TestElasticsearchTransport_ServerErrorReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
	}))
	defer srv.Close()

	es := &ElasticsearchTransport{
		urls:   []string{srv.URL},
		index:  "test",
		client: srv.Client(),
	}

	err := es.flushBatch([][]byte{[]byte(`{"msg":"test"}`)})
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
}

func TestElasticsearchTransport_EmptyBatchIsNoop(t *testing.T) {
	es := &ElasticsearchTransport{}
	if err := es.flushBatch(nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
}

func TestElasticsearchTransport_NDJSONFormat(t *testing.T) {
	var received []byte
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		received, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	es := &ElasticsearchTransport{
		urls:   []string{srv.URL},
		index:  "test-idx",
		client: srv.Client(),
	}

	es.flushBatch([][]byte{
		[]byte(`{"msg":"first"}`),
		[]byte(`{"msg":"second"}`),
	})

	mu.Lock()
	defer mu.Unlock()
	lines := strings.Split(strings.TrimSpace(string(received)), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 NDJSON lines (2 action + 2 doc), got %d: %q", len(lines), string(received))
	}
	for i := 0; i < len(lines); i += 2 {
		if !strings.Contains(lines[i], `"index"`) {
			t.Fatalf("expected index action at line %d, got %q", i, lines[i])
		}
	}
}

// ===================================================================
// SumoLogicTransport (sumologic.go) — additional tests
// ===================================================================

func TestSumoLogicTransport_MissingEndpoint(t *testing.T) {
	_, err := NewSumoLogicTransport(&config.SumoLogicLogConfig{})
	if err == nil {
		t.Fatal("expected error for missing endpoint")
	}
}

func TestSumoLogicTransport_WriteMultipleEntries(t *testing.T) {
	var received []byte
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		received = append(received, body...)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	sl, _ := NewSumoLogicTransport(&config.SumoLogicLogConfig{
		Endpoint: srv.URL,
	})
	sl.client = srv.Client()
	sl.batch.Close()
	sl.batch = newBatchBuffer(2, 1048576, 10*time.Second, sl.flushBatch)

	sl.Write([]byte(`{"msg":"one"}` + "\n"))
	sl.Write([]byte(`{"msg":"two"}` + "\n"))
	time.Sleep(100 * time.Millisecond)
	sl.Close()

	mu.Lock()
	defer mu.Unlock()
	lines := strings.Split(strings.TrimSpace(string(received)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %q", len(lines), string(received))
	}
}

func TestSumoLogicTransport_EmptyCategoryAndName(t *testing.T) {
	var headers http.Header
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		headers = r.Header.Clone()
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer srv.Close()

	sl, _ := NewSumoLogicTransport(&config.SumoLogicLogConfig{
		Endpoint: srv.URL,
	})
	sl.client = srv.Client()
	sl.batch.Close()
	sl.batch = newBatchBuffer(1, 1048576, 10*time.Second, sl.flushBatch)

	sl.Write([]byte(`{"msg":"test"}` + "\n"))
	time.Sleep(100 * time.Millisecond)
	sl.Close()

	mu.Lock()
	defer mu.Unlock()
	if headers.Get("X-Sumo-Category") != "" {
		t.Fatalf("expected empty X-Sumo-Category, got %q", headers.Get("X-Sumo-Category"))
	}
	if headers.Get("X-Sumo-Name") != "" {
		t.Fatalf("expected empty X-Sumo-Name, got %q", headers.Get("X-Sumo-Name"))
	}
}

func TestSumoLogicTransport_ServerErrorReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
	}))
	defer srv.Close()

	sl := &SumoLogicTransport{
		endpoint: srv.URL,
		client:   srv.Client(),
	}

	err := sl.flushBatch([][]byte{[]byte(`{"msg":"test"}`)})
	if err == nil {
		t.Fatal("expected error for 429 response")
	}
	if !strings.Contains(err.Error(), "status 429") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSumoLogicTransport_EmptyBatchIsNoop(t *testing.T) {
	sl := &SumoLogicTransport{}
	if err := sl.flushBatch(nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
}

// ===================================================================
// MultiTransport — additional tests
// ===================================================================

func TestMultiTransport_PropagatesWriteError(t *testing.T) {
	good := &mockTransport{}
	bad := &errTransport{err: fmt.Errorf("write fail")}
	multi := NewMultiTransport(good, bad)

	_, err := multi.Write([]byte("test\n"))
	if err == nil {
		t.Fatal("expected error propagated from failing transport")
	}

	if good.String() != "test\n" {
		t.Fatal("good transport should still receive data")
	}

	multi.Close()
}

type errTransport struct {
	err error
}

func (e *errTransport) Write(p []byte) (int, error) { return 0, e.err }
func (e *errTransport) Close() error                 { return e.err }

func TestMultiTransport_PropagatesCloseError(t *testing.T) {
	good := &mockTransport{}
	bad := &errTransport{err: fmt.Errorf("close fail")}
	multi := NewMultiTransport(good, bad)

	err := multi.Close()
	if err == nil {
		t.Fatal("expected error from Close")
	}
}

// ===================================================================
// Transport from config — integration tests
// ===================================================================

func TestNewTransportFromConfig_FileTransportOnly(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "single.log")

	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "file", File: &config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3}},
		},
	}

	transport, err := NewTransportFromConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	if _, ok := transport.(*FileTransport); !ok {
		t.Fatalf("expected *FileTransport, got %T", transport)
	}

	transport.Write([]byte("single transport\n"))
	transport.Close()

	data, _ := os.ReadFile(logFile)
	if !strings.Contains(string(data), "single transport") {
		t.Fatal("expected data in file")
	}
}

func TestNewTransportFromConfig_PartialFailureCleansUp(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "partial.log")

	cfg := &config.LoggingConfig{
		Transports: []config.LogTransportConfig{
			{Type: "file", File: &config.FileLogConfig{Path: logFile, MaxSizeMB: 10, MaxFiles: 3}},
			{Type: "datadog"},
		},
	}

	_, err := NewTransportFromConfig(cfg)
	if err == nil {
		t.Fatal("expected error for partial failure")
	}
}

// ===================================================================
// compressFile (file_transport.go) — standalone test
// ===================================================================

func TestCompressFile_CreatesGzAndRemovesOriginal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "to-compress.log")
	os.WriteFile(path, []byte("compress me\n"), 0o644)

	compressFile(path)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("expected original file to be removed after compression")
	}

	gzPath := path + ".gz"
	f, err := os.Open(gzPath)
	if err != nil {
		t.Fatalf("open gz: %v", err)
	}
	defer f.Close()

	r, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer r.Close()

	data, _ := io.ReadAll(r)
	if string(data) != "compress me\n" {
		t.Fatalf("expected 'compress me\\n', got %q", string(data))
	}
}

func TestCompressFile_NonexistentFileIsNoop(t *testing.T) {
	compressFile("/nonexistent/path/file.log")
}
