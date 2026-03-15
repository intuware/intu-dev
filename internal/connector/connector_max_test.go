package connector

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

func testLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---------------------------------------------------------------------------
// mergeMaps
// ---------------------------------------------------------------------------

func TestMergeMapsMax_BothNil(t *testing.T) {
	m := mergeMaps(nil, nil)
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
}

func TestMergeMapsMax_BaseOnly(t *testing.T) {
	base := map[string]string{"a": "1", "b": "2"}
	m := mergeMaps(base, nil)
	if m["a"] != "1" || m["b"] != "2" {
		t.Errorf("unexpected result: %v", m)
	}
}

func TestMergeMapsMax_OverrideOnly(t *testing.T) {
	m := mergeMaps(nil, map[string]string{"x": "9"})
	if m["x"] != "9" {
		t.Errorf("unexpected result: %v", m)
	}
}

func TestMergeMapsMax_OverridePrecedence(t *testing.T) {
	base := map[string]string{"a": "1", "b": "2"}
	over := map[string]string{"b": "override", "c": "3"}
	m := mergeMaps(base, over)
	if m["a"] != "1" {
		t.Error("base key should be preserved")
	}
	if m["b"] != "override" {
		t.Error("override key should win")
	}
	if m["c"] != "3" {
		t.Error("new override key should appear")
	}
}

// ---------------------------------------------------------------------------
// HTTPDest.Send with different response codes
// ---------------------------------------------------------------------------

func TestHTTPDest_Send_200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{URL: srv.URL, Method: "POST"}, testLog())
	msg := message.New("ch1", []byte(`hello`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_Send_404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		w.Write([]byte("not found"))
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{URL: srv.URL, Method: "GET"}, testLog())
	msg := message.New("ch1", []byte(`test`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_Send_500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{URL: srv.URL, Method: "POST"}, testLog())
	msg := message.New("ch1", []byte(`data`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 500 {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_Send_DefaultMethodPOST(t *testing.T) {
	var capturedMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedMethod = r.Method
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{URL: srv.URL}, testLog())
	msg := message.New("ch1", []byte(`data`))
	dest.Send(context.Background(), msg)
	if capturedMethod != "POST" {
		t.Errorf("expected default method POST, got %s", capturedMethod)
	}
}

func TestHTTPDest_Send_WithHeaders(t *testing.T) {
	var capturedHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{
		URL:     srv.URL,
		Method:  "POST",
		Headers: map[string]string{"X-Custom": "my-value"},
	}, testLog())
	msg := message.New("ch1", []byte(`data`))
	dest.Send(context.Background(), msg)
	if capturedHeaders.Get("X-Custom") != "my-value" {
		t.Errorf("expected X-Custom header, got %q", capturedHeaders.Get("X-Custom"))
	}
}

func TestHTTPDest_Send_WithQueryParams(t *testing.T) {
	var capturedQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{
		URL:         srv.URL,
		Method:      "POST",
		QueryParams: map[string]string{"key": "value"},
	}, testLog())
	msg := message.New("ch1", []byte(`data`))
	dest.Send(context.Background(), msg)
	if !strings.Contains(capturedQuery, "key=value") {
		t.Errorf("expected query param, got %q", capturedQuery)
	}
}

func TestHTTPDest_Send_WithPathParams(t *testing.T) {
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{
		URL:        srv.URL + "/api/{id}",
		Method:     "POST",
		PathParams: map[string]string{"id": "42"},
	}, testLog())
	msg := message.New("ch1", []byte(`data`))
	dest.Send(context.Background(), msg)
	if capturedPath != "/api/42" {
		t.Errorf("expected /api/42, got %s", capturedPath)
	}
}

func TestHTTPDest_Send_ConnectError(t *testing.T) {
	dest := NewHTTPDest("test", &config.HTTPDestConfig{
		URL:    "http://127.0.0.1:1",
		Method: "POST",
	}, testLog())
	msg := message.New("ch1", []byte(`data`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error (should be in resp.Error): %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error in response for unreachable server")
	}
}

func TestHTTPDest_Send_TransportMeta(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{URL: srv.URL, Method: "PUT"}, testLog())
	msg := message.New("ch1", []byte(`data`))
	msg.TCP = &message.TCPMeta{RemoteAddr: "1.2.3.4:5678"}
	dest.Send(context.Background(), msg)
	if msg.Transport != "http" {
		t.Errorf("expected transport 'http', got %q", msg.Transport)
	}
	if msg.TCP != nil {
		t.Error("TCP meta should be cleared")
	}
	if msg.HTTP == nil {
		t.Fatal("HTTP meta should be set")
	}
	if msg.HTTP.Method != "PUT" {
		t.Errorf("expected method PUT, got %q", msg.HTTP.Method)
	}
}

func TestHTTPDest_Stop_Graceful(t *testing.T) {
	dest := NewHTTPDest("test", &config.HTTPDestConfig{URL: "http://example.com"}, testLog())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

func TestHTTPDest_Type_Value(t *testing.T) {
	dest := NewHTTPDest("test", &config.HTTPDestConfig{URL: "http://example.com"}, testLog())
	if dest.Type() != "http" {
		t.Errorf("expected type 'http', got %q", dest.Type())
	}
}

func TestHTTPDest_Send_WithAuth_Bearer(t *testing.T) {
	var authHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{
		URL:    srv.URL,
		Method: "POST",
		Auth:   &config.HTTPAuthConfig{Type: "bearer", Token: "my-token"},
	}, testLog())
	msg := message.New("ch1", []byte(`data`))
	dest.Send(context.Background(), msg)
	if authHeader != "Bearer my-token" {
		t.Errorf("expected Bearer auth, got %q", authHeader)
	}
}

func TestHTTPDest_Send_WithAuth_Basic(t *testing.T) {
	var authHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{
		URL:    srv.URL,
		Method: "POST",
		Auth:   &config.HTTPAuthConfig{Type: "basic", Username: "user", Password: "pass"},
	}, testLog())
	msg := message.New("ch1", []byte(`data`))
	dest.Send(context.Background(), msg)
	if !strings.HasPrefix(authHeader, "Basic ") {
		t.Errorf("expected Basic auth, got %q", authHeader)
	}
}

func TestHTTPDest_Send_WithAuth_APIKey_Header(t *testing.T) {
	var apiKeyHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKeyHeader = r.Header.Get("X-API-Key")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{
		URL:    srv.URL,
		Method: "POST",
		Auth:   &config.HTTPAuthConfig{Type: "api_key", Header: "X-API-Key", Key: "secret-key"},
	}, testLog())
	msg := message.New("ch1", []byte(`data`))
	dest.Send(context.Background(), msg)
	if apiKeyHeader != "secret-key" {
		t.Errorf("expected api key, got %q", apiKeyHeader)
	}
}

func TestHTTPDest_Send_WithAuth_APIKey_QueryParam(t *testing.T) {
	var capturedQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.RawQuery
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{
		URL:    srv.URL,
		Method: "POST",
		Auth:   &config.HTTPAuthConfig{Type: "api_key", QueryParam: "api_key", Key: "secret"},
	}, testLog())
	msg := message.New("ch1", []byte(`data`))
	dest.Send(context.Background(), msg)
	if !strings.Contains(capturedQuery, "api_key=secret") {
		t.Errorf("expected api_key in query, got %q", capturedQuery)
	}
}

// ---------------------------------------------------------------------------
// readMLLP edge cases
// ---------------------------------------------------------------------------

func TestReadMLLPMax_Normal(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0x0B)
	buf.WriteString("MSH|^~\\&|test")
	buf.WriteByte(0x1C)
	buf.WriteByte(0x0D)

	data, err := readMLLP(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "MSH|^~\\&|test" {
		t.Errorf("unexpected data: %q", data)
	}
}

func TestReadMLLPMax_MissingStartBlock(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0xFF)
	buf.WriteString("data")

	_, err := readMLLP(bufio.NewReader(&buf))
	if err == nil {
		t.Fatal("expected error for missing start block")
	}
	if !strings.Contains(err.Error(), "expected MLLP start block") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestReadMLLPMax_EmptyAfterStart(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0x0B)
	buf.WriteByte(0x1C)
	buf.WriteByte(0x0D)

	data, err := readMLLP(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected empty data, got %q", data)
	}
}

func TestReadMLLPMax_EndBlockWithoutCR(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0x0B)
	buf.WriteString("data")
	buf.WriteByte(0x1C)

	data, err := readMLLP(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "data" {
		t.Errorf("unexpected data: %q", data)
	}
}

// ---------------------------------------------------------------------------
// readRawTCP
// ---------------------------------------------------------------------------

func TestReadRawTCPMax_Simple(t *testing.T) {
	buf := bytes.NewBufferString("hello world\n")
	data, err := readRawTCP(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "hello world" {
		t.Errorf("expected 'hello world', got %q", data)
	}
}

func TestReadRawTCPMax_EmptyLine(t *testing.T) {
	buf := bytes.NewBufferString("\n")
	data, err := readRawTCP(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected empty, got %q", data)
	}
}

func TestReadRawTCPMax_WithCRLF(t *testing.T) {
	buf := bytes.NewBufferString("data\r\n")
	data, err := readRawTCP(bufio.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "data" {
		t.Errorf("expected 'data', got %q", data)
	}
}

func TestReadRawTCPMax_EOF(t *testing.T) {
	buf := bytes.NewBufferString("no newline")
	_, err := readRawTCP(bufio.NewReader(buf))
	if err == nil {
		t.Fatal("expected EOF error")
	}
}

// ---------------------------------------------------------------------------
// extractHL7ControlID / extractHL7AckCode
// ---------------------------------------------------------------------------

func TestExtractHL7ControlID_Present(t *testing.T) {
	data := []byte("MSH|^~\\&|SENDER|FAC|RECV|FAC|20240101||ADT^A01|CTRL123|P|2.5\rPID|1||12345")
	id := extractHL7ControlID(data)
	if id != "CTRL123" {
		t.Errorf("expected CTRL123, got %q", id)
	}
}

func TestExtractHL7ControlID_Missing(t *testing.T) {
	data := []byte("PID|1||12345")
	id := extractHL7ControlID(data)
	if id != "0" {
		t.Errorf("expected '0' for missing control ID, got %q", id)
	}
}

func TestExtractHL7AckCode_Present(t *testing.T) {
	data := []byte("MSH|^~\\&|test\rMSA|AA|CTRL123")
	code := extractHL7AckCode(data)
	if code != "AA" {
		t.Errorf("expected AA, got %q", code)
	}
}

func TestExtractHL7AckCode_NACK(t *testing.T) {
	data := []byte("MSH|^~\\&|test\rMSA|AE|CTRL123")
	code := extractHL7AckCode(data)
	if code != "AE" {
		t.Errorf("expected AE, got %q", code)
	}
}

func TestExtractHL7AckCode_NoMSA(t *testing.T) {
	data := []byte("MSH|^~\\&|test\rPID|1")
	code := extractHL7AckCode(data)
	if code != "" {
		t.Errorf("expected empty, got %q", code)
	}
}

// ---------------------------------------------------------------------------
// TCPDest: dial error, Send
// ---------------------------------------------------------------------------

func TestTCPDest_Send_DialError(t *testing.T) {
	dest := NewTCPDest("test", &config.TCPDestMapConfig{
		Host:      "127.0.0.1",
		Port:      1,
		TimeoutMs: 500,
	}, testLog())

	msg := message.New("ch1", []byte("data"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected returned error: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error in response")
	}
	if resp.StatusCode != 502 {
		t.Errorf("expected 502, got %d", resp.StatusCode)
	}
}

func TestTCPDest_Send_RawMode(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().(*net.TCPAddr)

	received := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		n, _ := conn.Read(buf)
		received <- buf[:n]
	}()

	dest := NewTCPDest("test", &config.TCPDestMapConfig{
		Host: "127.0.0.1",
		Port: addr.Port,
		Mode: "raw",
	}, testLog())

	msg := message.New("ch1", []byte("hello"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("send error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	select {
	case data := <-received:
		if !bytes.Equal(data, []byte("hello\n")) {
			t.Errorf("expected 'hello\\n', got %q", data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for data")
	}
}

func TestTCPDest_Send_MLLPMode(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().(*net.TCPAddr)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		conn.Read(buf)
		var ack bytes.Buffer
		ack.WriteByte(0x0B)
		ack.WriteString("MSH|^~\\&|ACK\rMSA|AA|123")
		ack.WriteByte(0x1C)
		ack.WriteByte(0x0D)
		conn.Write(ack.Bytes())
	}()

	dest := NewTCPDest("test", &config.TCPDestMapConfig{
		Host: "127.0.0.1",
		Port: addr.Port,
		Mode: "mllp",
	}, testLog())

	msg := message.New("ch1", []byte("MSH|^~\\&|test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("send error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestTCPDest_Send_MLLP_NACK(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().(*net.TCPAddr)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		conn.Read(buf)
		var ack bytes.Buffer
		ack.WriteByte(0x0B)
		ack.WriteString("MSH|^~\\&|ACK\rMSA|AE|123")
		ack.WriteByte(0x1C)
		ack.WriteByte(0x0D)
		conn.Write(ack.Bytes())
	}()

	dest := NewTCPDest("test", &config.TCPDestMapConfig{
		Host: "127.0.0.1",
		Port: addr.Port,
		Mode: "mllp",
	}, testLog())

	msg := message.New("ch1", []byte("MSH|^~\\&|test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("send error: %v", err)
	}
	if resp.StatusCode != 422 {
		t.Errorf("expected 422 for NACK, got %d", resp.StatusCode)
	}
	if resp.Error == nil {
		t.Error("expected error in response for NACK")
	}
}

func TestTCPDest_Type_Value(t *testing.T) {
	dest := NewTCPDest("test", &config.TCPDestMapConfig{Host: "localhost", Port: 9999}, testLog())
	if dest.Type() != "tcp" {
		t.Errorf("expected 'tcp', got %q", dest.Type())
	}
}

func TestTCPDest_Stop(t *testing.T) {
	dest := NewTCPDest("test", &config.TCPDestMapConfig{Host: "localhost", Port: 9999}, testLog())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("stop error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TCPSource: start/stop, MLLP, raw
// ---------------------------------------------------------------------------

func TestTCPSource_StartStop_Raw(t *testing.T) {
	src := NewTCPSource(&config.TCPListener{Port: 0, Mode: "raw", TimeoutMs: 1000}, testLog())

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	src.cfg.Port = port

	handler := func(ctx context.Context, msg *message.Message) error { return nil }
	if err := src.Start(context.Background(), handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	if src.Addr() == "" {
		t.Error("expected non-empty address")
	}
	if src.Type() != "tcp" {
		t.Errorf("expected type 'tcp', got %q", src.Type())
	}
}

func TestTCPSource_MLLP_ACK(t *testing.T) {
	src := NewTCPSource(&config.TCPListener{
		Port:      0,
		Mode:      "mllp",
		TimeoutMs: 2000,
		ACK:       &config.ACKConfig{Auto: true},
	}, testLog())

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	src.cfg.Port = port

	received := make(chan *message.Message, 1)
	handler := func(ctx context.Context, msg *message.Message) error {
		received <- msg
		return nil
	}
	if err := src.Start(context.Background(), handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	time.Sleep(50 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	var mllpMsg bytes.Buffer
	mllpMsg.WriteByte(0x0B)
	mllpMsg.WriteString("MSH|^~\\&|SENDER|FAC|RECV|FAC|20240101||ADT^A01|CTRL999|P|2.5\rPID|1||12345")
	mllpMsg.WriteByte(0x1C)
	mllpMsg.WriteByte(0x0D)
	conn.Write(mllpMsg.Bytes())

	select {
	case msg := <-received:
		if !bytes.Contains(msg.Raw, []byte("MSH")) {
			t.Error("expected MSH in received message")
		}
		if msg.Transport != "tcp" {
			t.Errorf("expected transport 'tcp', got %q", msg.Transport)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	ackBuf := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(ackBuf)
	if err != nil {
		t.Fatalf("read ACK: %v", err)
	}
	ack := ackBuf[:n]
	if !bytes.Contains(ack, []byte("MSA|AA")) {
		t.Errorf("expected ACK with AA code, got %q", ack)
	}
}

// ---------------------------------------------------------------------------
// FileSource: poll and moveFile
// ---------------------------------------------------------------------------

func TestFileSource_Poll(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "test1.hl7"), []byte("MSH|data1"), 0o644)
	os.WriteFile(filepath.Join(dir, "test2.hl7"), []byte("MSH|data2"), 0o644)
	os.WriteFile(filepath.Join(dir, "ignore.txt"), []byte("skip"), 0o644)

	src := NewFileSource(&config.FileListener{
		Directory:   dir,
		FilePattern: "*.hl7",
	}, testLog())

	received := make(chan *message.Message, 10)
	handler := func(ctx context.Context, msg *message.Message) error {
		received <- msg
		return nil
	}

	src.poll(context.Background(), handler)

	count := 0
	timeout := time.After(1 * time.Second)
loop:
	for {
		select {
		case <-received:
			count++
		case <-timeout:
			break loop
		default:
			if count >= 2 {
				break loop
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if count != 2 {
		t.Errorf("expected 2 messages, got %d", count)
	}
}

func TestFileSource_Poll_MoveTo(t *testing.T) {
	dir := t.TempDir()
	processedDir := filepath.Join(dir, "processed")

	os.WriteFile(filepath.Join(dir, "test.hl7"), []byte("MSH|data"), 0o644)

	src := NewFileSource(&config.FileListener{
		Directory:   dir,
		FilePattern: "*.hl7",
		MoveTo:      processedDir,
	}, testLog())

	handler := func(ctx context.Context, msg *message.Message) error { return nil }
	src.poll(context.Background(), handler)

	if _, err := os.Stat(filepath.Join(dir, "test.hl7")); !os.IsNotExist(err) {
		t.Error("expected file to be moved from source dir")
	}
	if _, err := os.Stat(filepath.Join(processedDir, "test.hl7")); err != nil {
		t.Errorf("expected file in processed dir: %v", err)
	}
}

func TestFileSource_Poll_ErrorDir(t *testing.T) {
	dir := t.TempDir()
	errorDir := filepath.Join(dir, "errors")

	os.WriteFile(filepath.Join(dir, "test.hl7"), []byte("MSH|data"), 0o644)

	src := NewFileSource(&config.FileListener{
		Directory:   dir,
		FilePattern: "*.hl7",
		ErrorDir:    errorDir,
	}, testLog())

	handler := func(ctx context.Context, msg *message.Message) error {
		return fmt.Errorf("test error")
	}
	src.poll(context.Background(), handler)

	if _, err := os.Stat(filepath.Join(errorDir, "test.hl7")); err != nil {
		t.Errorf("expected file in error dir: %v", err)
	}
}

func TestFileSource_MoveFile_CrossDevice(t *testing.T) {
	src := NewFileSource(&config.FileListener{Directory: "."}, testLog())

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "test.txt")
	dstFile := filepath.Join(dstDir, "test.txt")

	os.WriteFile(srcFile, []byte("content"), 0o644)
	src.moveFile(srcFile, dstFile)

	data, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("expected file at destination: %v", err)
	}
	if string(data) != "content" {
		t.Errorf("expected 'content', got %q", data)
	}
}

func TestFileSource_SortByName(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "b.hl7"), []byte("B"), 0o644)
	os.WriteFile(filepath.Join(dir, "a.hl7"), []byte("A"), 0o644)

	src := NewFileSource(&config.FileListener{
		Directory:   dir,
		FilePattern: "*.hl7",
		SortBy:      "name",
	}, testLog())

	var names []string
	handler := func(ctx context.Context, msg *message.Message) error {
		names = append(names, msg.Metadata["filename"].(string))
		return nil
	}
	src.poll(context.Background(), handler)

	if len(names) != 2 || names[0] != "a.hl7" || names[1] != "b.hl7" {
		t.Errorf("expected sorted by name [a.hl7, b.hl7], got %v", names)
	}
}

func TestFileSource_Type(t *testing.T) {
	src := NewFileSource(&config.FileListener{Scheme: "local"}, testLog())
	typ := src.Type()
	if typ != "file/local" {
		t.Errorf("expected 'file/local', got %q", typ)
	}
}

// ---------------------------------------------------------------------------
// FileDest: Send with patterns
// ---------------------------------------------------------------------------

func TestFileDest_Send_DefaultFilename(t *testing.T) {
	dir := t.TempDir()
	dest := NewFileDest("test", &config.FileDestMapConfig{Directory: dir}, testLog())

	msg := message.New("ch1", []byte("data"))
	msg.ChannelID = "my-channel"
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("send error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) == 0 {
		t.Fatal("expected file in output dir")
	}
}

func TestFileDest_Send_WithPattern_ChannelID(t *testing.T) {
	dir := t.TempDir()
	dest := NewFileDest("test", &config.FileDestMapConfig{
		Directory:       dir,
		FilenamePattern: "{{channelId}}_out.dat",
	}, testLog())

	msg := message.New("ch1", []byte("data"))
	msg.ChannelID = "test-channel"
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("send error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	path := filepath.Join(dir, "test-channel_out.dat")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected file: %v", err)
	}
	if string(data) != "data" {
		t.Errorf("expected 'data', got %q", data)
	}
}

func TestFileDest_Send_WithPattern_MessageID(t *testing.T) {
	dir := t.TempDir()
	dest := NewFileDest("test", &config.FileDestMapConfig{
		Directory:       dir,
		FilenamePattern: "{{messageId}}.dat",
	}, testLog())

	msg := message.New("ch1", []byte("data"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("send error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	path := filepath.Join(dir, msg.ID+".dat")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file named by message ID: %v", err)
	}
}

func TestFileDest_Send_TransportMeta(t *testing.T) {
	dir := t.TempDir()
	dest := NewFileDest("test", &config.FileDestMapConfig{
		Directory:       dir,
		FilenamePattern: "out.dat",
	}, testLog())

	msg := message.New("ch1", []byte("data"))
	msg.HTTP = &message.HTTPMeta{Method: "POST"}
	dest.Send(context.Background(), msg)

	if msg.Transport != "file" {
		t.Errorf("expected transport 'file', got %q", msg.Transport)
	}
	if msg.HTTP != nil {
		t.Error("HTTP meta should be cleared")
	}
	if msg.File == nil {
		t.Fatal("File meta should be set")
	}
}

func TestFileDest_Type_Value(t *testing.T) {
	dest := NewFileDest("test", &config.FileDestMapConfig{}, testLog())
	if dest.Type() != "file" {
		t.Errorf("expected 'file', got %q", dest.Type())
	}
}

func TestFileDest_Stop(t *testing.T) {
	dest := NewFileDest("test", &config.FileDestMapConfig{}, testLog())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("stop error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Shared HTTP listener
// ---------------------------------------------------------------------------

func TestPathRouterMax_RegisterAndServe(t *testing.T) {
	router := newPathRouter()
	if router.Len() != 0 {
		t.Errorf("expected 0, got %d", router.Len())
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})

	if err := router.Register("/test", handler); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if router.Len() != 1 {
		t.Errorf("expected 1, got %d", router.Len())
	}

	err := router.Register("/test", handler)
	if err == nil {
		t.Fatal("expected error for duplicate registration")
	}

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("expected 200, got %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/notfound", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Errorf("expected 404, got %d", w.Code)
	}

	router.Deregister("/test")
	if router.Len() != 0 {
		t.Errorf("expected 0 after deregister, got %d", router.Len())
	}
}

func TestSharedHTTPListenerMax_AcquireRelease(t *testing.T) {
	ResetSharedHTTPListeners()
	defer ResetSharedHTTPListeners()

	logger := testLog()
	sl, err := acquireSharedHTTPListener(0, nil, logger)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if sl == nil {
		t.Fatal("expected non-nil listener")
	}

	port := sl.port

	sl2, err := acquireSharedHTTPListener(port, nil, logger)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if sl2 != sl {
		t.Error("expected same listener on same port")
	}

	releaseSharedHTTPListener(port, context.Background())
	releaseSharedHTTPListener(port, context.Background())
}

// ---------------------------------------------------------------------------
// Factory: CreateSource / CreateDestination for all types (nil config error)
// ---------------------------------------------------------------------------

func TestFactory_CreateSource_AllTypes_NilConfig(t *testing.T) {
	f := NewFactory(testLog())

	types := []string{"http", "tcp", "file", "channel", "sftp", "database", "kafka", "email", "dicom", "soap", "fhir", "fhir_poll", "fhir_subscription", "ihe"}
	for _, typ := range types {
		_, err := f.CreateSource(config.ListenerConfig{Type: typ})
		if err == nil {
			t.Errorf("CreateSource(%s) with nil config should error", typ)
		}
	}
}

func TestFactory_CreateSource_Unsupported(t *testing.T) {
	f := NewFactory(testLog())
	_, err := f.CreateSource(config.ListenerConfig{Type: "unknown"})
	if err == nil {
		t.Error("expected error for unsupported type")
	}
}

func TestFactory_CreateDestination_AllTypes_NilConfig(t *testing.T) {
	f := NewFactory(testLog())

	types := []string{"http", "file", "channel", "tcp", "kafka", "database", "smtp", "dicom", "jms", "sftp", "fhir", "direct"}
	for _, typ := range types {
		_, err := f.CreateDestination("test", config.Destination{Type: typ})
		if err == nil {
			t.Errorf("CreateDestination(%s) with nil config should error", typ)
		}
	}
}

func TestFactory_CreateDestination_Unsupported(t *testing.T) {
	f := NewFactory(testLog())
	_, err := f.CreateDestination("test", config.Destination{Type: "unknown"})
	if err == nil {
		t.Error("expected error for unsupported type")
	}
}

func TestFactory_CreateDestination_Log(t *testing.T) {
	f := NewFactory(testLog())
	dest, err := f.CreateDestination("test", config.Destination{Type: "log"})
	if err != nil {
		t.Fatalf("CreateDestination(log) error: %v", err)
	}
	if dest == nil {
		t.Fatal("expected non-nil log destination")
	}
	if dest.Type() != "log" {
		t.Errorf("expected type 'log', got %q", dest.Type())
	}
}

func TestFactory_CreateSource_HTTP(t *testing.T) {
	f := NewFactory(testLog())
	src, err := f.CreateSource(config.ListenerConfig{
		Type: "http",
		HTTP: &config.HTTPListener{Port: 0},
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if src == nil {
		t.Fatal("expected non-nil source")
	}
}

func TestFactory_CreateSource_TCP(t *testing.T) {
	f := NewFactory(testLog())
	src, err := f.CreateSource(config.ListenerConfig{
		Type: "tcp",
		TCP:  &config.TCPListener{Port: 0},
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if src == nil {
		t.Fatal("expected non-nil source")
	}
}

func TestFactory_CreateSource_File(t *testing.T) {
	f := NewFactory(testLog())
	src, err := f.CreateSource(config.ListenerConfig{
		Type: "file",
		File: &config.FileListener{Directory: "/tmp"},
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if src == nil {
		t.Fatal("expected non-nil source")
	}
}

func TestFactory_CreateDestination_HTTP(t *testing.T) {
	f := NewFactory(testLog())
	dest, err := f.CreateDestination("test", config.Destination{
		Type: "http",
		HTTP: &config.HTTPDestConfig{URL: "http://example.com"},
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if dest == nil {
		t.Fatal("expected non-nil destination")
	}
}

func TestFactory_CreateDestination_File(t *testing.T) {
	f := NewFactory(testLog())
	dest, err := f.CreateDestination("test", config.Destination{
		Type: "file",
		File: &config.FileDestMapConfig{Directory: "/tmp"},
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if dest == nil {
		t.Fatal("expected non-nil destination")
	}
}

func TestFactory_CreateDestination_TCP(t *testing.T) {
	f := NewFactory(testLog())
	dest, err := f.CreateDestination("test", config.Destination{
		Type: "tcp",
		TCP:  &config.TCPDestMapConfig{Host: "localhost", Port: 9999},
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if dest == nil {
		t.Fatal("expected non-nil destination")
	}
}

func TestFactory_CreateDestination_Channel(t *testing.T) {
	f := NewFactory(testLog())
	dest, err := f.CreateDestination("test", config.Destination{
		Type:    "channel",
		Channel: &config.ChannelDestMapConfig{TargetChannelID: "ch-2"},
	})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if dest == nil {
		t.Fatal("expected non-nil destination")
	}
}
