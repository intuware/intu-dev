package connector

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
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

// ===================================================================
// HTTP Destination Tests
// ===================================================================

func TestHTTPDest_BasicSend(t *testing.T) {
	var received []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received, _ = io.ReadAll(r.Body)
		w.Header().Set("X-Custom", "resp-header")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{URL: server.URL, Method: "POST"}
	dest := NewHTTPDest("test-http", cfg, testLogger())

	msg := message.New("ch1", []byte(`{"key":"value"}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if string(received) != `{"key":"value"}` {
		t.Fatalf("expected body, got %q", string(received))
	}
	if resp.Headers["X-Custom"] != "resp-header" {
		t.Fatalf("expected response header, got %v", resp.Headers)
	}
}

func TestHTTPDest_DefaultMethod(t *testing.T) {
	var method string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{URL: server.URL}
	dest := NewHTTPDest("test-http", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	dest.Send(context.Background(), msg)

	if method != "POST" {
		t.Fatalf("expected POST as default method, got %s", method)
	}
}

func TestHTTPDest_CustomHeaders(t *testing.T) {
	var headers http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:     server.URL,
		Headers: map[string]string{"X-Channel": "ch1", "Content-Type": "text/xml"},
	}
	dest := NewHTTPDest("test-http", cfg, testLogger())

	msg := message.New("ch1", []byte("<data/>"))
	dest.Send(context.Background(), msg)

	if headers.Get("X-Channel") != "ch1" {
		t.Fatalf("expected X-Channel header")
	}
	if headers.Get("Content-Type") != "text/xml" {
		t.Fatalf("expected Content-Type text/xml, got %s", headers.Get("Content-Type"))
	}
}

func TestHTTPDest_DefaultContentType(t *testing.T) {
	var contentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{URL: server.URL}
	dest := NewHTTPDest("test-http", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	dest.Send(context.Background(), msg)

	if contentType != "application/json" {
		t.Fatalf("expected default content-type application/json, got %s", contentType)
	}
}

func TestHTTPDest_BearerAuth(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:  server.URL,
		Auth: &config.HTTPAuthConfig{Type: "bearer", Token: "my-token"},
	}
	dest := NewHTTPDest("test-http", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	dest.Send(context.Background(), msg)

	if authHeader != "Bearer my-token" {
		t.Fatalf("expected Bearer auth, got %q", authHeader)
	}
}

func TestHTTPDest_BasicAuth(t *testing.T) {
	var user, pass string
	var ok bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok = r.BasicAuth()
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:  server.URL,
		Auth: &config.HTTPAuthConfig{Type: "basic", Username: "admin", Password: "secret"},
	}
	dest := NewHTTPDest("test-http", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	dest.Send(context.Background(), msg)

	if !ok || user != "admin" || pass != "secret" {
		t.Fatalf("expected basic auth admin:secret, got %s:%s (ok=%v)", user, pass, ok)
	}
}

func TestHTTPDest_APIKeyHeader(t *testing.T) {
	var keyHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keyHeader = r.Header.Get("X-API-Key")
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:  server.URL,
		Auth: &config.HTTPAuthConfig{Type: "api_key", Key: "my-api-key", Header: "X-API-Key"},
	}
	dest := NewHTTPDest("test-http", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	dest.Send(context.Background(), msg)

	if keyHeader != "my-api-key" {
		t.Fatalf("expected API key header, got %q", keyHeader)
	}
}

func TestHTTPDest_APIKeyQueryParam(t *testing.T) {
	var queryKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queryKey = r.URL.Query().Get("api_key")
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:  server.URL,
		Auth: &config.HTTPAuthConfig{Type: "api_key", Key: "query-key-value", QueryParam: "api_key"},
	}
	dest := NewHTTPDest("test-http", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	dest.Send(context.Background(), msg)

	if queryKey != "query-key-value" {
		t.Fatalf("expected API key in query param, got %q", queryKey)
	}
}

func TestHTTPDest_OAuth2ClientCredentials(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.Form.Get("grant_type") != "client_credentials" {
			w.WriteHeader(400)
			return
		}
		if r.Form.Get("client_id") != "my-client" || r.Form.Get("client_secret") != "my-secret" {
			w.WriteHeader(401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "oauth-test-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	defer tokenServer.Close()

	var authHeader string
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	defer apiServer.Close()

	// Clear cache
	oauth2CacheMu.Lock()
	oauth2Cache = make(map[string]*oauth2CachedToken)
	oauth2CacheMu.Unlock()

	cfg := &config.HTTPDestConfig{
		URL: apiServer.URL,
		Auth: &config.HTTPAuthConfig{
			Type:         "oauth2_client_credentials",
			TokenURL:     tokenServer.URL,
			ClientID:     "my-client",
			ClientSecret: "my-secret",
			Scopes:       []string{"read", "write"},
		},
	}
	dest := NewHTTPDest("test-http-oauth", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if authHeader != "Bearer oauth-test-token" {
		t.Fatalf("expected OAuth2 bearer token, got %q", authHeader)
	}
}

func TestHTTPDest_NetworkError(t *testing.T) {
	cfg := &config.HTTPDestConfig{URL: "http://127.0.0.1:1", TimeoutMs: 1000}
	dest := NewHTTPDest("test-http", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected hard error: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error in response for connection failure")
	}
}

func TestHTTPDest_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{URL: server.URL}
	dest := NewHTTPDest("test-http", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 500 {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
	if string(resp.Body) != "internal error" {
		t.Fatalf("expected error body, got %q", string(resp.Body))
	}
}

func TestHTTPDest_TLS(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL: server.URL,
		TLS: &config.TLSMapConfig{
			Enabled:            true,
			InsecureSkipVerify: true,
		},
	}
	dest := NewHTTPDest("test-http-tls", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_Stop(t *testing.T) {
	cfg := &config.HTTPDestConfig{URL: "http://localhost"}
	dest := NewHTTPDest("test-http", cfg, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestHTTPDest_Type(t *testing.T) {
	cfg := &config.HTTPDestConfig{URL: "http://localhost"}
	dest := NewHTTPDest("test-http", cfg, testLogger())
	if dest.Type() != "http" {
		t.Fatalf("expected type 'http', got %q", dest.Type())
	}
}

func TestHTTPDest_TransportStamping(t *testing.T) {
	var receivedHeaders http.Header
	var receivedQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header
		receivedQuery = r.URL.RawQuery
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:         server.URL,
		Method:      "PUT",
		Headers:     map[string]string{"X-Dest": "custom"},
		QueryParams: map[string]string{"page": "1"},
		Auth:        &config.HTTPAuthConfig{Type: "bearer", Token: "tok123"},
	}
	dest := NewHTTPDest("test-http-stamp", cfg, testLogger())

	msg := message.New("ch1", []byte("body"))
	msg.Transport = "sftp"
	msg.FTP = &message.FTPMeta{Filename: "in.dat", Directory: "/upload"}

	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if msg.Transport != "http" {
		t.Fatalf("expected transport=http after Send, got %q", msg.Transport)
	}
	if msg.FTP != nil {
		t.Fatal("expected FTP meta to be cleared after Send")
	}
	if msg.HTTP == nil {
		t.Fatal("expected HTTP meta to be set after Send")
	}
	if msg.HTTP.Method != "PUT" {
		t.Fatalf("expected method=PUT, got %q", msg.HTTP.Method)
	}
	if msg.HTTP.Headers["X-Dest"] != "custom" {
		t.Fatalf("expected X-Dest header, got %v", msg.HTTP.Headers)
	}
	if msg.HTTP.Headers["Authorization"] != "Bearer tok123" {
		t.Fatalf("expected Authorization header from auth config, got %v", msg.HTTP.Headers["Authorization"])
	}
	if msg.HTTP.QueryParams["page"] != "1" {
		t.Fatalf("expected page=1 query param, got %v", msg.HTTP.QueryParams)
	}

	_ = receivedHeaders
	_ = receivedQuery
}

func TestHTTPDest_TransportStampingWithMessageHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:         server.URL,
		Headers:     map[string]string{"X-Config": "base"},
		QueryParams: map[string]string{"from": "config"},
	}
	dest := NewHTTPDest("test-http-merge", cfg, testLogger())

	msg := message.New("ch1", []byte("body"))
	msg.Transport = "tcp"
	msg.TCP = &message.TCPMeta{RemoteAddr: "1.2.3.4:9999"}
	msg.HTTP = &message.HTTPMeta{
		Headers:     map[string]string{"X-Msg": "override"},
		QueryParams: map[string]string{"from": "msg", "extra": "yes"},
	}

	_, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	if msg.Transport != "http" {
		t.Fatalf("expected transport=http, got %q", msg.Transport)
	}
	if msg.TCP != nil {
		t.Fatal("expected TCP meta to be cleared")
	}
	if msg.HTTP.Headers["X-Config"] != "base" {
		t.Fatalf("expected X-Config header from config, got %v", msg.HTTP.Headers)
	}
	if msg.HTTP.Headers["X-Msg"] != "override" {
		t.Fatalf("expected X-Msg header from message, got %v", msg.HTTP.Headers)
	}
	if msg.HTTP.QueryParams["from"] != "msg" {
		t.Fatalf("expected message query param to override config, got %v", msg.HTTP.QueryParams["from"])
	}
	if msg.HTTP.QueryParams["extra"] != "yes" {
		t.Fatalf("expected extra query param, got %v", msg.HTTP.QueryParams)
	}
	if msg.HTTP.Method != "POST" {
		t.Fatalf("expected default method=POST, got %q", msg.HTTP.Method)
	}
}

// ===================================================================
// File Destination Tests
// ===================================================================

func TestFileDest_BasicWrite(t *testing.T) {
	dir := t.TempDir()

	cfg := &config.FileDestMapConfig{Directory: dir}
	dest := NewFileDest("test-file", cfg, testLogger())

	msg := message.New("ch1", []byte("file-content"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("expected 1 file, got %d", len(entries))
	}

	data, _ := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if string(data) != "file-content" {
		t.Fatalf("expected 'file-content', got %q", string(data))
	}
}

func TestFileDest_FilenamePattern(t *testing.T) {
	dir := t.TempDir()

	cfg := &config.FileDestMapConfig{
		Directory:       dir,
		FilenamePattern: "{{channelId}}_{{messageId}}.dat",
	}
	dest := NewFileDest("test-file", cfg, testLogger())

	msg := message.New("my-channel", []byte("patterned"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	path := string(resp.Body)
	if !strings.Contains(path, "my-channel_") {
		t.Fatalf("expected filename with channel ID, got %q", path)
	}

	data, _ := os.ReadFile(path)
	if string(data) != "patterned" {
		t.Fatalf("expected 'patterned', got %q", string(data))
	}
}

func TestFileDest_CreateDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub", "dir")

	cfg := &config.FileDestMapConfig{Directory: dir}
	dest := NewFileDest("test-file", cfg, testLogger())

	msg := message.New("ch1", []byte("nested"))
	_, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("expected 1 file, got %d", len(entries))
	}
}

func TestFileDest_Type(t *testing.T) {
	cfg := &config.FileDestMapConfig{Directory: "/tmp"}
	dest := NewFileDest("test-file", cfg, testLogger())
	if dest.Type() != "file" {
		t.Fatalf("expected type 'file', got %q", dest.Type())
	}
}

// ===================================================================
// Channel Destination Tests
// ===================================================================

func TestChannelDest_PublishToBus(t *testing.T) {
	targetCh := "dest-target-ch-" + fmt.Sprintf("%d", time.Now().UnixNano())
	dest := NewChannelDest("test-channel-dest", targetCh, testLogger())

	bus := GetChannelBus()
	sub := bus.Subscribe(targetCh)

	msg := message.New("source-ch", []byte("channel-msg"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	select {
	case received := <-sub:
		if string(received.Raw) != "channel-msg" {
			t.Fatalf("expected 'channel-msg', got %q", string(received.Raw))
		}
		if received.ChannelID != targetCh {
			t.Fatalf("expected channel ID %q, got %q", targetCh, received.ChannelID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for message on channel bus")
	}
}

func TestChannelDest_MessageClone(t *testing.T) {
	targetCh := "clone-test-ch-" + fmt.Sprintf("%d", time.Now().UnixNano())
	dest := NewChannelDest("test-channel-dest", targetCh, testLogger())

	bus := GetChannelBus()
	sub := bus.Subscribe(targetCh)

	msg := message.New("source-ch", []byte("original"))
	msg.EnsureHTTP().Headers["key"] = "value"
	msg.Metadata["meta"] = "data"

	dest.Send(context.Background(), msg)

	select {
	case received := <-sub:
		// Modify the original to verify the clone is independent
		msg.Raw = []byte("modified")
		if string(received.Raw) != "original" {
			t.Fatal("message was not properly cloned")
		}
		if received.HTTP == nil || received.HTTP.Headers["key"] != "value" {
			t.Fatal("HTTP meta was not cloned")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout")
	}
}

func TestChannelDest_Type(t *testing.T) {
	dest := NewChannelDest("test", "target", testLogger())
	if dest.Type() != "channel" {
		t.Fatalf("expected type 'channel', got %q", dest.Type())
	}
}

// ===================================================================
// Log Destination Tests
// ===================================================================

func TestLogDest_Send(t *testing.T) {
	dest := NewLogDest("test-log", testLogger())

	msg := message.New("ch1", []byte("log-this"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if string(resp.Body) != `{"status":"logged"}` {
		t.Fatalf("unexpected body: %q", string(resp.Body))
	}
}

func TestLogDest_Type(t *testing.T) {
	dest := NewLogDest("test-log", testLogger())
	if dest.Type() != "log" {
		t.Fatalf("expected type 'log', got %q", dest.Type())
	}
}

func TestLogDest_Stop(t *testing.T) {
	dest := NewLogDest("test-log", testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ===================================================================
// TCP Destination Tests
// ===================================================================

func TestTCPDest_RawSend(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	var received []byte
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		received, _ = io.ReadAll(conn)
	}()

	cfg := &config.TCPDestMapConfig{Host: host, Port: port, Mode: "raw", TimeoutMs: 5000}
	dest := NewTCPDest("test-tcp", cfg, testLogger())

	msg := message.New("ch1", []byte("hello-tcp"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected response error: %v", resp.Error)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	dest.Stop(context.Background())

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for server")
	}

	if !strings.Contains(string(received), "hello-tcp") {
		t.Fatalf("expected 'hello-tcp' in received data, got %q", string(received))
	}
}

func TestTCPDest_MLLPSend(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	hl7msg := "MSH|^~\\&|SEND|FAC|RECV|FAC|20230101||ADT^A01|99999|P|2.5\rPID|1||MRN123\r"

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read MLLP frame
		buf := make([]byte, 4096)
		n, _ := conn.Read(buf)
		data := buf[:n]

		// Verify MLLP framing
		if data[0] != 0x0B {
			return
		}
		if data[len(data)-2] != 0x1C || data[len(data)-1] != 0x0D {
			return
		}

		// Send ACK
		ack := "MSH|^~\\&|RECV|FAC|SEND|FAC|20230101||ACK||P|2.5\rMSA|AA|99999\r"
		var ackBuf bytes.Buffer
		ackBuf.WriteByte(0x0B)
		ackBuf.WriteString(ack)
		ackBuf.WriteByte(0x1C)
		ackBuf.WriteByte(0x0D)
		conn.Write(ackBuf.Bytes())
	}()

	cfg := &config.TCPDestMapConfig{Host: host, Port: port, Mode: "mllp", TimeoutMs: 5000}
	dest := NewTCPDest("test-tcp-mllp", cfg, testLogger())

	msg := message.New("ch1", []byte(hl7msg))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected response error: %v", resp.Error)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify ACK body
	if !strings.Contains(string(resp.Body), "MSA|AA|99999") {
		t.Fatalf("expected ACK with MSA|AA|99999, got %q", string(resp.Body))
	}

	dest.Stop(context.Background())
	<-done
}

func TestTCPDest_MLLPNack(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send NACK
		ack := "MSH|^~\\&|RECV|FAC|SEND|FAC|20230101||ACK||P|2.5\rMSA|AE|12345\r"
		var ackBuf bytes.Buffer
		ackBuf.WriteByte(0x0B)
		ackBuf.WriteString(ack)
		ackBuf.WriteByte(0x1C)
		ackBuf.WriteByte(0x0D)
		conn.Write(ackBuf.Bytes())
	}()

	cfg := &config.TCPDestMapConfig{Host: host, Port: port, Mode: "mllp", TimeoutMs: 5000}
	dest := NewTCPDest("test-tcp-nack", cfg, testLogger())

	msg := message.New("ch1", []byte("MSH|^~\\&|S|F|R|F|20230101||ADT^A01|12345|P|2.5\r"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error for NACK response")
	}
	if resp.StatusCode != 422 {
		t.Fatalf("expected 422, got %d", resp.StatusCode)
	}

	dest.Stop(context.Background())
	<-done
}

func TestTCPDest_ConnectionRefused(t *testing.T) {
	cfg := &config.TCPDestMapConfig{Host: "127.0.0.1", Port: 1, Mode: "raw", TimeoutMs: 1000}
	dest := NewTCPDest("test-tcp-refused", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected hard error: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error for refused connection")
	}
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502, got %d", resp.StatusCode)
	}
}

func TestTCPDest_TLS(t *testing.T) {
	certFile, keyFile := generateTestCerts(t)

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("load cert: %v", err)
	}

	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	done := make(chan []byte)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- nil
			return
		}
		defer conn.Close()
		data, _ := io.ReadAll(conn)
		done <- data
	}()

	cfg := &config.TCPDestMapConfig{
		Host:      host,
		Port:      port,
		Mode:      "raw",
		TimeoutMs: 5000,
		TLS:       &config.TLSMapConfig{Enabled: true, InsecureSkipVerify: true},
	}
	dest := NewTCPDest("test-tcp-tls", cfg, testLogger())

	msg := message.New("ch1", []byte("tls-data"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	dest.Stop(context.Background())

	received := <-done
	if !strings.Contains(string(received), "tls-data") {
		t.Fatalf("expected 'tls-data', got %q", string(received))
	}
}

func TestTCPDest_Type(t *testing.T) {
	cfg := &config.TCPDestMapConfig{Host: "localhost", Port: 1234}
	dest := NewTCPDest("test-tcp", cfg, testLogger())
	if dest.Type() != "tcp" {
		t.Fatalf("expected type 'tcp', got %q", dest.Type())
	}
}

// ===================================================================
// DICOM Destination Tests
// ===================================================================

func TestDICOMDest_SendAndReceiveAC(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	var receivedPData []byte
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read A-ASSOCIATE-RQ
		header := make([]byte, 6)
		io.ReadFull(conn, header)
		if header[0] != 0x01 {
			return
		}
		pduLen := binary.BigEndian.Uint32(header[2:6])
		body := make([]byte, pduLen)
		io.ReadFull(conn, body)

		// Send A-ASSOCIATE-AC
		var acData []byte
		acData = append(acData, 0x00, 0x01)
		acData = append(acData, 0x00, 0x00)
		acData = append(acData, []byte(fmt.Sprintf("%-16s", "TESTSCP"))...)
		acData = append(acData, []byte(fmt.Sprintf("%-16s", "TESTSCU"))...)
		acData = append(acData, make([]byte, 32)...)

		acPDU := make([]byte, 6+len(acData))
		acPDU[0] = 0x02
		binary.BigEndian.PutUint32(acPDU[2:6], uint32(len(acData)))
		copy(acPDU[6:], acData)
		conn.Write(acPDU)

		// Read P-DATA-TF
		io.ReadFull(conn, header)
		if header[0] == 0x04 {
			dataLen := binary.BigEndian.Uint32(header[2:6])
			receivedPData = make([]byte, dataLen)
			io.ReadFull(conn, receivedPData)
		}

		// Read A-RELEASE-RQ
		io.ReadFull(conn, header)
		if header[0] == 0x05 {
			relLen := binary.BigEndian.Uint32(header[2:6])
			if relLen > 0 {
				relBody := make([]byte, relLen)
				io.ReadFull(conn, relBody)
			}
			// Send A-RELEASE-RP
			rpPDU := make([]byte, 10)
			rpPDU[0] = 0x06
			binary.BigEndian.PutUint32(rpPDU[2:6], 4)
			conn.Write(rpPDU)
		}
	}()

	cfg := &config.DICOMDestMapConfig{
		Host:          host,
		Port:          port,
		AETitle:       "TESTSCU",
		CalledAETitle: "TESTSCP",
		TimeoutMs:     5000,
	}
	dest := NewDICOMDest("test-dicom", cfg, testLogger())

	msg := message.New("ch1", []byte("DICOM-PAYLOAD"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	<-done

	if string(receivedPData) != "DICOM-PAYLOAD" {
		t.Fatalf("expected DICOM-PAYLOAD, got %q", string(receivedPData))
	}
}

func TestDICOMDest_AssociationRejected(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		header := make([]byte, 6)
		io.ReadFull(conn, header)
		pduLen := binary.BigEndian.Uint32(header[2:6])
		body := make([]byte, pduLen)
		io.ReadFull(conn, body)

		// Send A-ASSOCIATE-RJ
		rjPDU := make([]byte, 10)
		rjPDU[0] = 0x03
		binary.BigEndian.PutUint32(rjPDU[2:6], 4)
		rjPDU[7] = 0x01
		rjPDU[8] = 0x01
		rjPDU[9] = 0x03
		conn.Write(rjPDU)
	}()

	cfg := &config.DICOMDestMapConfig{Host: host, Port: port, TimeoutMs: 5000}
	dest := NewDICOMDest("test-dicom-rj", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error for rejected association")
	}
	if resp.StatusCode != 403 {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}

	<-done
}

func TestDICOMDest_ConnectionFailed(t *testing.T) {
	cfg := &config.DICOMDestMapConfig{Host: "127.0.0.1", Port: 1, TimeoutMs: 1000}
	dest := NewDICOMDest("test-dicom-fail", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected hard error: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error")
	}
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502, got %d", resp.StatusCode)
	}
}

func TestDICOMDest_Type(t *testing.T) {
	cfg := &config.DICOMDestMapConfig{Host: "localhost", Port: 104}
	dest := NewDICOMDest("test-dicom", cfg, testLogger())
	if dest.Type() != "dicom" {
		t.Fatalf("expected type 'dicom', got %q", dest.Type())
	}
}

// ===================================================================
// JMS Destination Tests
// ===================================================================

func TestJMSDest_BasicSend(t *testing.T) {
	var receivedBody []byte
	var receivedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
		w.Write([]byte(`{"status":"enqueued"}`))
	}))
	defer server.Close()

	cfg := &config.JMSDestMapConfig{
		Provider:  "activemq",
		URL:       server.URL,
		Queue:     "test-queue",
		TimeoutMs: 5000,
	}
	dest := NewJMSDest("test-jms", cfg, testLogger())

	msg := message.New("ch1", []byte(`{"event":"test"}`))
	msg.ContentType = message.ContentTypeJSON
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if !strings.Contains(receivedPath, "test-queue") {
		t.Fatalf("expected queue in URL path, got %q", receivedPath)
	}
	if string(receivedBody) != `{"event":"test"}` {
		t.Fatalf("expected body, got %q", string(receivedBody))
	}
}

func TestJMSDest_BearerAuth(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.JMSDestMapConfig{
		URL:   server.URL,
		Queue: "q",
		Auth:  &config.HTTPAuthConfig{Type: "bearer", Token: "jms-token"},
	}
	dest := NewJMSDest("test-jms-auth", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	dest.Send(context.Background(), msg)

	if authHeader != "Bearer jms-token" {
		t.Fatalf("expected Bearer auth, got %q", authHeader)
	}
}

func TestJMSDest_BasicAuth(t *testing.T) {
	var user, pass string
	var ok bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok = r.BasicAuth()
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.JMSDestMapConfig{
		URL:   server.URL,
		Queue: "q",
		Auth:  &config.HTTPAuthConfig{Type: "basic", Username: "jms-user", Password: "jms-pass"},
	}
	dest := NewJMSDest("test-jms-basic", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	dest.Send(context.Background(), msg)

	if !ok || user != "jms-user" || pass != "jms-pass" {
		t.Fatalf("expected basic auth jms-user:jms-pass, got %s:%s", user, pass)
	}
}

func TestJMSDest_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("queue full"))
	}))
	defer server.Close()

	cfg := &config.JMSDestMapConfig{URL: server.URL, Queue: "q"}
	dest := NewJMSDest("test-jms-err", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error for 500 response")
	}
	if resp.StatusCode != 500 {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
}

func TestJMSDest_MissingConfig(t *testing.T) {
	dest := NewJMSDest("test-jms-nourl", &config.JMSDestMapConfig{Queue: "q"}, testLogger())
	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 for missing URL, got %d", resp.StatusCode)
	}

	dest2 := NewJMSDest("test-jms-noq", &config.JMSDestMapConfig{URL: "http://localhost"}, testLogger())
	resp2, _ := dest2.Send(context.Background(), msg)
	if resp2.StatusCode != 400 {
		t.Fatalf("expected 400 for missing queue, got %d", resp2.StatusCode)
	}
}

func TestJMSDest_Type(t *testing.T) {
	cfg := &config.JMSDestMapConfig{URL: "http://localhost", Queue: "q"}
	dest := NewJMSDest("test-jms", cfg, testLogger())
	if dest.Type() != "jms" {
		t.Fatalf("expected type 'jms', got %q", dest.Type())
	}
}

// ===================================================================
// FHIR Destination Tests
// ===================================================================

func TestFHIRDest_CreateResource(t *testing.T) {
	var receivedPath, receivedMethod string
	var receivedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		receivedMethod = r.Method
		receivedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/fhir+json")
		w.WriteHeader(201)
		w.Write([]byte(`{"resourceType":"Patient","id":"123"}`))
	}))
	defer server.Close()

	cfg := &config.FHIRDestMapConfig{
		BaseURL:    server.URL + "/fhir",
		Version:    "R4",
		Operations: []string{"create"},
	}
	dest := NewFHIRDest("test-fhir", cfg, testLogger())

	patient := `{"resourceType":"Patient","name":[{"family":"Doe"}]}`
	msg := message.New("ch1", []byte(patient))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 201 {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	if receivedMethod != "POST" {
		t.Fatalf("expected POST, got %s", receivedMethod)
	}
	if receivedPath != "/fhir/Patient" {
		t.Fatalf("expected /fhir/Patient, got %s", receivedPath)
	}
	if string(receivedBody) != patient {
		t.Fatalf("body mismatch")
	}
}

func TestFHIRDest_UpdateResource(t *testing.T) {
	var receivedPath, receivedMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		receivedMethod = r.Method
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.FHIRDestMapConfig{BaseURL: server.URL + "/fhir"}
	dest := NewFHIRDest("test-fhir-update", cfg, testLogger())

	msg := message.New("ch1", []byte(`{"resourceType":"Patient","id":"456","name":[{"family":"Smith"}]}`))
	msg.Metadata["fhir_operation"] = "update"
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if receivedMethod != "PUT" {
		t.Fatalf("expected PUT, got %s", receivedMethod)
	}
	if receivedPath != "/fhir/Patient/456" {
		t.Fatalf("expected /fhir/Patient/456, got %s", receivedPath)
	}
}

func TestFHIRDest_BearerAuth(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(201)
	}))
	defer server.Close()

	cfg := &config.FHIRDestMapConfig{
		BaseURL: server.URL + "/fhir",
		Auth:    &config.HTTPAuthConfig{Type: "bearer", Token: "fhir-token"},
	}
	dest := NewFHIRDest("test-fhir-auth", cfg, testLogger())

	msg := message.New("ch1", []byte(`{"resourceType":"Observation"}`))
	dest.Send(context.Background(), msg)

	if authHeader != "Bearer fhir-token" {
		t.Fatalf("expected Bearer auth, got %q", authHeader)
	}
}

func TestFHIRDest_BasicAuth(t *testing.T) {
	var user, pass string
	var ok bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok = r.BasicAuth()
		w.WriteHeader(201)
	}))
	defer server.Close()

	cfg := &config.FHIRDestMapConfig{
		BaseURL: server.URL + "/fhir",
		Auth:    &config.HTTPAuthConfig{Type: "basic", Username: "fhir-user", Password: "fhir-pass"},
	}
	dest := NewFHIRDest("test-fhir-basic", cfg, testLogger())

	msg := message.New("ch1", []byte(`{"resourceType":"Patient"}`))
	dest.Send(context.Background(), msg)

	if !ok || user != "fhir-user" || pass != "fhir-pass" {
		t.Fatalf("expected basic auth fhir-user:fhir-pass")
	}
}

func TestFHIRDest_OAuth2(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "fhir-oauth-token",
			"expires_in":   3600,
		})
	}))
	defer tokenServer.Close()

	var authHeader string
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(201)
	}))
	defer apiServer.Close()

	// Clear cache
	oauth2CacheMu.Lock()
	oauth2Cache = make(map[string]*oauth2CachedToken)
	oauth2CacheMu.Unlock()

	cfg := &config.FHIRDestMapConfig{
		BaseURL: apiServer.URL + "/fhir",
		Auth: &config.HTTPAuthConfig{
			Type:         "oauth2_client_credentials",
			TokenURL:     tokenServer.URL,
			ClientID:     "fhir-client",
			ClientSecret: "fhir-secret",
		},
	}
	dest := NewFHIRDest("test-fhir-oauth", cfg, testLogger())

	msg := message.New("ch1", []byte(`{"resourceType":"Patient"}`))
	dest.Send(context.Background(), msg)

	if authHeader != "Bearer fhir-oauth-token" {
		t.Fatalf("expected OAuth2 token, got %q", authHeader)
	}
}

func TestFHIRDest_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(422)
		w.Write([]byte(`{"resourceType":"OperationOutcome","issue":[{"severity":"error"}]}`))
	}))
	defer server.Close()

	cfg := &config.FHIRDestMapConfig{BaseURL: server.URL + "/fhir"}
	dest := NewFHIRDest("test-fhir-err", cfg, testLogger())

	msg := message.New("ch1", []byte(`{"resourceType":"Patient"}`))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.StatusCode != 422 {
		t.Fatalf("expected 422, got %d", resp.StatusCode)
	}
	if resp.Error == nil {
		t.Fatal("expected error")
	}
}

func TestFHIRDest_MissingBaseURL(t *testing.T) {
	dest := NewFHIRDest("test-fhir-nourl", &config.FHIRDestMapConfig{}, testLogger())
	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestFHIRDest_TransactionBundle(t *testing.T) {
	var receivedPath, receivedMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		receivedMethod = r.Method
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.FHIRDestMapConfig{BaseURL: server.URL + "/fhir"}
	dest := NewFHIRDest("test-fhir-tx", cfg, testLogger())

	bundle := `{"resourceType":"Bundle","type":"transaction","entry":[]}`
	msg := message.New("ch1", []byte(bundle))
	msg.Metadata["fhir_operation"] = "transaction"
	dest.Send(context.Background(), msg)

	if receivedMethod != "POST" {
		t.Fatalf("expected POST for transaction, got %s", receivedMethod)
	}
	if receivedPath != "/fhir" {
		t.Fatalf("expected /fhir for transaction, got %s", receivedPath)
	}
}

func TestFHIRDest_Type(t *testing.T) {
	cfg := &config.FHIRDestMapConfig{BaseURL: "http://localhost/fhir"}
	dest := NewFHIRDest("test-fhir", cfg, testLogger())
	if dest.Type() != "fhir" {
		t.Fatalf("expected type 'fhir', got %q", dest.Type())
	}
}

// ===================================================================
// Database Destination Tests
// ===================================================================

func TestDatabaseDest_MissingStatement(t *testing.T) {
	cfg := &config.DBDestMapConfig{Driver: "sqlite3", DSN: ":memory:"}
	dest := NewDatabaseDest("test-db-nostmt", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 for missing statement, got %d", resp.StatusCode)
	}
	if resp.Error == nil {
		t.Fatal("expected error for missing statement")
	}
}

func TestDatabaseDest_Type(t *testing.T) {
	cfg := &config.DBDestMapConfig{Driver: "sqlite3", DSN: ":memory:"}
	dest := NewDatabaseDest("test-db", cfg, testLogger())
	if dest.Type() != "database" {
		t.Fatalf("expected type 'database', got %q", dest.Type())
	}
}

func TestDatabaseDest_Stop(t *testing.T) {
	cfg := &config.DBDestMapConfig{Driver: "sqlite3", DSN: ":memory:"}
	dest := NewDatabaseDest("test-db", cfg, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestDatabaseDest_DriverMapping(t *testing.T) {
	tests := []struct {
		driver   string
		expected string
	}{
		{"postgres", "pgx"},
		{"postgresql", "pgx"},
		{"mysql", "mysql"},
		{"mssql", "sqlserver"},
		{"sqlserver", "sqlserver"},
		{"sqlite", "sqlite"},
		{"sqlite3", "sqlite"},
		{"custom", "custom"},
	}

	for _, tt := range tests {
		cfg := &config.DBDestMapConfig{Driver: tt.driver, DSN: "test"}
		dest := NewDatabaseDest("test", cfg, testLogger())
		if dest.driverName() != tt.expected {
			t.Errorf("driverName(%q) = %q, expected %q", tt.driver, dest.driverName(), tt.expected)
		}
	}
}

// ===================================================================
// SMTP Destination Tests
// ===================================================================

func TestSMTPDest_MissingConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.SMTPDestMapConfig
	}{
		{"no host", &config.SMTPDestMapConfig{From: "a@b.com", To: []string{"c@d.com"}}},
		{"no from", &config.SMTPDestMapConfig{Host: "localhost", To: []string{"c@d.com"}}},
		{"no to", &config.SMTPDestMapConfig{Host: "localhost", From: "a@b.com"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := NewSMTPDest("test-smtp", tt.cfg, testLogger())
			msg := message.New("ch1", []byte("test"))
			resp, _ := dest.Send(context.Background(), msg)
			if resp.StatusCode != 400 {
				t.Fatalf("expected 400 for %s, got %d", tt.name, resp.StatusCode)
			}
		})
	}
}

func TestSMTPDest_ConnectionFailed(t *testing.T) {
	cfg := &config.SMTPDestMapConfig{
		Host: "127.0.0.1",
		Port: 1,
		From: "test@example.com",
		To:   []string{"dest@example.com"},
	}
	dest := NewSMTPDest("test-smtp-fail", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.Error == nil {
		t.Fatal("expected error for connection failure")
	}
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502, got %d", resp.StatusCode)
	}
}

func TestSMTPDest_Type(t *testing.T) {
	cfg := &config.SMTPDestMapConfig{Host: "localhost"}
	dest := NewSMTPDest("test-smtp", cfg, testLogger())
	if dest.Type() != "smtp" {
		t.Fatalf("expected type 'smtp', got %q", dest.Type())
	}
}

// ===================================================================
// Direct Destination Tests
// ===================================================================

func TestDirectDest_MissingConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.DirectDestMapConfig
	}{
		{"no to", &config.DirectDestMapConfig{From: "a@direct.example.com"}},
		{"no from", &config.DirectDestMapConfig{To: "b@direct.example.com"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := NewDirectDest("test-direct", tt.cfg, testLogger())
			msg := message.New("ch1", []byte("test"))
			resp, _ := dest.Send(context.Background(), msg)
			if resp.StatusCode != 400 {
				t.Fatalf("expected 400, got %d", resp.StatusCode)
			}
		})
	}
}

func TestDirectDest_ConnectionFailed(t *testing.T) {
	cfg := &config.DirectDestMapConfig{
		To:       "dest@direct.example.com",
		From:     "src@direct.example.com",
		SMTPHost: "127.0.0.1",
		SMTPPort: 1,
	}
	dest := NewDirectDest("test-direct-fail", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.Error == nil {
		t.Fatal("expected error for connection failure")
	}
}

func TestDirectDest_Type(t *testing.T) {
	cfg := &config.DirectDestMapConfig{To: "a@b.com", From: "c@d.com"}
	dest := NewDirectDest("test-direct", cfg, testLogger())
	if dest.Type() != "direct" {
		t.Fatalf("expected type 'direct', got %q", dest.Type())
	}
}

// ===================================================================
// Kafka Destination Tests
// ===================================================================

func TestKafkaDest_ConnectionFailed(t *testing.T) {
	cfg := &config.KafkaDestConfig{
		Brokers: []string{"127.0.0.1:1"},
		Topic:   "test-topic",
	}
	dest := NewKafkaDest("test-kafka-fail", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected hard error: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error for connection failure")
	}
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502, got %d", resp.StatusCode)
	}
}

func TestKafkaDest_NoBrokers(t *testing.T) {
	cfg := &config.KafkaDestConfig{Topic: "test-topic"}
	dest := NewKafkaDest("test-kafka-nobroker", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.Error == nil {
		t.Fatal("expected error for no brokers")
	}
}

func TestKafkaDest_Type(t *testing.T) {
	cfg := &config.KafkaDestConfig{Brokers: []string{"localhost:9092"}, Topic: "t"}
	dest := NewKafkaDest("test-kafka", cfg, testLogger())
	if dest.Type() != "kafka" {
		t.Fatalf("expected type 'kafka', got %q", dest.Type())
	}
}

func TestKafkaDest_ClientID(t *testing.T) {
	cfg := &config.KafkaDestConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "t",
		ClientID: "custom-client",
	}
	dest := NewKafkaDest("test-kafka", cfg, testLogger())
	if dest.clientID() != "custom-client" {
		t.Fatalf("expected custom client ID, got %q", dest.clientID())
	}

	cfg2 := &config.KafkaDestConfig{Brokers: []string{"localhost:9092"}, Topic: "t"}
	dest2 := NewKafkaDest("test-kafka", cfg2, testLogger())
	if dest2.clientID() != "intu-kafka-dest" {
		t.Fatalf("expected default client ID, got %q", dest2.clientID())
	}
}

func TestKafkaDest_Stop(t *testing.T) {
	cfg := &config.KafkaDestConfig{Brokers: []string{"localhost:9092"}, Topic: "t"}
	dest := NewKafkaDest("test-kafka", cfg, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ===================================================================
// Factory Destination Tests
// ===================================================================

func TestFactory_CreateAllDestinationTypes(t *testing.T) {
	factory := NewFactory(testLogger())

	tests := []struct {
		name     string
		dest     config.Destination
		wantType string
	}{
		{
			name:     "http",
			dest:     config.Destination{Type: "http", HTTP: &config.HTTPDestConfig{URL: "http://example.com"}},
			wantType: "http",
		},
		{
			name:     "file",
			dest:     config.Destination{Type: "file", File: &config.FileDestMapConfig{Directory: "/tmp"}},
			wantType: "file",
		},
		{
			name:     "channel",
			dest:     config.Destination{Type: "channel", Channel: &config.ChannelDestMapConfig{TargetChannelID: "ch-1"}},
			wantType: "channel",
		},
		{
			name:     "tcp",
			dest:     config.Destination{Type: "tcp", TCP: &config.TCPDestMapConfig{Host: "localhost", Port: 1234}},
			wantType: "tcp",
		},
		{
			name:     "kafka",
			dest:     config.Destination{Type: "kafka", Kafka: &config.KafkaDestConfig{Brokers: []string{"localhost:9092"}, Topic: "t"}},
			wantType: "kafka",
		},
		{
			name:     "database",
			dest:     config.Destination{Type: "database", Database: &config.DBDestMapConfig{Driver: "sqlite3", DSN: ":memory:"}},
			wantType: "database",
		},
		{
			name:     "smtp",
			dest:     config.Destination{Type: "smtp", SMTP: &config.SMTPDestMapConfig{Host: "localhost"}},
			wantType: "smtp",
		},
		{
			name:     "dicom",
			dest:     config.Destination{Type: "dicom", DICOM: &config.DICOMDestMapConfig{Host: "localhost", Port: 104}},
			wantType: "dicom",
		},
		{
			name:     "jms",
			dest:     config.Destination{Type: "jms", JMS: &config.JMSDestMapConfig{URL: "http://localhost", Queue: "q"}},
			wantType: "jms",
		},
		{
			name:     "fhir",
			dest:     config.Destination{Type: "fhir", FHIR: &config.FHIRDestMapConfig{BaseURL: "http://localhost/fhir"}},
			wantType: "fhir",
		},
		{
			name:     "direct",
			dest:     config.Destination{Type: "direct", Direct: &config.DirectDestMapConfig{To: "a@b.com", From: "c@d.com"}},
			wantType: "direct",
		},
		{
			name:     "log",
			dest:     config.Destination{Type: "log"},
			wantType: "log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest, err := factory.CreateDestination(tt.name, tt.dest)
			if err != nil {
				t.Fatalf("CreateDestination(%s): %v", tt.name, err)
			}
			if dest == nil {
				t.Fatalf("CreateDestination(%s) returned nil", tt.name)
			}
			if dest.Type() != tt.wantType {
				t.Fatalf("expected type %q, got %q", tt.wantType, dest.Type())
			}
		})
	}
}

func TestFactory_DestinationNilConfig(t *testing.T) {
	factory := NewFactory(testLogger())

	nilConfigTests := []struct {
		name string
		dest config.Destination
	}{
		{"http_nil", config.Destination{Type: "http"}},
		{"file_nil", config.Destination{Type: "file"}},
		{"channel_nil", config.Destination{Type: "channel"}},
		{"tcp_nil", config.Destination{Type: "tcp"}},
		{"kafka_nil", config.Destination{Type: "kafka"}},
		{"database_nil", config.Destination{Type: "database"}},
		{"smtp_nil", config.Destination{Type: "smtp"}},
		{"dicom_nil", config.Destination{Type: "dicom"}},
		{"jms_nil", config.Destination{Type: "jms"}},
		{"fhir_nil", config.Destination{Type: "fhir"}},
		{"direct_nil", config.Destination{Type: "direct"}},
	}

	for _, tt := range nilConfigTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := factory.CreateDestination(tt.name, tt.dest)
			if err == nil {
				t.Fatalf("expected error for nil config on %s", tt.name)
			}
		})
	}
}

func TestFactory_DestinationUnsupportedType(t *testing.T) {
	factory := NewFactory(testLogger())
	_, err := factory.CreateDestination("test", config.Destination{Type: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for unsupported destination type")
	}
}

// ===================================================================
// Named Destination (Config Reference) Tests
// ===================================================================

func TestNamedDestination_HTTPWithAuth(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	defer server.Close()

	destinations := map[string]config.Destination{
		"audit-http": {
			Type: "http",
			HTTP: &config.HTTPDestConfig{
				URL:    server.URL,
				Method: "POST",
				Auth:   &config.HTTPAuthConfig{Type: "bearer", Token: "audit-token"},
			},
		},
	}

	factory := NewFactory(testLogger())
	dest, err := factory.CreateDestination("audit-http", destinations["audit-http"])
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	msg := message.New("ch1", []byte(`{"audit":"event"}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if authHeader != "Bearer audit-token" {
		t.Fatalf("expected auth header, got %q", authHeader)
	}
}

func TestNamedDestination_KafkaOutput(t *testing.T) {
	destinations := map[string]config.Destination{
		"kafka-output": {
			Type: "kafka",
			Kafka: &config.KafkaDestConfig{
				Brokers: []string{"kafka.example.com:9093"},
				Topic:   "hl7-output",
				Auth: &config.HTTPAuthConfig{
					Type:     "sasl_plain",
					Username: "kafka-user",
					Password: "kafka-pass",
				},
				TLS: &config.TLSMapConfig{Enabled: true, InsecureSkipVerify: true},
			},
		},
	}

	factory := NewFactory(testLogger())
	dest, err := factory.CreateDestination("kafka-output", destinations["kafka-output"])
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if dest.Type() != "kafka" {
		t.Fatalf("expected kafka, got %s", dest.Type())
	}
}

func TestNamedDestination_TCPMLLPDest(t *testing.T) {
	destinations := map[string]config.Destination{
		"downstream-hl7": {
			Type: "tcp",
			TCP: &config.TCPDestMapConfig{
				Host:      "hl7-server.local",
				Port:      2575,
				Mode:      "mllp",
				TimeoutMs: 10000,
			},
		},
	}

	factory := NewFactory(testLogger())
	dest, err := factory.CreateDestination("downstream-hl7", destinations["downstream-hl7"])
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if dest.Type() != "tcp" {
		t.Fatalf("expected tcp, got %s", dest.Type())
	}
}

func TestNamedDestination_FHIRWithOAuth2(t *testing.T) {
	destinations := map[string]config.Destination{
		"fhir-server": {
			Type: "fhir",
			FHIR: &config.FHIRDestMapConfig{
				BaseURL:    "https://fhir.example.com/r4",
				Version:    "R4",
				Operations: []string{"create", "update"},
				Auth: &config.HTTPAuthConfig{
					Type:         "oauth2_client_credentials",
					TokenURL:     "https://auth.example.com/token",
					ClientID:     "fhir-app",
					ClientSecret: "fhir-secret",
					Scopes:       []string{"system/*.read", "system/*.write"},
				},
				TLS: &config.TLSMapConfig{Enabled: true},
			},
		},
	}

	factory := NewFactory(testLogger())
	dest, err := factory.CreateDestination("fhir-server", destinations["fhir-server"])
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if dest.Type() != "fhir" {
		t.Fatalf("expected fhir, got %s", dest.Type())
	}
}

func TestNamedDestination_FileDest(t *testing.T) {
	dir := t.TempDir()

	destinations := map[string]config.Destination{
		"archive": {
			Type: "file",
			File: &config.FileDestMapConfig{
				Directory:       dir,
				FilenamePattern: "{{channelId}}_{{timestamp}}.hl7",
			},
		},
	}

	factory := NewFactory(testLogger())
	dest, err := factory.CreateDestination("archive", destinations["archive"])
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	msg := message.New("adt-channel", []byte("MSH|^~\\&|test\r"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("expected 1 file, got %d", len(entries))
	}
	if !strings.HasPrefix(entries[0].Name(), "adt-channel_") {
		t.Fatalf("unexpected filename: %s", entries[0].Name())
	}
}

func TestNamedDestination_DatabaseDest(t *testing.T) {
	destinations := map[string]config.Destination{
		"audit-db": {
			Type: "database",
			Database: &config.DBDestMapConfig{
				Driver:    "postgres",
				DSN:       "postgres://user:pass@localhost:5432/audit",
				Statement: "INSERT INTO audit_log (message_id, channel_id, payload) VALUES ('${messageId}', '${channelId}', '${raw}')",
			},
		},
	}

	factory := NewFactory(testLogger())
	dest, err := factory.CreateDestination("audit-db", destinations["audit-db"])
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if dest.Type() != "database" {
		t.Fatalf("expected database, got %s", dest.Type())
	}
}

func TestNamedDestination_SMTPWithAuth(t *testing.T) {
	destinations := map[string]config.Destination{
		"email-alert": {
			Type: "smtp",
			SMTP: &config.SMTPDestMapConfig{
				Host:    "smtp.example.com",
				Port:    587,
				From:    "alerts@example.com",
				To:      []string{"admin@example.com", "ops@example.com"},
				Subject: "Alert: {{channelId}} - {{messageId}}",
				Auth:    &config.HTTPAuthConfig{Type: "basic", Username: "smtp-user", Password: "smtp-pass"},
				TLS:     &config.TLSMapConfig{Enabled: true},
			},
		},
	}

	factory := NewFactory(testLogger())
	dest, err := factory.CreateDestination("email-alert", destinations["email-alert"])
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if dest.Type() != "smtp" {
		t.Fatalf("expected smtp, got %s", dest.Type())
	}
}

func TestNamedDestination_DICOMWithTLS(t *testing.T) {
	destinations := map[string]config.Destination{
		"pacs-store": {
			Type: "dicom",
			DICOM: &config.DICOMDestMapConfig{
				Host:          "pacs.hospital.org",
				Port:          11112,
				AETitle:       "INTU_SCU",
				CalledAETitle: "PACS_SCP",
				TimeoutMs:     15000,
				TLS:           &config.TLSMapConfig{Enabled: true, InsecureSkipVerify: true},
			},
		},
	}

	factory := NewFactory(testLogger())
	dest, err := factory.CreateDestination("pacs-store", destinations["pacs-store"])
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if dest.Type() != "dicom" {
		t.Fatalf("expected dicom, got %s", dest.Type())
	}
}

func TestNamedDestination_DirectMessaging(t *testing.T) {
	destinations := map[string]config.Destination{
		"direct-provider": {
			Type: "direct",
			Direct: &config.DirectDestMapConfig{
				To:          "provider@direct.hospital.org",
				From:        "lab@direct.lab.org",
				SMTPHost:    "direct-smtp.hospital.org",
				SMTPPort:    465,
				Certificate: "/etc/certs/direct.pem",
				TLS:         &config.TLSMapConfig{Enabled: true},
			},
		},
	}

	factory := NewFactory(testLogger())
	dest, err := factory.CreateDestination("direct-provider", destinations["direct-provider"])
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if dest.Type() != "direct" {
		t.Fatalf("expected direct, got %s", dest.Type())
	}
}

func TestNamedDestination_MultipleReferences(t *testing.T) {
	dir := t.TempDir()

	var httpReceived []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		httpReceived = append(httpReceived, string(body))
		w.WriteHeader(200)
	}))
	defer server.Close()

	destinations := map[string]config.Destination{
		"primary-http": {
			Type: "http",
			HTTP: &config.HTTPDestConfig{URL: server.URL},
		},
		"archive-file": {
			Type: "file",
			File: &config.FileDestMapConfig{Directory: dir},
		},
	}

	factory := NewFactory(testLogger())

	httpDest, err := factory.CreateDestination("primary-http", destinations["primary-http"])
	if err != nil {
		t.Fatalf("create http: %v", err)
	}
	fileDest, err := factory.CreateDestination("archive-file", destinations["archive-file"])
	if err != nil {
		t.Fatalf("create file: %v", err)
	}

	msg := message.New("multi-dest-ch", []byte("multi-dest-payload"))

	resp1, _ := httpDest.Send(context.Background(), msg)
	resp2, _ := fileDest.Send(context.Background(), msg)

	if resp1.StatusCode != 200 {
		t.Fatalf("http dest failed: %d", resp1.StatusCode)
	}
	if resp2.StatusCode != 200 {
		t.Fatalf("file dest failed: %d", resp2.StatusCode)
	}

	if len(httpReceived) != 1 || httpReceived[0] != "multi-dest-payload" {
		t.Fatal("http dest did not receive message")
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatal("file dest did not write message")
	}
}

// ===================================================================
// OAuth2 Helper Tests
// ===================================================================

func TestOAuth2TokenFetch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.Form.Get("grant_type") != "client_credentials" {
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "test-token-123",
			"expires_in":   3600,
			"token_type":   "Bearer",
		})
	}))
	defer server.Close()

	// Clear cache
	oauth2CacheMu.Lock()
	oauth2Cache = make(map[string]*oauth2CachedToken)
	oauth2CacheMu.Unlock()

	token, err := fetchOAuth2Token(server.URL, "client", "secret", []string{"read"})
	if err != nil {
		t.Fatalf("fetchOAuth2Token: %v", err)
	}
	if token != "test-token-123" {
		t.Fatalf("expected test-token-123, got %q", token)
	}
}

func TestOAuth2TokenCaching(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": fmt.Sprintf("token-%d", callCount),
			"expires_in":   3600,
		})
	}))
	defer server.Close()

	// Clear cache
	oauth2CacheMu.Lock()
	oauth2Cache = make(map[string]*oauth2CachedToken)
	oauth2CacheMu.Unlock()

	token1, _ := fetchOAuth2Token(server.URL, "c", "s", nil)
	token2, _ := fetchOAuth2Token(server.URL, "c", "s", nil)

	if token1 != token2 {
		t.Fatalf("expected same token from cache, got %q and %q", token1, token2)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 token request (cached), got %d", callCount)
	}
}

func TestOAuth2TokenMissingConfig(t *testing.T) {
	_, err := fetchOAuth2Token("", "client", "secret", nil)
	if err == nil {
		t.Fatal("expected error for empty token URL")
	}
	_, err = fetchOAuth2Token("http://localhost", "", "secret", nil)
	if err == nil {
		t.Fatal("expected error for empty client ID")
	}
}

// ===================================================================
// extractHL7AckCode Tests
// ===================================================================

func TestExtractHL7AckCode_AA(t *testing.T) {
	ack := []byte("MSH|^~\\&|R|F|S|F|20230101||ACK||P|2.5\rMSA|AA|12345\r")
	code := extractHL7AckCode(ack)
	if code != "AA" {
		t.Fatalf("expected AA, got %q", code)
	}
}

func TestExtractHL7AckCode_AE(t *testing.T) {
	ack := []byte("MSH|^~\\&|R|F|S|F|20230101||ACK||P|2.5\rMSA|AE|12345\r")
	code := extractHL7AckCode(ack)
	if code != "AE" {
		t.Fatalf("expected AE, got %q", code)
	}
}

func TestExtractHL7AckCode_Empty(t *testing.T) {
	code := extractHL7AckCode([]byte("no MSA segment"))
	if code != "" {
		t.Fatalf("expected empty, got %q", code)
	}
}
