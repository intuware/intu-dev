package connector

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/pkg/config"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func noopHandler(_ context.Context, _ *message.Message) error {
	return nil
}

// -------------------------------------------------------------------
// HTTP Source Tests
// -------------------------------------------------------------------

func TestHTTPSource_StartStop(t *testing.T) {
	src := NewHTTPSource(&config.HTTPListener{Port: 0, Path: "/"}, testLogger())
	// Port 0 means the OS picks one
	cfg := &config.HTTPListener{Port: 0, Path: "/"}
	src = NewHTTPSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()
	if addr == "" {
		t.Fatal("expected non-empty addr")
	}

	resp, err := http.Post("http://"+addr+"/", "text/plain", strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if string(received[0].Raw) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(received[0].Raw))
	}
}

func TestHTTPSource_MethodFilter(t *testing.T) {
	cfg := &config.HTTPListener{Port: 0, Methods: []string{"POST"}}
	src := NewHTTPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Get("http://" + src.Addr() + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestHTTPSource_BearerAuth(t *testing.T) {
	cfg := &config.HTTPListener{
		Port: 0,
		Auth: &config.AuthConfig{Type: "bearer", Token: "secret-token"},
	}
	src := NewHTTPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()

	// No auth → 401
	resp, err := http.Post("http://"+addr+"/", "text/plain", strings.NewReader("test"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", resp.StatusCode)
	}

	// Wrong token → 401
	req, _ := http.NewRequest("POST", "http://"+addr+"/", strings.NewReader("test"))
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong token, got %d", resp.StatusCode)
	}

	// Correct token → 200
	req, _ = http.NewRequest("POST", "http://"+addr+"/", strings.NewReader("test"))
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with correct token, got %d", resp.StatusCode)
	}
}

func TestHTTPSource_BasicAuth(t *testing.T) {
	cfg := &config.HTTPListener{
		Port: 0,
		Auth: &config.AuthConfig{Type: "basic", Username: "admin", Password: "pass123"},
	}
	src := NewHTTPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()

	// No auth → 401
	resp, err := http.Post("http://"+addr+"/", "text/plain", strings.NewReader("test"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	// Correct basic auth → 200
	req, _ := http.NewRequest("POST", "http://"+addr+"/", strings.NewReader("test"))
	req.SetBasicAuth("admin", "pass123")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPSource_APIKeyAuth(t *testing.T) {
	cfg := &config.HTTPListener{
		Port: 0,
		Auth: &config.AuthConfig{Type: "api_key", Key: "my-key", Header: "X-API-Key"},
	}
	src := NewHTTPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()

	// No key → 401
	resp, err := http.Post("http://"+addr+"/", "text/plain", strings.NewReader("test"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	// Correct key → 200
	req, _ := http.NewRequest("POST", "http://"+addr+"/", strings.NewReader("test"))
	req.Header.Set("X-API-Key", "my-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPSource_APIKeyQueryParam(t *testing.T) {
	cfg := &config.HTTPListener{
		Port: 0,
		Auth: &config.AuthConfig{Type: "api_key", Key: "qp-key", QueryParam: "api_key"},
	}
	src := NewHTTPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()

	resp, err := http.Post("http://"+addr+"/?api_key=qp-key", "text/plain", strings.NewReader("test"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPSource_CorrelationID(t *testing.T) {
	cfg := &config.HTTPListener{Port: 0}
	src := NewHTTPSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	req, _ := http.NewRequest("POST", "http://"+src.Addr()+"/", strings.NewReader("test"))
	req.Header.Set("X-Correlation-Id", "corr-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if received[0].CorrelationID != "corr-123" {
		t.Fatalf("expected correlation ID 'corr-123', got %q", received[0].CorrelationID)
	}
}

// -------------------------------------------------------------------
// HTTP Source TLS Test
// -------------------------------------------------------------------

func generateTestCerts(t *testing.T) (certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(1 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	os.WriteFile(certFile, certPEM, 0o644)

	keyDER, _ := x509.MarshalECPrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	os.WriteFile(keyFile, keyPEM, 0o600)

	return certFile, keyFile
}

func TestHTTPSource_TLS(t *testing.T) {
	certFile, keyFile := generateTestCerts(t)

	cfg := &config.HTTPListener{
		Port: 0,
		TLS: &config.TLSConfig{
			Enabled:            true,
			CertFile:           certFile,
			KeyFile:            keyFile,
			InsecureSkipVerify: true,
		},
	}
	src := NewHTTPSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := client.Post("https://"+src.Addr()+"/", "text/plain", strings.NewReader("tls-test"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if string(received[0].Raw) != "tls-test" {
		t.Fatalf("expected 'tls-test', got %q", string(received[0].Raw))
	}
}

// -------------------------------------------------------------------
// TCP Source Tests
// -------------------------------------------------------------------

func TestTCPSource_RawMode(t *testing.T) {
	cfg := &config.TCPListener{Port: 0, Mode: "raw", TimeoutMs: 5000}
	src := NewTCPSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	// Use a specific port so we know the address
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	cfg.Port = port

	src = NewTCPSource(cfg, testLogger())
	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Write([]byte("hello\n"))
	conn.Close()

	time.Sleep(100 * time.Millisecond)

	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if string(received[0].Raw) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(received[0].Raw))
	}
}

func TestTCPSource_MLLPMode(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	cfg := &config.TCPListener{Port: port, Mode: "mllp", TimeoutMs: 5000}
	src := NewTCPSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	hl7msg := "MSH|^~\\&|SEND|FAC|RECV|FAC|20230101||ADT^A01|12345|P|2.5\rPID|1||MRN123||Doe^John\r"

	var buf bytes.Buffer
	buf.WriteByte(0x0B) // MLLP start
	buf.WriteString(hl7msg)
	buf.WriteByte(0x1C) // MLLP end
	buf.WriteByte(0x0D) // CR
	conn.Write(buf.Bytes())
	conn.Close()

	time.Sleep(100 * time.Millisecond)

	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if string(received[0].Raw) != hl7msg {
		t.Fatalf("MLLP message mismatch:\nexpected: %q\ngot:      %q", hl7msg, string(received[0].Raw))
	}
}

func TestTCPSource_MLLPWithACK(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	cfg := &config.TCPListener{
		Port:      port,
		Mode:      "mllp",
		TimeoutMs: 5000,
		ACK:       &config.ACKConfig{Auto: true, SuccessCode: "AA", ErrorCode: "AE"},
	}
	src := NewTCPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	hl7msg := "MSH|^~\\&|SEND|FAC|RECV|FAC|20230101||ADT^A01|12345|P|2.5\rPID|1||MRN123||Doe^John\r"

	var buf bytes.Buffer
	buf.WriteByte(0x0B)
	buf.WriteString(hl7msg)
	buf.WriteByte(0x1C)
	buf.WriteByte(0x0D)
	conn.Write(buf.Bytes())

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	ackBuf := make([]byte, 4096)
	n, err := conn.Read(ackBuf)
	if err != nil {
		t.Fatalf("read ACK: %v", err)
	}

	ackStr := string(ackBuf[:n])
	if !strings.Contains(ackStr, "MSA|AA|12345") {
		t.Fatalf("expected ACK with MSA|AA|12345, got: %q", ackStr)
	}
}

func TestTCPSource_TLS(t *testing.T) {
	certFile, keyFile := generateTestCerts(t)

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	cfg := &config.TCPListener{
		Port:      port,
		Mode:      "raw",
		TimeoutMs: 5000,
		TLS: &config.TLSConfig{
			Enabled:  true,
			CertFile: certFile,
			KeyFile:  keyFile,
		},
	}
	src := NewTCPSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	time.Sleep(50 * time.Millisecond)

	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	conn.Write([]byte("tls-hello\n"))
	conn.Close()

	time.Sleep(100 * time.Millisecond)

	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if string(received[0].Raw) != "tls-hello" {
		t.Fatalf("expected 'tls-hello', got %q", string(received[0].Raw))
	}
}

// -------------------------------------------------------------------
// File Source Tests
// -------------------------------------------------------------------

func TestFileSource_PollAndProcess(t *testing.T) {
	dir := t.TempDir()
	inputDir := filepath.Join(dir, "input")
	moveDir := filepath.Join(dir, "processed")
	os.MkdirAll(inputDir, 0o755)
	os.MkdirAll(moveDir, 0o755)

	os.WriteFile(filepath.Join(inputDir, "test1.txt"), []byte("file-content-1"), 0o644)
	os.WriteFile(filepath.Join(inputDir, "test2.txt"), []byte("file-content-2"), 0o644)

	cfg := &config.FileListener{
		Directory:    inputDir,
		FilePattern:  "*.txt",
		PollInterval: "100ms",
		MoveTo:       moveDir,
		SortBy:       "name",
	}
	src := NewFileSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	src.Stop(ctx)

	if len(received) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(received))
	}

	// Files should be moved to processed dir
	entries, _ := os.ReadDir(moveDir)
	if len(entries) != 2 {
		t.Fatalf("expected 2 files in processed dir, got %d", len(entries))
	}

	// Input dir should be empty
	entries, _ = os.ReadDir(inputDir)
	if len(entries) != 0 {
		t.Fatalf("expected 0 files in input dir, got %d", len(entries))
	}
}

func TestFileSource_ErrorDir(t *testing.T) {
	dir := t.TempDir()
	inputDir := filepath.Join(dir, "input")
	errorDir := filepath.Join(dir, "errors")
	os.MkdirAll(inputDir, 0o755)

	os.WriteFile(filepath.Join(inputDir, "test.txt"), []byte("content"), 0o644)

	cfg := &config.FileListener{
		Directory:    inputDir,
		FilePattern:  "*.txt",
		PollInterval: "100ms",
		ErrorDir:     errorDir,
	}
	src := NewFileSource(cfg, testLogger())

	ctx := context.Background()
	handler := func(_ context.Context, msg *message.Message) error {
		return fmt.Errorf("simulated error")
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	src.Stop(ctx)

	entries, _ := os.ReadDir(errorDir)
	if len(entries) != 1 {
		t.Fatalf("expected 1 file in error dir, got %d", len(entries))
	}
}

func TestFileSource_MetadataPopulated(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "meta.txt"), []byte("metadata-test"), 0o644)

	cfg := &config.FileListener{
		Directory:    dir,
		PollInterval: "100ms",
	}
	src := NewFileSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	src.Stop(ctx)

	if len(received) < 1 {
		t.Fatal("expected at least 1 message")
	}

	msg := received[0]
	if msg.Metadata["filename"] != "meta.txt" {
		t.Fatalf("expected filename 'meta.txt', got %v", msg.Metadata["filename"])
	}
}

// -------------------------------------------------------------------
// Channel Source Tests
// -------------------------------------------------------------------

func TestChannelSource_ReceiveFromBus(t *testing.T) {
	cfg := &config.ChannelListener{SourceChannelID: "test-channel-123"}
	src := NewChannelSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	bus := GetChannelBus()
	testMsg := message.New("", []byte("bus-message"))
	bus.Publish("test-channel-123", testMsg)

	time.Sleep(100 * time.Millisecond)

	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if string(received[0].Raw) != "bus-message" {
		t.Fatalf("expected 'bus-message', got %q", string(received[0].Raw))
	}
}

// -------------------------------------------------------------------
// SOAP Source Tests
// -------------------------------------------------------------------

func TestSOAPSource_StartStop(t *testing.T) {
	cfg := &config.SOAPListener{Port: 0, ServiceName: "TestService"}
	src := NewSOAPSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()

	soapEnvelope := `<?xml version="1.0"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><test/></soap:Body></soap:Envelope>`
	req, _ := http.NewRequest("POST", "http://"+addr+"/", strings.NewReader(soapEnvelope))
	req.Header.Set("Content-Type", "text/xml; charset=utf-8")
	req.Header.Set("SOAPAction", "process")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if received[0].Metadata["soap_action"] != "process" {
		t.Fatalf("expected soap_action 'process', got %v", received[0].Metadata["soap_action"])
	}
}

func TestSOAPSource_WSDL(t *testing.T) {
	cfg := &config.SOAPListener{Port: 0, ServiceName: "MyService", WSDLPath: "/wsdl"}
	src := NewSOAPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Get("http://" + addr + "/wsdl")
	if err != nil {
		t.Fatalf("GET WSDL: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "MyService") {
		t.Fatalf("WSDL should contain service name")
	}
}

func TestSOAPSource_Auth(t *testing.T) {
	cfg := &config.SOAPListener{
		Port: 0,
		Auth: &config.AuthConfig{Type: "bearer", Token: "soap-secret"},
	}
	src := NewSOAPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()

	// No auth → 401
	req, _ := http.NewRequest("POST", "http://"+addr+"/", strings.NewReader("<soap/>"))
	req.Header.Set("Content-Type", "text/xml")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	// With auth → 200
	req, _ = http.NewRequest("POST", "http://"+addr+"/", strings.NewReader("<soap/>"))
	req.Header.Set("Content-Type", "text/xml")
	req.Header.Set("Authorization", "Bearer soap-secret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestSOAPSource_ContentTypeEnforcement(t *testing.T) {
	cfg := &config.SOAPListener{Port: 0}
	src := NewSOAPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()

	// Wrong content type → 415
	req, _ := http.NewRequest("POST", "http://"+addr+"/", strings.NewReader("not-xml"))
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("expected 415, got %d", resp.StatusCode)
	}
}

// -------------------------------------------------------------------
// FHIR Source Tests
// -------------------------------------------------------------------

func TestFHIRSource_CapabilityStatement(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir", Version: "R4"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Get("http://" + addr + "/fhir/metadata")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var cap map[string]any
	if err := json.Unmarshal(body, &cap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cap["resourceType"] != "CapabilityStatement" {
		t.Fatalf("expected CapabilityStatement, got %v", cap["resourceType"])
	}
}

func TestFHIRSource_CreateResource(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir", Version: "R4"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	patient := `{"resourceType":"Patient","name":[{"family":"Doe","given":["John"]}]}`
	resp, err := http.Post("http://"+addr+"/fhir/Patient", "application/fhir+json", strings.NewReader(patient))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if received[0].Metadata["resource_type"] != "Patient" {
		t.Fatalf("expected resource_type 'Patient', got %v", received[0].Metadata["resource_type"])
	}
}

func TestFHIRSource_Auth(t *testing.T) {
	cfg := &config.FHIRListener{
		Port:     0,
		BasePath: "/fhir",
		Auth:     &config.AuthConfig{Type: "bearer", Token: "fhir-token"},
	}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()

	// No auth → 401
	resp, err := http.Post("http://"+addr+"/fhir/Patient", "application/fhir+json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	// With auth → 201
	req, _ := http.NewRequest("POST", "http://"+addr+"/fhir/Patient", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer fhir-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
}

func TestFHIRSource_TLS(t *testing.T) {
	certFile, keyFile := generateTestCerts(t)

	cfg := &config.FHIRListener{
		Port:     0,
		BasePath: "/fhir",
		TLS: &config.TLSConfig{
			Enabled:  true,
			CertFile: certFile,
			KeyFile:  keyFile,
		},
	}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := client.Get("https://" + src.listener.Addr().String() + "/fhir/metadata")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestFHIRSource_ResourceFiltering(t *testing.T) {
	cfg := &config.FHIRListener{
		Port:      0,
		BasePath:  "/fhir",
		Version:   "R4",
		Resources: []string{"Patient"},
	}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()

	resp, err := http.Post("http://"+addr+"/fhir/Patient", "application/fhir+json",
		strings.NewReader(`{"resourceType":"Patient","id":"1"}`))
	if err != nil {
		t.Fatalf("POST Patient: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 for Patient, got %d", resp.StatusCode)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}

	resp, err = http.Post("http://"+addr+"/fhir/Observation", "application/fhir+json",
		strings.NewReader(`{"resourceType":"Observation","id":"2"}`))
	if err != nil {
		t.Fatalf("POST Observation: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for Observation, got %d", resp.StatusCode)
	}
	if len(received) != 1 {
		t.Fatalf("Observation should not have been received, got %d messages", len(received))
	}
}

func TestFHIRSource_DynamicCapabilityStatement(t *testing.T) {
	cfg := &config.FHIRListener{
		Port:      0,
		BasePath:  "/fhir",
		Version:   "R4",
		Resources: []string{"Patient"},
	}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Get("http://" + addr + "/fhir/metadata")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var cap map[string]any
	if err := json.Unmarshal(body, &cap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	rest := cap["rest"].([]any)
	server := rest[0].(map[string]any)
	resources := server["resource"].([]any)

	if len(resources) != 1 {
		t.Fatalf("expected 1 resource in CapabilityStatement, got %d", len(resources))
	}
	res := resources[0].(map[string]any)
	if res["type"] != "Patient" {
		t.Fatalf("expected resource type 'Patient', got %v", res["type"])
	}
}

func TestFHIRSource_NoResourcesAcceptsAll(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir", Version: "R4"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()

	resp, err := http.Post("http://"+addr+"/fhir/Observation", "application/fhir+json",
		strings.NewReader(`{"resourceType":"Observation","id":"1"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 for unrestricted source, got %d", resp.StatusCode)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
}

// -------------------------------------------------------------------
// IHE Source Tests
// -------------------------------------------------------------------

func TestIHESource_XDSRepository(t *testing.T) {
	cfg := &config.IHEListener{Profile: "xds_repository", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Post("http://"+addr+"/xds/repository/provide", "text/xml", strings.NewReader("<doc>test</doc>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if received[0].Metadata["ihe_profile"] != "xds_repository" {
		t.Fatalf("expected profile 'xds_repository', got %v", received[0].Metadata["ihe_profile"])
	}
}

func TestIHESource_PIX(t *testing.T) {
	cfg := &config.IHEListener{Profile: "pix", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Post("http://"+addr+"/pix/query", "text/xml", strings.NewReader("<query/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if received[0].Metadata["ihe_transaction"] != "PatientIdentityCrossReference" {
		t.Fatalf("expected PatientIdentityCrossReference transaction")
	}
}

func TestIHESource_PDQ(t *testing.T) {
	cfg := &config.IHEListener{Profile: "pdq", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Post("http://"+addr+"/pdq/query", "text/xml", strings.NewReader("<query/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if received[0].Metadata["ihe_transaction"] != "PatientDemographicsQuery" {
		t.Fatalf("expected PatientDemographicsQuery transaction")
	}
}

func TestIHESource_Auth(t *testing.T) {
	cfg := &config.IHEListener{
		Profile: "xds_repository",
		Port:    0,
		Auth:    &config.AuthConfig{Type: "basic", Username: "ihe", Password: "pass"},
	}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()

	// No auth → 401
	resp, err := http.Post("http://"+addr+"/xds/repository/provide", "text/xml", strings.NewReader("<doc/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	// With auth → 200
	req, _ := http.NewRequest("POST", "http://"+addr+"/xds/repository/provide", strings.NewReader("<doc/>"))
	req.SetBasicAuth("ihe", "pass")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestIHESource_StatusEndpoint(t *testing.T) {
	cfg := &config.IHEListener{Profile: "pix", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Get("http://" + addr + "/ihe/status")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var status map[string]any
	json.Unmarshal(body, &status)
	if status["profile"] != "pix" {
		t.Fatalf("expected profile 'pix', got %v", status["profile"])
	}
	if status["status"] != "running" {
		t.Fatalf("expected status 'running', got %v", status["status"])
	}
}

// -------------------------------------------------------------------
// DICOM Source Tests
// -------------------------------------------------------------------

func TestDICOMSource_AAssociateAndData(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	cfg := &config.DICOMListener{Port: port, AETitle: "TESTSCP"}
	src := NewDICOMSource(cfg, testLogger())

	ctx := context.Background()
	var received []*message.Message
	handler := func(_ context.Context, msg *message.Message) error {
		received = append(received, msg)
		return nil
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send A-ASSOCIATE-RQ (PDU type 0x01)
	associateData := make([]byte, 68)
	associateData[0] = 0x00
	associateData[1] = 0x01
	copy(associateData[4:20], []byte(fmt.Sprintf("%-16s", "TESTSCP")))
	copy(associateData[20:36], []byte(fmt.Sprintf("%-16s", "TESTSCU")))

	pdu := make([]byte, 6+len(associateData))
	pdu[0] = 0x01 // A-ASSOCIATE-RQ
	binary.BigEndian.PutUint32(pdu[2:6], uint32(len(associateData)))
	copy(pdu[6:], associateData)
	conn.Write(pdu)

	// Read A-ASSOCIATE-AC
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	header := make([]byte, 6)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		t.Fatalf("read AC header: %v", err)
	}
	if header[0] != 0x02 {
		t.Fatalf("expected A-ASSOCIATE-AC (0x02), got 0x%02X", header[0])
	}

	acLen := binary.BigEndian.Uint32(header[2:6])
	if acLen > 0 {
		acBody := make([]byte, acLen)
		io.ReadFull(conn, acBody)
	}

	// Send P-DATA-TF (PDU type 0x04)
	dataPayload := []byte("DICOM-TEST-DATA")
	dataPDU := make([]byte, 6+len(dataPayload))
	dataPDU[0] = 0x04
	binary.BigEndian.PutUint32(dataPDU[2:6], uint32(len(dataPayload)))
	copy(dataPDU[6:], dataPayload)
	conn.Write(dataPDU)

	time.Sleep(200 * time.Millisecond)

	// Send A-RELEASE-RQ
	releasePDU := make([]byte, 10)
	releasePDU[0] = 0x05
	binary.BigEndian.PutUint32(releasePDU[2:6], 4)
	conn.Write(releasePDU)

	time.Sleep(100 * time.Millisecond)

	if len(received) < 1 {
		t.Fatalf("expected at least 1 message, got %d", len(received))
	}
	if received[0].ContentType != "dicom" {
		t.Fatalf("expected content type 'dicom', got %q", received[0].ContentType)
	}
}

func TestDICOMSource_AETitleValidation(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	cfg := &config.DICOMListener{
		Port:            port,
		AETitle:         "TESTSCP",
		CallingAETitles: []string{"ALLOWED_SCU"},
	}
	src := NewDICOMSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	time.Sleep(50 * time.Millisecond)

	// Send with unauthorized calling AE
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	associateData := make([]byte, 68)
	associateData[0] = 0x00
	associateData[1] = 0x01
	copy(associateData[4:20], []byte(fmt.Sprintf("%-16s", "TESTSCP")))
	copy(associateData[20:36], []byte(fmt.Sprintf("%-16s", "UNAUTHORIZED")))

	pdu := make([]byte, 6+len(associateData))
	pdu[0] = 0x01
	binary.BigEndian.PutUint32(pdu[2:6], uint32(len(associateData)))
	copy(pdu[6:], associateData)
	conn.Write(pdu)

	// Should receive A-ASSOCIATE-RJ (0x03)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	header := make([]byte, 6)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		t.Fatalf("read response header: %v", err)
	}
	if header[0] != 0x03 {
		t.Fatalf("expected A-ASSOCIATE-RJ (0x03), got 0x%02X", header[0])
	}
}

func TestDICOMSource_AETitleValidation_Allowed(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	cfg := &config.DICOMListener{
		Port:            port,
		AETitle:         "TESTSCP",
		CallingAETitles: []string{"ALLOWED_SCU"},
	}
	src := NewDICOMSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	associateData := make([]byte, 68)
	associateData[0] = 0x00
	associateData[1] = 0x01
	copy(associateData[4:20], []byte(fmt.Sprintf("%-16s", "TESTSCP")))
	copy(associateData[20:36], []byte(fmt.Sprintf("%-16s", "ALLOWED_SCU")))

	pdu := make([]byte, 6+len(associateData))
	pdu[0] = 0x01
	binary.BigEndian.PutUint32(pdu[2:6], uint32(len(associateData)))
	copy(pdu[6:], associateData)
	conn.Write(pdu)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	header := make([]byte, 6)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		t.Fatalf("read response header: %v", err)
	}
	if header[0] != 0x02 {
		t.Fatalf("expected A-ASSOCIATE-AC (0x02), got 0x%02X", header[0])
	}
}

// -------------------------------------------------------------------
// Factory Tests
// -------------------------------------------------------------------

func TestFactory_CreateAllSourceTypes(t *testing.T) {
	factory := NewFactory(testLogger())

	tests := []struct {
		name string
		cfg  config.ListenerConfig
	}{
		{
			name: "http",
			cfg: config.ListenerConfig{
				Type: "http",
				HTTP: &config.HTTPListener{Port: 0},
			},
		},
		{
			name: "tcp",
			cfg: config.ListenerConfig{
				Type: "tcp",
				TCP:  &config.TCPListener{Port: 0},
			},
		},
		{
			name: "file",
			cfg: config.ListenerConfig{
				Type: "file",
				File: &config.FileListener{Directory: "/tmp"},
			},
		},
		{
			name: "sftp",
			cfg: config.ListenerConfig{
				Type: "sftp",
				SFTP: &config.SFTPListener{Host: "localhost"},
			},
		},
		{
			name: "database",
			cfg: config.ListenerConfig{
				Type:     "database",
				Database: &config.DBListener{Driver: "sqlite3", DSN: ":memory:"},
			},
		},
		{
			name: "kafka",
			cfg: config.ListenerConfig{
				Type:  "kafka",
				Kafka: &config.KafkaListener{Brokers: []string{"localhost:9092"}, Topic: "test"},
			},
		},
		{
			name: "email",
			cfg: config.ListenerConfig{
				Type:  "email",
				Email: &config.EmailListener{Host: "localhost"},
			},
		},
		{
			name: "dicom",
			cfg: config.ListenerConfig{
				Type:  "dicom",
				DICOM: &config.DICOMListener{Port: 0},
			},
		},
		{
			name: "soap",
			cfg: config.ListenerConfig{
				Type: "soap",
				SOAP: &config.SOAPListener{Port: 0},
			},
		},
		{
			name: "fhir",
			cfg: config.ListenerConfig{
				Type: "fhir",
				FHIR: &config.FHIRListener{Port: 0},
			},
		},
		{
			name: "ihe",
			cfg: config.ListenerConfig{
				Type: "ihe",
				IHE:  &config.IHEListener{Profile: "pix", Port: 0},
			},
		},
		{
			name: "channel",
			cfg: config.ListenerConfig{
				Type:    "channel",
				Channel: &config.ChannelListener{SourceChannelID: "test"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := factory.CreateSource(tt.cfg)
			if err != nil {
				t.Fatalf("CreateSource(%s): %v", tt.name, err)
			}
			if src == nil {
				t.Fatalf("CreateSource(%s) returned nil", tt.name)
			}
			if src.Type() == "" {
				t.Fatalf("Type() returned empty string for %s", tt.name)
			}
		})
	}
}

func TestFactory_UnsupportedType(t *testing.T) {
	factory := NewFactory(testLogger())
	_, err := factory.CreateSource(config.ListenerConfig{Type: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestFactory_NilConfig(t *testing.T) {
	factory := NewFactory(testLogger())
	_, err := factory.CreateSource(config.ListenerConfig{Type: "http"})
	if err == nil {
		t.Fatal("expected error for nil http config")
	}
}

// -------------------------------------------------------------------
// Auth Helper Tests
// -------------------------------------------------------------------

func TestAuthenticateHTTP_NoConfig(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	if !authenticateHTTP(req, nil) {
		t.Fatal("nil config should allow all")
	}
}

func TestAuthenticateHTTP_NoneType(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	if !authenticateHTTP(req, &config.AuthConfig{Type: "none"}) {
		t.Fatal("none type should allow all")
	}
}

func TestAuthenticateHTTP_BearerValid(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	if !authenticateHTTP(req, &config.AuthConfig{Type: "bearer", Token: "valid-token"}) {
		t.Fatal("valid bearer should pass")
	}
}

func TestAuthenticateHTTP_BearerInvalid(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	if authenticateHTTP(req, &config.AuthConfig{Type: "bearer", Token: "valid-token"}) {
		t.Fatal("invalid bearer should fail")
	}
}

func TestAuthenticateHTTP_BasicValid(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	req.SetBasicAuth("user", "pass")
	if !authenticateHTTP(req, &config.AuthConfig{Type: "basic", Username: "user", Password: "pass"}) {
		t.Fatal("valid basic auth should pass")
	}
}

func TestAuthenticateHTTP_BasicInvalid(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	req.SetBasicAuth("user", "wrong")
	if authenticateHTTP(req, &config.AuthConfig{Type: "basic", Username: "user", Password: "pass"}) {
		t.Fatal("invalid basic auth should fail")
	}
}

func TestAuthenticateHTTP_APIKeyHeader(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	req.Header.Set("X-Key", "my-key")
	if !authenticateHTTP(req, &config.AuthConfig{Type: "api_key", Key: "my-key", Header: "X-Key"}) {
		t.Fatal("valid API key header should pass")
	}
}

func TestAuthenticateHTTP_MTLS(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	if authenticateHTTP(req, &config.AuthConfig{Type: "mtls"}) {
		t.Fatal("mTLS without TLS connection should fail")
	}
}

// -------------------------------------------------------------------
// extractHL7ControlID Tests
// -------------------------------------------------------------------

func TestExtractHL7ControlID(t *testing.T) {
	msg := []byte("MSH|^~\\&|SEND|FAC|RECV|FAC|20230101||ADT^A01|12345|P|2.5\rPID|1||MRN123\r")
	id := extractHL7ControlID(msg)
	if id != "12345" {
		t.Fatalf("expected '12345', got %q", id)
	}
}

func TestExtractHL7ControlID_NotFound(t *testing.T) {
	msg := []byte("PID|1||MRN123\r")
	id := extractHL7ControlID(msg)
	if id != "0" {
		t.Fatalf("expected '0', got %q", id)
	}
}
