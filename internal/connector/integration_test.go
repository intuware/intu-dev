package connector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// SharedHTTPListener
// ===================================================================

func TestSharedHTTPListener_AcquireAndRelease(t *testing.T) {
	ResetSharedHTTPListeners()
	defer ResetSharedHTTPListeners()

	sl, err := acquireSharedHTTPListener(0, nil, testLogger())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	actualPort := sl.listener.Addr().(*net.TCPAddr).Port
	if actualPort == 0 {
		t.Fatal("expected non-zero port")
	}
	if sl.refs != 1 {
		t.Fatalf("expected refs=1, got %d", sl.refs)
	}

	releaseSharedHTTPListener(sl.port, context.Background())
}

func TestSharedHTTPListener_RegisterPathAndServe(t *testing.T) {
	ResetSharedHTTPListeners()
	defer ResetSharedHTTPListeners()

	sl, err := acquireSharedHTTPListener(0, nil, testLogger())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	actualPort := sl.listener.Addr().(*net.TCPAddr).Port

	var called bool
	sl.router.Register("/hello", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(200)
		w.Write([]byte("world"))
	}))

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/hello", actualPort))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if !called {
		t.Fatal("expected handler to be called")
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	releaseSharedHTTPListener(sl.port, context.Background())
}

func TestResetSharedHTTPListeners(t *testing.T) {
	ResetSharedHTTPListeners()
	_, err := acquireSharedHTTPListener(0, nil, testLogger())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	sharedMu.Lock()
	count := len(sharedListeners)
	sharedMu.Unlock()
	if count != 1 {
		t.Fatalf("expected 1 shared listener, got %d", count)
	}

	ResetSharedHTTPListeners()

	sharedMu.Lock()
	count = len(sharedListeners)
	sharedMu.Unlock()
	if count != 0 {
		t.Fatalf("expected 0 shared listeners after reset, got %d", count)
	}
}

// ===================================================================
// HTTPSource — shared listener, handler error
// ===================================================================

func TestHTTPSource_SharedListenerMultiplePaths(t *testing.T) {
	ResetSharedHTTPListeners()
	defer ResetSharedHTTPListeners()

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	time.Sleep(50 * time.Millisecond)

	cfg1 := &config.HTTPListener{Port: port, Path: "/ch1"}
	cfg2 := &config.HTTPListener{Port: port, Path: "/ch2"}

	src1 := NewHTTPSource(cfg1, testLogger())
	src2 := NewHTTPSource(cfg2, testLogger())

	ctx := context.Background()
	cap1 := &msgCapture{}
	cap2 := &msgCapture{}

	if err := src1.Start(ctx, cap1.handler()); err != nil {
		t.Fatalf("start src1: %v", err)
	}
	defer src1.Stop(ctx)

	if err := src2.Start(ctx, cap2.handler()); err != nil {
		t.Fatalf("start src2: %v", err)
	}
	defer src2.Stop(ctx)

	resp1, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/ch1", port), "text/plain", strings.NewReader("msg1"))
	if err != nil {
		t.Fatalf("POST /ch1: %v", err)
	}
	resp1.Body.Close()

	resp2, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/ch2", port), "text/plain", strings.NewReader("msg2"))
	if err != nil {
		t.Fatalf("POST /ch2: %v", err)
	}
	resp2.Body.Close()

	time.Sleep(100 * time.Millisecond)

	if cap1.count() != 1 {
		t.Fatalf("cap1: expected 1, got %d", cap1.count())
	}
	if cap2.count() != 1 {
		t.Fatalf("cap2: expected 1, got %d", cap2.count())
	}
	if string(cap1.get(0).Raw) != "msg1" {
		t.Fatalf("cap1: expected 'msg1', got %q", string(cap1.get(0).Raw))
	}
	if string(cap2.get(0).Raw) != "msg2" {
		t.Fatalf("cap2: expected 'msg2', got %q", string(cap2.get(0).Raw))
	}
}

func TestHTTPSource_HandlerError(t *testing.T) {
	cfg := &config.HTTPListener{Port: 0}
	src := NewHTTPSource(cfg, testLogger())

	ctx := context.Background()
	errHandler := func(_ context.Context, _ *message.Message) error {
		return fmt.Errorf("simulated error")
	}

	if err := src.Start(ctx, errHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Post("http://"+src.Addr()+"/", "text/plain", strings.NewReader("fail"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
}

// ===================================================================
// ChannelBus
// ===================================================================

func TestChannelBus_MultipleSubscribers(t *testing.T) {
	bus := GetChannelBus()
	chID := fmt.Sprintf("multi-sub-%d", time.Now().UnixNano())

	sub1 := bus.Subscribe(chID)
	sub2 := bus.Subscribe(chID)

	msg := message.New("", []byte("broadcast"))
	bus.Publish(chID, msg)

	select {
	case m := <-sub1:
		if string(m.Raw) != "broadcast" {
			t.Fatalf("sub1: expected 'broadcast', got %q", string(m.Raw))
		}
	case <-time.After(time.Second):
		t.Fatal("sub1 timed out")
	}

	select {
	case m := <-sub2:
		if string(m.Raw) != "broadcast" {
			t.Fatalf("sub2: expected 'broadcast', got %q", string(m.Raw))
		}
	case <-time.After(time.Second):
		t.Fatal("sub2 timed out")
	}
}

func TestChannelBus_Singleton(t *testing.T) {
	b1 := GetChannelBus()
	b2 := GetChannelBus()
	if b1 != b2 {
		t.Fatal("GetChannelBus should return the same singleton")
	}
}

// ===================================================================
// applyTLSToListener
// ===================================================================

func TestApplyTLSToListener_NilConfig(t *testing.T) {
	ln, _ := net.Listen("tcp", ":0")
	defer ln.Close()
	server := &http.Server{}

	result, err := applyTLSToListener(ln, server, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != ln {
		t.Fatal("expected same listener returned for nil config")
	}
}

func TestApplyTLSToListener_DisabledConfig(t *testing.T) {
	ln, _ := net.Listen("tcp", ":0")
	defer ln.Close()
	server := &http.Server{}

	result, err := applyTLSToListener(ln, server, &config.TLSConfig{Enabled: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != ln {
		t.Fatal("expected same listener returned for disabled TLS")
	}
}

func TestApplyTLSToListener_EnabledWithValidCert(t *testing.T) {
	certFile, keyFile := generateTestCerts(t)
	ln, _ := net.Listen("tcp", ":0")
	defer ln.Close()
	server := &http.Server{}

	result, err := applyTLSToListener(ln, server, &config.TLSConfig{
		Enabled:  true,
		CertFile: certFile,
		KeyFile:  keyFile,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == ln {
		t.Fatal("expected a new TLS listener, got the same one")
	}
	if server.TLSConfig == nil {
		t.Fatal("expected server TLSConfig to be set")
	}
	result.Close()
}

// ===================================================================
// FHIRSource — pipeline error, subscription notification
// ===================================================================

func TestFHIRSource_PipelineError_Validator(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	errHandler := func(_ context.Context, _ *message.Message) error {
		return fmt.Errorf("pipeline execute: validator: call validate in dist/ch/validator.js: Invalid patient record")
	}
	if err := src.Start(ctx, errHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()
	resp, err := http.Post("http://"+addr+"/fhir/Patient", "application/fhir+json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for validator error, got %d", resp.StatusCode)
	}
}

func TestFHIRSource_SubscriptionNotification(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir", SubscriptionType: "rest-hook"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()
	bundle := `{"resourceType":"Bundle","type":"subscription-notification"}`
	resp, err := http.Post("http://"+addr+"/fhir/subscription-notification", "application/fhir+json", strings.NewReader(bundle))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if capture.count() != 1 {
		t.Fatalf("expected 1 message, got %d", capture.count())
	}
}

// ===================================================================
// FHIRPollSource — poll with mock server
// ===================================================================

func TestFHIRPollSource_Poll_MockFHIRServer_Bundle(t *testing.T) {
	bundle := map[string]any{
		"resourceType": "Bundle",
		"type":         "searchset",
		"entry": []any{
			map[string]any{
				"resource": map[string]any{
					"resourceType": "Patient",
					"id":           "pat-1",
					"name":         []any{map[string]any{"family": "Smith"}},
				},
			},
			map[string]any{
				"resource": map[string]any{
					"resourceType": "Patient",
					"id":           "pat-2",
					"name":         []any{map[string]any{"family": "Doe"}},
				},
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(bundle)
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   ts.URL,
		Resources: []string{"Patient"},
	}, testLogger())

	cap := &msgCapture{}
	err := src.poll(context.Background(), cap.handler(), "", "")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(cap.msgs))
	}
}

func TestFHIRPollSource_StartStop(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/fhir+json")
		json.NewEncoder(w).Encode(map[string]any{
			"resourceType": "Bundle",
			"type":         "searchset",
			"entry":        []any{},
		})
	}))
	defer ts.Close()

	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:      ts.URL,
		Resources:    []string{"Patient"},
		PollInterval: "100ms",
	}, testLogger())

	err := src.Start(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// ===================================================================
// FHIRSubscriptionSource — rest-hook
// ===================================================================

func TestFHIRSubscriptionSource_RestHook_PostNotification(t *testing.T) {
	cap := &msgCapture{}
	src := NewFHIRSubscriptionSource(&config.FHIRSubscriptionListener{
		ChannelType:    "rest-hook",
		Port:           0,
		Path:           "/fhir/notify",
		Version:        "R4",
		SubscriptionID: "sub-test-123",
	}, testLogger())

	err := src.Start(context.Background(), cap.handler())
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(context.Background())

	addr := src.Addr()
	if addr == "" {
		t.Fatal("expected non-empty addr")
	}

	notification := map[string]any{
		"resourceType": "Bundle",
		"type":         "subscription-notification",
		"subscription": "Subscription/sub-test-123",
		"eventNumber":  float64(42),
	}
	body, _ := json.Marshal(notification)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/fhir/notify", addr),
		"application/fhir+json",
		strings.NewReader(string(body)),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	time.Sleep(50 * time.Millisecond)
	cap.mu.Lock()
	defer cap.mu.Unlock()

	if len(cap.msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(cap.msgs))
	}

	msg := cap.msgs[0]
	if msg.Transport != "fhir_subscription" {
		t.Errorf("expected transport fhir_subscription, got %q", msg.Transport)
	}
}

// ===================================================================
// KafkaSource / KafkaDest — mock TCP
// ===================================================================

func TestKafkaDest_Produce_MockTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	received := make(chan []byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 65536)
		n, _ := conn.Read(buf)
		if n > 0 {
			received <- buf[:n]
		}
		resp := make([]byte, 4+4)
		resp[3] = 4
		resp[7] = 201
		conn.Write(resp)
	}()

	addr := ln.Addr().(*net.TCPAddr)
	kd := NewKafkaDest("test", &config.KafkaDestConfig{
		Brokers: []string{fmt.Sprintf("127.0.0.1:%d", addr.Port)},
		Topic:   "test-topic",
	}, testLogger())

	msg := message.New("ch1", []byte("hello-produce"))
	resp, err := kd.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	select {
	case data := <-received:
		if len(data) == 0 {
			t.Fatal("expected non-empty data sent to broker")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for produce data")
	}
}

// ===================================================================
// EmailSource — mock IMAP/POP3
// ===================================================================

func TestEmailSource_MockIMAPServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte("* OK IMAP4rev1 Service Ready\r\n"))
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "A001 LOGIN") {
				conn.Write([]byte("A001 OK LOGIN completed\r\n"))
			} else if strings.HasPrefix(line, "A002 SELECT") {
				conn.Write([]byte("* 0 EXISTS\r\n"))
				conn.Write([]byte("A002 OK SELECT completed\r\n"))
			} else if strings.HasPrefix(line, "A003 SEARCH") {
				conn.Write([]byte("* SEARCH\r\n"))
				conn.Write([]byte("A003 OK SEARCH completed\r\n"))
			} else if strings.HasPrefix(line, "A099 LOGOUT") {
				conn.Write([]byte("* BYE\r\nA099 OK LOGOUT\r\n"))
				return
			}
		}
	}()

	cfg := &config.EmailListener{
		Host: "127.0.0.1",
		Port: port,
		Auth: &config.AuthConfig{Username: "user", Password: "pass"},
	}
	src := NewEmailSource(cfg, testLogger())
	err = src.pollIMAP(context.Background(), noopHandler)
	if err != nil {
		t.Fatalf("pollIMAP: %v", err)
	}
}

// ===================================================================
// FileSource — additional tests
// ===================================================================

func TestFileSource_DeleteModeNoMoveTo(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "del.txt"), []byte("delete-me"), 0o644)

	cfg := &config.FileListener{
		Directory:    dir,
		FilePattern:  "*.txt",
		PollInterval: "100ms",
	}
	src := NewFileSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}

	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	src.Stop(ctx)

	if capture.count() != 1 {
		t.Fatalf("expected 1, got %d", capture.count())
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("expected file to be deleted, but %d remaining", len(entries))
	}
}

func TestFileSource_SortByModified(t *testing.T) {
	dir := t.TempDir()

	os.WriteFile(filepath.Join(dir, "b.txt"), []byte("second"), 0o644)
	time.Sleep(20 * time.Millisecond)
	os.WriteFile(filepath.Join(dir, "a.txt"), []byte("first"), 0o644)

	cfg := &config.FileListener{
		Directory:    dir,
		FilePattern:  "*.txt",
		PollInterval: "100ms",
		SortBy:       "modified",
	}
	src := NewFileSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}

	src.Start(ctx, capture.handler())
	time.Sleep(300 * time.Millisecond)
	src.Stop(ctx)

	if capture.count() != 2 {
		t.Fatalf("expected 2, got %d", capture.count())
	}
	if string(capture.get(0).Raw) != "second" {
		t.Fatalf("expected 'second' first (older mtime), got %q", string(capture.get(0).Raw))
	}
}

// ===================================================================
// TCPSource — raw mode multiple lines, MLLP multiple messages
// ===================================================================

func TestTCPSource_RawModeMultipleLines(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	_, portStr, _ := net.SplitHostPort(addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	cfg := &config.TCPListener{Port: port, Mode: "raw", TimeoutMs: 5000}
	src := NewTCPSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Write([]byte("line1\nline2\nline3\n"))
	conn.Close()

	time.Sleep(200 * time.Millisecond)

	if capture.count() != 3 {
		t.Fatalf("expected 3 messages, got %d", capture.count())
	}
	if string(capture.get(0).Raw) != "line1" {
		t.Fatalf("expected 'line1', got %q", string(capture.get(0).Raw))
	}
}

func TestTCPSource_MLLPMultipleMessages(t *testing.T) {
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
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	for i := 0; i < 3; i++ {
		hl7 := fmt.Sprintf("MSH|^~\\&|S|F|R|F|20230101||ADT^A01|%d|P|2.5\r", i)
		var buf bytes.Buffer
		buf.WriteByte(0x0B)
		buf.WriteString(hl7)
		buf.WriteByte(0x1C)
		buf.WriteByte(0x0D)
		conn.Write(buf.Bytes())
	}
	conn.Close()

	time.Sleep(200 * time.Millisecond)

	if capture.count() != 3 {
		t.Fatalf("expected 3 MLLP messages, got %d", capture.count())
	}
}

// ===================================================================
// IHESource — XDS registry, generic profile
// ===================================================================

func TestIHESource_XDSRegistry(t *testing.T) {
	cfg := &config.IHEListener{Profile: "xds_registry", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()

	resp, err := http.Post("http://"+addr+"/xds/registry/register", "text/xml", strings.NewReader("<doc/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if capture.count() != 1 {
		t.Fatalf("expected 1 message, got %d", capture.count())
	}
	if capture.get(0).Metadata["ihe_transaction"] != "RegisterDocumentSet" {
		t.Fatalf("expected RegisterDocumentSet, got %v", capture.get(0).Metadata["ihe_transaction"])
	}
}

// ===================================================================
// SOAPSource — handler error returns fault
// ===================================================================

func TestSOAPSource_HandlerErrorReturnsFault(t *testing.T) {
	cfg := &config.SOAPListener{Port: 0}
	src := NewSOAPSource(cfg, testLogger())

	ctx := context.Background()
	handler := func(_ context.Context, _ *message.Message) error {
		return fmt.Errorf("processing failed")
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	req, _ := http.NewRequest("POST", "http://"+addr+"/", strings.NewReader("<soap/>"))
	req.Header.Set("Content-Type", "text/xml")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "processing failed") {
		t.Fatalf("expected fault with error message, got %q", string(body))
	}
}

// ===================================================================
// HTTPDest — path params, query params (httptest)
// ===================================================================

func TestHTTPDest_PathParams_URLEncoding(t *testing.T) {
	var receivedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:        server.URL + "/api/{resource}/{id}",
		PathParams: map[string]string{"resource": "Patient", "id": "abc/123"},
	}
	dest := NewHTTPDest("test-path-enc", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if !strings.Contains(receivedPath, "Patient") {
		t.Fatalf("expected Patient in path, got %q", receivedPath)
	}
}

// ===================================================================
// FHIRDest — Send with httptest
// ===================================================================

func TestFHIRDest_Send_DeleteHasNoBody(t *testing.T) {
	var receivedMethod string
	var bodyLen int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		b, _ := io.ReadAll(r.Body)
		bodyLen = len(b)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: srv.URL + "/fhir"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Patient","id":"1"}`))
	msg.Metadata["fhir_operation"] = "delete"
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if receivedMethod != "DELETE" {
		t.Fatalf("expected DELETE, got %s", receivedMethod)
	}
	if bodyLen != 0 {
		t.Fatalf("expected empty body for DELETE, got %d bytes", bodyLen)
	}
}

// ===================================================================
// Concurrent access
// ===================================================================

func TestPathRouter_ConcurrentAccess(t *testing.T) {
	router := newPathRouter()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			path := fmt.Sprintf("/path-%d", idx)
			router.Register(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
			}))
		}(i)
	}
	wg.Wait()

	if router.Len() != 50 {
		t.Fatalf("expected 50 registered paths, got %d", router.Len())
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", fmt.Sprintf("/path-%d", idx), nil)
			router.ServeHTTP(rec, req)
			if rec.Code != 200 {
				t.Errorf("expected 200 for /path-%d, got %d", idx, rec.Code)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			router.Deregister(fmt.Sprintf("/path-%d", idx))
		}(i)
	}
	wg.Wait()

	if router.Len() != 0 {
		t.Fatalf("expected 0 after deregister all, got %d", router.Len())
	}
}
