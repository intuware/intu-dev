package connector

import (
	"context"
	"crypto/tls"
	"fmt"
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
// pathRouter Tests
// ===================================================================

func TestPathRouter_RegisterAndServe(t *testing.T) {
	router := newPathRouter()
	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	if err := router.Register("/test", handler); err != nil {
		t.Fatalf("Register: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	router.ServeHTTP(rec, req)

	if !called {
		t.Fatal("expected handler to be called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestPathRouter_UnregisteredPathReturns404(t *testing.T) {
	router := newPathRouter()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/nonexistent", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestPathRouter_DuplicateRegisterReturnsError(t *testing.T) {
	router := newPathRouter()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	if err := router.Register("/dup", handler); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := router.Register("/dup", handler); err == nil {
		t.Fatal("expected error on duplicate Register")
	}
}

func TestPathRouter_Deregister(t *testing.T) {
	router := newPathRouter()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	router.Register("/remove-me", handler)
	if router.Len() != 1 {
		t.Fatalf("expected Len=1, got %d", router.Len())
	}

	router.Deregister("/remove-me")
	if router.Len() != 0 {
		t.Fatalf("expected Len=0, got %d", router.Len())
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/remove-me", nil)
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after Deregister, got %d", rec.Code)
	}
}

func TestPathRouter_Len(t *testing.T) {
	router := newPathRouter()
	if router.Len() != 0 {
		t.Fatalf("expected Len=0, got %d", router.Len())
	}

	router.Register("/a", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	router.Register("/b", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	if router.Len() != 2 {
		t.Fatalf("expected Len=2, got %d", router.Len())
	}
}

func TestPathRouter_MultiplePathsDispatched(t *testing.T) {
	router := newPathRouter()
	var hitA, hitB bool

	router.Register("/a", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitA = true
		w.WriteHeader(200)
	}))
	router.Register("/b", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitB = true
		w.WriteHeader(201)
	}))

	recA := httptest.NewRecorder()
	router.ServeHTTP(recA, httptest.NewRequest("GET", "/a", nil))
	if !hitA || recA.Code != 200 {
		t.Fatalf("expected /a to be dispatched correctly")
	}

	recB := httptest.NewRecorder()
	router.ServeHTTP(recB, httptest.NewRequest("GET", "/b", nil))
	if !hitB || recB.Code != 201 {
		t.Fatalf("expected /b to be dispatched correctly")
	}
}

// ===================================================================
// SharedHTTPListener Tests
// ===================================================================

func TestSharedHTTPListener_AcquireAndRelease(t *testing.T) {
	ResetSharedHTTPListeners()
	defer ResetSharedHTTPListeners()

	sl, err := acquireSharedHTTPListener(0, nil, testLogger())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	port := sl.listener.Addr().(*net.TCPAddr).Port
	if port == 0 {
		t.Fatal("expected non-zero port")
	}
	if sl.refs != 1 {
		t.Fatalf("expected refs=1, got %d", sl.refs)
	}

	releaseSharedHTTPListener(port, context.Background())
}

func TestSharedHTTPListener_SharedPortRefsCount(t *testing.T) {
	ResetSharedHTTPListeners()
	defer ResetSharedHTTPListeners()

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	time.Sleep(50 * time.Millisecond)

	sl1, err := acquireSharedHTTPListener(port, nil, testLogger())
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}

	sl2, err := acquireSharedHTTPListener(port, nil, testLogger())
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if sl1 != sl2 {
		t.Fatal("expected same sharedHTTPListener instance for same port")
	}

	sl1.mu.Lock()
	refs := sl1.refs
	sl1.mu.Unlock()
	if refs != 2 {
		t.Fatalf("expected refs=2, got %d", refs)
	}

	releaseSharedHTTPListener(port, context.Background())
	sl1.mu.Lock()
	refs = sl1.refs
	sl1.mu.Unlock()
	if refs != 1 {
		t.Fatalf("expected refs=1 after first release, got %d", refs)
	}

	releaseSharedHTTPListener(port, context.Background())
}

func TestSharedHTTPListener_RegisterPathAndServe(t *testing.T) {
	ResetSharedHTTPListeners()
	defer ResetSharedHTTPListeners()

	sl, err := acquireSharedHTTPListener(0, nil, testLogger())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	port := sl.listener.Addr().(*net.TCPAddr).Port

	var called bool
	sl.router.Register("/hello", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(200)
		w.Write([]byte("world"))
	}))

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/hello", port))
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

	releaseSharedHTTPListener(port, context.Background())
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

func TestReleaseSharedHTTPListener_NonexistentPort(t *testing.T) {
	releaseSharedHTTPListener(99999, context.Background())
}

// ===================================================================
// authenticateHTTP — additional edge-case tests
// ===================================================================

func TestAuthenticateHTTP_EmptyType(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	if !authenticateHTTP(req, &config.AuthConfig{Type: ""}) {
		t.Fatal("empty type should allow all")
	}
}

func TestAuthenticateHTTP_UnknownType(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	if !authenticateHTTP(req, &config.AuthConfig{Type: "unknown_custom"}) {
		t.Fatal("unknown type should allow all (default branch)")
	}
}

func TestAuthenticateHTTP_APIKeyNoHeaderNoQueryParam(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	if authenticateHTTP(req, &config.AuthConfig{Type: "api_key", Key: "k"}) {
		t.Fatal("api_key with no header and no query_param should fail")
	}
}

func TestAuthenticateHTTP_BearerEmptyAuthHeader(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	if authenticateHTTP(req, &config.AuthConfig{Type: "bearer", Token: "tok"}) {
		t.Fatal("missing Authorization header should fail bearer auth")
	}
}

func TestAuthenticateHTTP_BasicNoCredentials(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	if authenticateHTTP(req, &config.AuthConfig{Type: "basic", Username: "u", Password: "p"}) {
		t.Fatal("missing credentials should fail basic auth")
	}
}

func TestAuthenticateHTTP_APIKeyWrongHeaderValue(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	req.Header.Set("X-Key", "wrong")
	if authenticateHTTP(req, &config.AuthConfig{Type: "api_key", Key: "correct", Header: "X-Key"}) {
		t.Fatal("wrong API key value should fail")
	}
}

func TestAuthenticateHTTP_APIKeyWrongQueryParamValue(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/?key=wrong", nil)
	if authenticateHTTP(req, &config.AuthConfig{Type: "api_key", Key: "correct", QueryParam: "key"}) {
		t.Fatal("wrong API key query param should fail")
	}
}

func TestAuthenticateHTTP_MTLSWithEmptyPeerCertificates(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	req.TLS = &tls.ConnectionState{}
	if authenticateHTTP(req, &config.AuthConfig{Type: "mtls"}) {
		t.Fatal("mTLS with empty PeerCertificates should fail")
	}
}

// ===================================================================
// ChannelBus Tests
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

func TestChannelBus_PublishToNonexistentChannel(t *testing.T) {
	bus := GetChannelBus()
	bus.Publish("nonexistent-channel-12345", message.New("", []byte("nothing")))
}

func TestChannelBus_BufferFullDropsMessage(t *testing.T) {
	bus := GetChannelBus()
	chID := fmt.Sprintf("full-buf-%d", time.Now().UnixNano())
	sub := bus.Subscribe(chID)

	for i := 0; i < 110; i++ {
		bus.Publish(chID, message.New("", []byte(fmt.Sprintf("msg-%d", i))))
	}

	received := 0
	for {
		select {
		case <-sub:
			received++
		default:
			goto done
		}
	}
done:
	if received > 100 {
		t.Fatalf("expected at most 100 buffered messages, got %d", received)
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
// ChannelSource — additional tests
// ===================================================================

func TestChannelSource_TransportSetToChannel(t *testing.T) {
	chID := fmt.Sprintf("transport-test-%d", time.Now().UnixNano())
	cfg := &config.ChannelListener{SourceChannelID: chID}
	src := NewChannelSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}

	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	bus := GetChannelBus()
	bus.Publish(chID, message.New("", []byte("test")))
	time.Sleep(100 * time.Millisecond)

	if capture.count() != 1 {
		t.Fatalf("expected 1, got %d", capture.count())
	}
	if capture.get(0).Transport != "channel" {
		t.Fatalf("expected transport 'channel', got %q", capture.get(0).Transport)
	}
}

func TestChannelSource_StopBeforeStart(t *testing.T) {
	cfg := &config.ChannelListener{SourceChannelID: "never-started"}
	src := NewChannelSource(cfg, testLogger())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop before Start should not error: %v", err)
	}
}

func TestChannelSource_Type(t *testing.T) {
	src := NewChannelSource(&config.ChannelListener{SourceChannelID: "x"}, testLogger())
	if src.Type() != "channel" {
		t.Fatalf("expected 'channel', got %q", src.Type())
	}
}

// ===================================================================
// ChannelDest — additional tests
// ===================================================================

func TestChannelDest_TransportStamping(t *testing.T) {
	targetCh := fmt.Sprintf("stamp-%d", time.Now().UnixNano())
	dest := NewChannelDest("test", targetCh, testLogger())

	msg := message.New("orig", []byte("data"))
	msg.Transport = "http"
	msg.HTTP = &message.HTTPMeta{Method: "POST"}

	_, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	if msg.Transport != "channel" {
		t.Fatalf("expected transport stamped to 'channel', got %q", msg.Transport)
	}
}

func TestChannelDest_Stop(t *testing.T) {
	dest := NewChannelDest("test", "t", testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ===================================================================
// StubSource Tests
// ===================================================================

func TestStubSource_CustomType(t *testing.T) {
	src := NewStubSource("my-custom", testLogger())
	if src.Type() != "my-custom" {
		t.Fatalf("expected 'my-custom', got %q", src.Type())
	}
}

func TestStubSource_StartAndStop(t *testing.T) {
	src := NewStubSource("test-stub", testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := src.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStubSource_NoMessagesReceived(t *testing.T) {
	src := NewStubSource("stub", testLogger())
	capture := &msgCapture{}

	ctx := context.Background()
	src.Start(ctx, capture.handler())
	time.Sleep(50 * time.Millisecond)
	src.Stop(ctx)

	if capture.count() != 0 {
		t.Fatalf("stub should not receive messages, got %d", capture.count())
	}
}

// ===================================================================
// LogDest — transport stamping
// ===================================================================

func TestLogDest_TransportStamping(t *testing.T) {
	dest := NewLogDest("test-log", testLogger())

	msg := message.New("ch1", []byte("data"))
	msg.Transport = "http"
	msg.HTTP = &message.HTTPMeta{Method: "POST"}

	_, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if msg.Transport != "log" {
		t.Fatalf("expected transport 'log', got %q", msg.Transport)
	}
}

// ===================================================================
// FileDest — additional tests
// ===================================================================

func TestFileDest_TransportStamping(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.FileDestMapConfig{Directory: dir}
	dest := NewFileDest("test", cfg, testLogger())

	msg := message.New("ch1", []byte("data"))
	msg.Transport = "tcp"
	msg.TCP = &message.TCPMeta{RemoteAddr: "1.2.3.4"}

	_, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	if msg.Transport != "file" {
		t.Fatalf("expected transport 'file', got %q", msg.Transport)
	}
	if msg.File == nil {
		t.Fatal("expected File meta to be populated")
	}
	if msg.File.Directory != dir {
		t.Fatalf("expected directory %q, got %q", dir, msg.File.Directory)
	}
}

func TestFileDest_EmptyDirectoryDefaultsToDot(t *testing.T) {
	origDir, _ := os.Getwd()
	tmpDir := t.TempDir()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cfg := &config.FileDestMapConfig{Directory: ""}
	dest := NewFileDest("test", cfg, testLogger())

	msg := message.New("ch1", []byte("data"))
	_, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	entries, _ := os.ReadDir(".")
	found := false
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "ch1_") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected file in current directory")
	}
}

func TestFileDest_StopIsNoop(t *testing.T) {
	cfg := &config.FileDestMapConfig{Directory: t.TempDir()}
	dest := NewFileDest("test", cfg, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
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

func TestFileSource_InvalidPollIntervalUsesDefault(t *testing.T) {
	dir := t.TempDir()

	cfg := &config.FileListener{
		Directory:    dir,
		PollInterval: "not-a-duration",
	}
	src := NewFileSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	src.Stop(ctx)
}

func TestFileSource_NonLocalSchemeStillWorks(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "f.txt"), []byte("data"), 0o644)

	cfg := &config.FileListener{
		Directory:    dir,
		Scheme:       "s3",
		PollInterval: "100ms",
	}
	src := NewFileSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	src.Start(ctx, capture.handler())
	time.Sleep(300 * time.Millisecond)
	src.Stop(ctx)

	if capture.count() != 1 {
		t.Fatalf("expected 1 message even with non-local scheme, got %d", capture.count())
	}
}

func TestFileSource_TypeIncludesScheme(t *testing.T) {
	cfg := &config.FileListener{Directory: "/tmp", Scheme: "S3"}
	src := NewFileSource(cfg, testLogger())
	if src.Type() != "file/s3" {
		t.Fatalf("expected 'file/s3', got %q", src.Type())
	}
}

func TestFileSource_TypeEmptyScheme(t *testing.T) {
	cfg := &config.FileListener{Directory: "/tmp"}
	src := NewFileSource(cfg, testLogger())
	if src.Type() != "file/" {
		t.Fatalf("expected 'file/', got %q", src.Type())
	}
}

func TestFileSource_StopBeforeStart(t *testing.T) {
	cfg := &config.FileListener{Directory: "/tmp", PollInterval: "10s"}
	src := NewFileSource(cfg, testLogger())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ===================================================================
// Factory CreateSource — nil config tests (not covered in existing tests)
// ===================================================================

func TestFactory_NilTCPConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateSource(config.ListenerConfig{Type: "tcp"})
	if err == nil {
		t.Fatal("expected error for nil tcp config")
	}
}

func TestFactory_NilFileConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateSource(config.ListenerConfig{Type: "file"})
	if err == nil {
		t.Fatal("expected error for nil file config")
	}
}

func TestFactory_NilChannelConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateSource(config.ListenerConfig{Type: "channel"})
	if err == nil {
		t.Fatal("expected error for nil channel config")
	}
}

func TestFactory_NilSFTPConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateSource(config.ListenerConfig{Type: "sftp"})
	if err == nil {
		t.Fatal("expected error for nil sftp config")
	}
}

func TestFactory_NilDatabaseConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateSource(config.ListenerConfig{Type: "database"})
	if err == nil {
		t.Fatal("expected error for nil database config")
	}
}

func TestFactory_NilKafkaConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateSource(config.ListenerConfig{Type: "kafka"})
	if err == nil {
		t.Fatal("expected error for nil kafka config")
	}
}

func TestFactory_NilEmailConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateSource(config.ListenerConfig{Type: "email"})
	if err == nil {
		t.Fatal("expected error for nil email config")
	}
}

func TestFactory_NilDICOMConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateSource(config.ListenerConfig{Type: "dicom"})
	if err == nil {
		t.Fatal("expected error for nil dicom config")
	}
}

func TestFactory_NilSOAPConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateSource(config.ListenerConfig{Type: "soap"})
	if err == nil {
		t.Fatal("expected error for nil soap config")
	}
}

func TestFactory_NilFHIRConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateSource(config.ListenerConfig{Type: "fhir"})
	if err == nil {
		t.Fatal("expected error for nil fhir config")
	}
}

func TestFactory_NilIHEConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateSource(config.ListenerConfig{Type: "ihe"})
	if err == nil {
		t.Fatal("expected error for nil ihe config")
	}
}

// ===================================================================
// Factory CreateDestination — sftp nil config
// ===================================================================

func TestFactory_NilSFTPDestConfig(t *testing.T) {
	f := NewFactory(testLogger())
	_, err := f.CreateDestination("test", config.Destination{Type: "sftp"})
	if err == nil {
		t.Fatal("expected error for nil sftp dest config")
	}
}

// ===================================================================
// applyTLSToListener Tests
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

func TestApplyTLSToListener_InvalidCertReturnsError(t *testing.T) {
	ln, _ := net.Listen("tcp", ":0")
	defer ln.Close()
	server := &http.Server{}

	_, err := applyTLSToListener(ln, server, &config.TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	})
	if err == nil {
		t.Fatal("expected error for invalid cert paths")
	}
}

// ===================================================================
// HTTP Source — shared listener path-based routing
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

// ===================================================================
// ChannelSource handler error path
// ===================================================================

func TestChannelSource_HandlerError(t *testing.T) {
	chID := fmt.Sprintf("err-handler-%d", time.Now().UnixNano())
	cfg := &config.ChannelListener{SourceChannelID: chID}
	src := NewChannelSource(cfg, testLogger())

	ctx := context.Background()
	handler := func(_ context.Context, _ *message.Message) error {
		return fmt.Errorf("simulated handler error")
	}

	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	bus := GetChannelBus()
	bus.Publish(chID, message.New("", []byte("err-test")))
	time.Sleep(100 * time.Millisecond)
}

// ===================================================================
// Concurrent access tests
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

func TestChannelBus_ConcurrentPublishSubscribe(t *testing.T) {
	bus := GetChannelBus()
	chID := fmt.Sprintf("concurrent-%d", time.Now().UnixNano())

	subs := make([]chan *message.Message, 5)
	for i := range subs {
		subs[i] = bus.Subscribe(chID)
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bus.Publish(chID, message.New("", []byte(fmt.Sprintf("msg-%d", idx))))
		}(i)
	}
	wg.Wait()

	time.Sleep(50 * time.Millisecond)
	for i, sub := range subs {
		count := 0
	drain:
		for {
			select {
			case <-sub:
				count++
			default:
				break drain
			}
		}
		if count != 20 {
			t.Errorf("sub[%d]: expected 20 messages, got %d", i, count)
		}
	}
}
