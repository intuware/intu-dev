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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// readMLLP / readRawTCP unit tests
// ===================================================================

func TestReadMLLP_ValidMessage(t *testing.T) {
	payload := "MSH|^~\\&|S|F|R|F|20230101||ADT^A01|123|P|2.5\r"
	var buf bytes.Buffer
	buf.WriteByte(0x0B)
	buf.WriteString(payload)
	buf.WriteByte(0x1C)
	buf.WriteByte(0x0D)

	reader := bufio.NewReader(&buf)
	data, err := readMLLP(reader)
	if err != nil {
		t.Fatalf("readMLLP: %v", err)
	}
	if string(data) != payload {
		t.Fatalf("expected %q, got %q", payload, string(data))
	}
}

func TestReadMLLP_MissingStartBlock(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0x0A) // not 0x0B
	buf.WriteString("data")
	buf.WriteByte(0x1C)
	buf.WriteByte(0x0D)

	reader := bufio.NewReader(&buf)
	_, err := readMLLP(reader)
	if err == nil {
		t.Fatal("expected error for missing start block")
	}
	if !strings.Contains(err.Error(), "expected MLLP start block") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadMLLP_EmptyReader(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader(nil))
	_, err := readMLLP(reader)
	if err == nil {
		t.Fatal("expected error for empty reader")
	}
}

func TestReadMLLP_EndBlockWithoutCR(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0x0B)
	buf.WriteString("msg")
	buf.WriteByte(0x1C)
	// EOF instead of CR

	reader := bufio.NewReader(&buf)
	data, err := readMLLP(reader)
	if err != nil {
		t.Fatalf("readMLLP: %v", err)
	}
	if string(data) != "msg" {
		t.Fatalf("expected 'msg', got %q", string(data))
	}
}

func TestReadRawTCP_BasicLine(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("hello world\n"))
	data, err := readRawTCP(reader)
	if err != nil {
		t.Fatalf("readRawTCP: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("expected 'hello world', got %q", string(data))
	}
}

func TestReadRawTCP_EmptyLine(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("\n"))
	data, err := readRawTCP(reader)
	if err != nil {
		t.Fatalf("readRawTCP: %v", err)
	}
	if string(data) != "" {
		t.Fatalf("expected empty, got %q", string(data))
	}
}

func TestReadRawTCP_NoNewline(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("no newline"))
	_, err := readRawTCP(reader)
	if err == nil {
		t.Fatal("expected error for missing newline (EOF)")
	}
}

// ===================================================================
// buildMLLPACK unit tests
// ===================================================================

func TestBuildMLLPACK_SuccessCode(t *testing.T) {
	cfg := &config.TCPListener{
		Mode: "mllp",
		ACK:  &config.ACKConfig{Auto: true, SuccessCode: "CA", ErrorCode: "CR"},
	}
	src := &TCPSource{cfg: cfg, logger: testLogger()}

	msgData := []byte("MSH|^~\\&|S|F|R|F|20230101||ADT^A01|CTRL123|P|2.5\r")
	ack := src.buildMLLPACK(msgData, nil)

	if ack[0] != 0x0B {
		t.Fatal("expected MLLP start block")
	}
	if ack[len(ack)-2] != 0x1C || ack[len(ack)-1] != 0x0D {
		t.Fatal("expected MLLP end block")
	}
	ackStr := string(ack[1 : len(ack)-2])
	if !strings.Contains(ackStr, "MSA|CA|CTRL123") {
		t.Fatalf("expected MSA|CA|CTRL123, got %q", ackStr)
	}
}

func TestBuildMLLPACK_ErrorCode(t *testing.T) {
	cfg := &config.TCPListener{
		Mode: "mllp",
		ACK:  &config.ACKConfig{Auto: true, SuccessCode: "CA", ErrorCode: "CR"},
	}
	src := &TCPSource{cfg: cfg, logger: testLogger()}

	msgData := []byte("MSH|^~\\&|S|F|R|F|20230101||ADT^A01|555|P|2.5\r")
	ack := src.buildMLLPACK(msgData, fmt.Errorf("handler failed"))

	ackStr := string(ack[1 : len(ack)-2])
	if !strings.Contains(ackStr, "MSA|CR|555") {
		t.Fatalf("expected MSA|CR|555, got %q", ackStr)
	}
}

func TestBuildMLLPACK_DefaultCodes(t *testing.T) {
	cfg := &config.TCPListener{
		Mode: "mllp",
		ACK:  &config.ACKConfig{Auto: true},
	}
	src := &TCPSource{cfg: cfg, logger: testLogger()}

	msgData := []byte("MSH|^~\\&|S|F|R|F|20230101||ADT^A01|777|P|2.5\r")

	ack := src.buildMLLPACK(msgData, nil)
	if !strings.Contains(string(ack), "MSA|AA|777") {
		t.Fatal("expected default success code AA")
	}

	ackErr := src.buildMLLPACK(msgData, fmt.Errorf("fail"))
	if !strings.Contains(string(ackErr), "MSA|AE|777") {
		t.Fatal("expected default error code AE")
	}
}

// ===================================================================
// extractHL7ControlID edge cases
// ===================================================================

func TestExtractHL7ControlID_ShortMSH(t *testing.T) {
	msg := []byte("MSH|^~\\&|S|F|R|F\r")
	id := extractHL7ControlID(msg)
	if id != "0" {
		t.Fatalf("expected '0' for short MSH, got %q", id)
	}
}

func TestExtractHL7ControlID_MultipleSegments(t *testing.T) {
	msg := []byte("PID|1\rMSH|^~\\&|S|F|R|F|20230101||ADT^A01|MULTI99|P|2.5\r")
	id := extractHL7ControlID(msg)
	if id != "MULTI99" {
		t.Fatalf("expected 'MULTI99', got %q", id)
	}
}

// ===================================================================
// extractHL7AckCode edge cases
// ===================================================================

func TestExtractHL7AckCode_CR(t *testing.T) {
	ack := []byte("MSH|^~\\&|R|F|S|F|20230101||ACK||P|2.5\rMSA|CR|12345\r")
	code := extractHL7AckCode(ack)
	if code != "CR" {
		t.Fatalf("expected CR, got %q", code)
	}
}

func TestExtractHL7AckCode_AR(t *testing.T) {
	ack := []byte("MSH|^~\\&\rMSA|AR|99\r")
	code := extractHL7AckCode(ack)
	if code != "AR" {
		t.Fatalf("expected AR, got %q", code)
	}
}

func TestExtractHL7AckCode_ShortMSA(t *testing.T) {
	ack := []byte("MSA\r")
	code := extractHL7AckCode(ack)
	if code != "" {
		t.Fatalf("expected empty for single-field MSA, got %q", code)
	}
}

// ===================================================================
// mergeMaps unit tests
// ===================================================================

func TestMergeMaps_BothNil(t *testing.T) {
	result := mergeMaps(nil, nil)
	if len(result) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(result))
	}
}

func TestMergeMaps_BaseOnly(t *testing.T) {
	base := map[string]string{"a": "1", "b": "2"}
	result := mergeMaps(base, nil)
	if result["a"] != "1" || result["b"] != "2" {
		t.Fatalf("unexpected: %v", result)
	}
}

func TestMergeMaps_OverrideOnly(t *testing.T) {
	override := map[string]string{"x": "9"}
	result := mergeMaps(nil, override)
	if result["x"] != "9" {
		t.Fatalf("unexpected: %v", result)
	}
}

func TestMergeMaps_OverrideTakesPrecedence(t *testing.T) {
	base := map[string]string{"k": "old", "keep": "yes"}
	override := map[string]string{"k": "new"}
	result := mergeMaps(base, override)
	if result["k"] != "new" {
		t.Fatalf("expected override value 'new', got %q", result["k"])
	}
	if result["keep"] != "yes" {
		t.Fatalf("expected base value 'yes', got %q", result["keep"])
	}
}

// ===================================================================
// HTTPSource additional coverage
// ===================================================================

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

func TestHTTPSource_MultipleMethods(t *testing.T) {
	cfg := &config.HTTPListener{Port: 0, Methods: []string{"POST", "PUT"}}
	src := NewHTTPSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()

	req, _ := http.NewRequest("PUT", "http://"+addr+"/", strings.NewReader("put-data"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for PUT, got %d", resp.StatusCode)
	}

	resp, err = http.Post("http://"+addr+"/", "text/plain", strings.NewReader("post-data"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for POST, got %d", resp.StatusCode)
	}

	req, _ = http.NewRequest("DELETE", "http://"+addr+"/", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for DELETE, got %d", resp.StatusCode)
	}

	if capture.count() != 2 {
		t.Fatalf("expected 2 messages (PUT+POST), got %d", capture.count())
	}
}

func TestHTTPSource_AddrNotStarted(t *testing.T) {
	cfg := &config.HTTPListener{Port: 0}
	src := NewHTTPSource(cfg, testLogger())
	if src.Addr() != "" {
		t.Fatalf("expected empty addr before start, got %q", src.Addr())
	}
}

func TestHTTPSource_StopBeforeStart(t *testing.T) {
	cfg := &config.HTTPListener{Port: 0}
	src := NewHTTPSource(cfg, testLogger())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop before Start should not error: %v", err)
	}
}

func TestHTTPSource_HTTPMetadata(t *testing.T) {
	cfg := &config.HTTPListener{Port: 0}
	src := NewHTTPSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	req, _ := http.NewRequest("POST", "http://"+src.Addr()+"/?foo=bar", strings.NewReader("body"))
	req.Header.Set("X-Custom-Header", "custom-value")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if capture.count() != 1 {
		t.Fatalf("expected 1 message, got %d", capture.count())
	}
	msg := capture.get(0)
	if msg.Transport != "http" {
		t.Fatalf("expected transport 'http', got %q", msg.Transport)
	}
	if msg.HTTP == nil {
		t.Fatal("expected HTTP meta to be set")
	}
	if msg.HTTP.Method != "POST" {
		t.Fatalf("expected method POST, got %q", msg.HTTP.Method)
	}
	if msg.HTTP.QueryParams["foo"] != "bar" {
		t.Fatalf("expected query param foo=bar, got %v", msg.HTTP.QueryParams)
	}
	if msg.HTTP.Headers["X-Custom-Header"] != "custom-value" {
		t.Fatalf("expected custom header, got %v", msg.HTTP.Headers)
	}
}

// ===================================================================
// HTTPDest additional coverage
// ===================================================================

func TestHTTPDest_PUTMethod(t *testing.T) {
	var method string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{URL: server.URL, Method: "PUT"}
	dest := NewHTTPDest("test", cfg, testLogger())

	msg := message.New("ch1", []byte("data"))
	dest.Send(context.Background(), msg)
	if method != "PUT" {
		t.Fatalf("expected PUT, got %s", method)
	}
}

func TestHTTPDest_PathParams(t *testing.T) {
	var receivedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:        server.URL + "/api/{resource}/{id}",
		PathParams: map[string]string{"resource": "Patient", "id": "123"},
	}
	dest := NewHTTPDest("test", cfg, testLogger())

	msg := message.New("ch1", []byte("data"))
	dest.Send(context.Background(), msg)
	if receivedPath != "/api/Patient/123" {
		t.Fatalf("expected /api/Patient/123, got %q", receivedPath)
	}
}

func TestHTTPDest_QueryParams(t *testing.T) {
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.RawQuery
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:         server.URL,
		QueryParams: map[string]string{"page": "2", "limit": "10"},
	}
	dest := NewHTTPDest("test", cfg, testLogger())

	msg := message.New("ch1", []byte("data"))
	dest.Send(context.Background(), msg)
	if !strings.Contains(query, "page=2") || !strings.Contains(query, "limit=10") {
		t.Fatalf("expected query params, got %q", query)
	}
}

func TestHTTPDest_MessageHTTPOverridesConfig(t *testing.T) {
	var receivedHeaders http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:     server.URL,
		Headers: map[string]string{"X-Config": "from-config"},
	}
	dest := NewHTTPDest("test", cfg, testLogger())

	msg := message.New("ch1", []byte("data"))
	msg.HTTP = &message.HTTPMeta{
		Headers: map[string]string{"X-Config": "from-msg", "X-Extra": "extra"},
	}
	dest.Send(context.Background(), msg)

	if receivedHeaders.Get("X-Config") != "from-msg" {
		t.Fatalf("expected message header to override config, got %q", receivedHeaders.Get("X-Config"))
	}
	if receivedHeaders.Get("X-Extra") != "extra" {
		t.Fatalf("expected extra header from message, got %q", receivedHeaders.Get("X-Extra"))
	}
}

// ===================================================================
// TCPDest additional coverage
// ===================================================================

func TestTCPDest_TransportStamping(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		io.ReadAll(conn)
		conn.Close()
	}()

	cfg := &config.TCPDestMapConfig{Host: host, Port: port, Mode: "raw", TimeoutMs: 5000}
	dest := NewTCPDest("test", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	msg.Transport = "http"
	msg.HTTP = &message.HTTPMeta{Method: "POST"}

	dest.Send(context.Background(), msg)
	dest.Stop(context.Background())
	<-done

	if msg.Transport != "tcp" {
		t.Fatalf("expected transport 'tcp', got %q", msg.Transport)
	}
	if msg.TCP == nil {
		t.Fatal("expected TCP meta to be set")
	}
	if msg.HTTP != nil {
		t.Fatal("expected HTTP meta to be cleared")
	}
}

func TestTCPDest_StopWithoutConnection(t *testing.T) {
	cfg := &config.TCPDestMapConfig{Host: "localhost", Port: 0}
	dest := NewTCPDest("test", cfg, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ===================================================================
// SOAPSource additional coverage
// ===================================================================

func TestSOAPSource_GenerateWSDL(t *testing.T) {
	cfg := &config.SOAPListener{Port: 8888, ServiceName: "HL7Service"}
	src := NewSOAPSource(cfg, testLogger())

	wsdl := src.generateWSDL("HL7Service", 8888, "/wsdl")
	if !strings.Contains(wsdl, "HL7Service") {
		t.Fatal("expected service name in WSDL")
	}
	if !strings.Contains(wsdl, ":8888/") {
		t.Fatal("expected port in WSDL location")
	}
	if !strings.Contains(wsdl, "HL7ServicePortType") {
		t.Fatal("expected port type in WSDL")
	}
	if !strings.Contains(wsdl, "HL7ServiceBinding") {
		t.Fatal("expected binding in WSDL")
	}
}

func TestSOAPSource_SoapFaultResponse(t *testing.T) {
	resp := soapFaultResponse("Client", "Invalid request")
	if !strings.Contains(resp, "soap:Client") {
		t.Fatal("expected fault code")
	}
	if !strings.Contains(resp, "Invalid request") {
		t.Fatal("expected fault string")
	}
	if !strings.Contains(resp, "soap:Envelope") {
		t.Fatal("expected SOAP envelope")
	}
}

func TestSOAPSource_SoapSuccessResponse(t *testing.T) {
	resp := soapSuccessResponse()
	if !strings.Contains(resp, "accepted") {
		t.Fatal("expected 'accepted' in success response")
	}
	if !strings.Contains(resp, "soap:Envelope") {
		t.Fatal("expected SOAP envelope")
	}
}

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

func TestSOAPSource_GETOnRootReturns405(t *testing.T) {
	cfg := &config.SOAPListener{Port: 0}
	src := NewSOAPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Get("http://" + addr + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestSOAPSource_DefaultServiceName(t *testing.T) {
	cfg := &config.SOAPListener{Port: 0}
	src := NewSOAPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Get("http://" + addr + "/wsdl")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "IntuService") {
		t.Fatal("expected default service name 'IntuService' in WSDL")
	}
}

func TestSOAPSource_AddrAndType(t *testing.T) {
	cfg := &config.SOAPListener{Port: 0}
	src := NewSOAPSource(cfg, testLogger())

	if src.Addr() != "" {
		t.Fatal("expected empty addr before start")
	}
	if src.Type() != "soap" {
		t.Fatalf("expected type 'soap', got %q", src.Type())
	}
}

// ===================================================================
// IHESource additional coverage
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

	resp, err := http.Post("http://"+addr+"/xds/registry/register", "text/xml", strings.NewReader("<reg/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if capture.count() != 1 {
		t.Fatalf("expected 1, got %d", capture.count())
	}
	if capture.get(0).Metadata["ihe_transaction"] != "RegisterDocumentSet" {
		t.Fatalf("expected RegisterDocumentSet, got %v", capture.get(0).Metadata["ihe_transaction"])
	}
}

func TestIHESource_XDSRegistryQuery(t *testing.T) {
	cfg := &config.IHEListener{Profile: "xds_registry", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Post("http://"+addr+"/xds/registry/query", "text/xml", strings.NewReader("<query/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if capture.get(0).Metadata["ihe_transaction"] != "RegistryStoredQuery" {
		t.Fatalf("expected RegistryStoredQuery, got %v", capture.get(0).Metadata["ihe_transaction"])
	}
}

func TestIHESource_GenericProfile(t *testing.T) {
	cfg := &config.IHEListener{Profile: "custom-profile", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Post("http://"+addr+"/", "text/xml", strings.NewReader("<generic/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if capture.get(0).Metadata["ihe_transaction"] != "GenericRequest" {
		t.Fatalf("expected GenericRequest, got %v", capture.get(0).Metadata["ihe_transaction"])
	}
	if capture.get(0).Metadata["ihe_profile"] != "custom-profile" {
		t.Fatalf("expected custom-profile, got %v", capture.get(0).Metadata["ihe_profile"])
	}
}

func TestIHESource_ProfileGetStatusJSON(t *testing.T) {
	profiles := []string{"xds_repository", "xds_registry", "pix", "pdq"}

	for _, profile := range profiles {
		t.Run(profile, func(t *testing.T) {
			cfg := &config.IHEListener{Profile: profile, Port: 0}
			src := NewIHESource(cfg, testLogger())

			ctx := context.Background()
			if err := src.Start(ctx, noopHandler); err != nil {
				t.Fatalf("start: %v", err)
			}
			defer src.Stop(ctx)

			addr := src.listener.Addr().String()
			resp, err := http.Get("http://" + addr + "/")
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			var status map[string]string
			json.Unmarshal(body, &status)
			if status["profile"] != profile {
				t.Fatalf("expected profile %q, got %v", profile, status["profile"])
			}
			if status["status"] != "active" {
				t.Fatalf("expected status 'active', got %v", status["status"])
			}
		})
	}
}

func TestIHESource_XDSRepositoryRetrieve(t *testing.T) {
	cfg := &config.IHEListener{Profile: "xds_repository", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Post("http://"+addr+"/xds/repository/retrieve", "text/xml", strings.NewReader("<retrieve/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if capture.get(0).Metadata["ihe_transaction"] != "RetrieveDocumentSet" {
		t.Fatalf("expected RetrieveDocumentSet")
	}
}

func TestIHESource_PIXFeed(t *testing.T) {
	cfg := &config.IHEListener{Profile: "pix", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Post("http://"+addr+"/pix/feed", "text/xml", strings.NewReader("<feed/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if capture.get(0).Metadata["ihe_transaction"] != "PatientIdentityFeed" {
		t.Fatalf("expected PatientIdentityFeed")
	}
}

func TestIHESource_HandlerError(t *testing.T) {
	cfg := &config.IHEListener{Profile: "pix", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	handler := func(_ context.Context, _ *message.Message) error {
		return fmt.Errorf("ihe error")
	}
	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Post("http://"+addr+"/pix/query", "text/xml", strings.NewReader("<q/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "ihe error") {
		t.Fatalf("expected error message, got %q", string(body))
	}
}

func TestIHESource_MethodNotAllowed(t *testing.T) {
	cfg := &config.IHEListener{Profile: "pix", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	req, _ := http.NewRequest("GET", "http://"+addr+"/pix/query", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestIHESource_AddrAndType(t *testing.T) {
	cfg := &config.IHEListener{Profile: "pix", Port: 0}
	src := NewIHESource(cfg, testLogger())
	if src.Addr() != "" {
		t.Fatal("expected empty addr before start")
	}
	if src.Type() != "ihe/pix" {
		t.Fatalf("expected 'ihe/pix', got %q", src.Type())
	}
}

// ===================================================================
// FHIRSource additional coverage
// ===================================================================

func TestFHIRSource_ClassifyPipelineError_Validator(t *testing.T) {
	err := fmt.Errorf("pipeline execute: validator: call validate in dist/path/validator.js: field X is required")
	status, severity, code, diag := classifyPipelineError(err)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", status)
	}
	if severity != "error" {
		t.Fatalf("expected severity 'error', got %q", severity)
	}
	if code != "processing" {
		t.Fatalf("expected code 'processing', got %q", code)
	}
	if diag != "field X is required" {
		t.Fatalf("expected stripped diagnostics, got %q", diag)
	}
}

func TestFHIRSource_ClassifyPipelineError_Transformer(t *testing.T) {
	err := fmt.Errorf("pipeline execute: transformer: call transform in dist/path/transformer.js: null reference")
	status, _, code, diag := classifyPipelineError(err)
	if status != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", status)
	}
	if code != "exception" {
		t.Fatalf("expected code 'exception', got %q", code)
	}
	if diag != "null reference" {
		t.Fatalf("expected stripped diagnostics, got %q", diag)
	}
}

func TestFHIRSource_ClassifyPipelineError_TypeScript(t *testing.T) {
	err := fmt.Errorf("pipeline execute: transformer: call transform in dist/path/transformer.ts: ts error")
	_, _, _, diag := classifyPipelineError(err)
	if diag != "ts error" {
		t.Fatalf("expected 'ts error', got %q", diag)
	}
}

func TestFHIRSource_ClassifyPipelineError_PlainError(t *testing.T) {
	err := fmt.Errorf("something went wrong")
	status, _, code, diag := classifyPipelineError(err)
	if status != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", status)
	}
	if code != "exception" {
		t.Fatalf("expected code 'exception', got %q", code)
	}
	if diag != "something went wrong" {
		t.Fatalf("expected full error as diagnostics, got %q", diag)
	}
}

func TestFHIRSource_WriteOperationOutcome(t *testing.T) {
	rec := httptest.NewRecorder()
	writeOperationOutcome(rec, "warning", "processing", "test diagnostic")

	var oo map[string]any
	json.Unmarshal(rec.Body.Bytes(), &oo)
	if oo["resourceType"] != "OperationOutcome" {
		t.Fatalf("expected OperationOutcome, got %v", oo["resourceType"])
	}
	issues := oo["issue"].([]any)
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue, got %d", len(issues))
	}
	issue := issues[0].(map[string]any)
	if issue["severity"] != "warning" {
		t.Fatalf("expected severity 'warning', got %v", issue["severity"])
	}
	if issue["diagnostics"] != "test diagnostic" {
		t.Fatalf("expected diagnostics 'test diagnostic', got %v", issue["diagnostics"])
	}
}

func TestFHIRSource_CapabilityStatementDefaultResources(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0}
	src := NewFHIRSource(cfg, testLogger())

	cap := src.capabilityStatement("R4")
	if cap["resourceType"] != "CapabilityStatement" {
		t.Fatal("expected CapabilityStatement")
	}
	if cap["fhirVersion"] != "R4" {
		t.Fatalf("expected R4, got %v", cap["fhirVersion"])
	}

	rest := cap["rest"].([]map[string]any)
	resources := rest[0]["resource"].([]map[string]any)
	if len(resources) != 3 {
		t.Fatalf("expected 3 default resources, got %d", len(resources))
	}
}

func TestFHIRSource_CapabilityStatementCustomResources(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, Resources: []string{"Encounter"}}
	src := NewFHIRSource(cfg, testLogger())
	src.allowedResources = map[string]bool{"encounter": true}

	cap := src.capabilityStatement("R4")
	rest := cap["rest"].([]map[string]any)
	resources := rest[0]["resource"].([]map[string]any)
	if len(resources) != 1 {
		t.Fatalf("expected 1 resource, got %d", len(resources))
	}
	if resources[0]["type"] != "Encounter" {
		t.Fatalf("expected Encounter, got %v", resources[0]["type"])
	}
}

func TestFHIRSource_ReadOperationNotSupported(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir", Version: "R4"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Get("http://" + addr + "/fhir/Patient/123")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var oo map[string]any
	json.Unmarshal(body, &oo)
	if oo["resourceType"] != "OperationOutcome" {
		t.Fatal("expected OperationOutcome for read operation")
	}
}

func TestFHIRSource_SubscriptionNotification(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir", Version: "R4"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Post("http://"+addr+"/fhir/subscription-notification",
		"application/fhir+json", strings.NewReader(`{"resourceType":"Bundle","type":"subscription-notification"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if capture.count() != 1 {
		t.Fatalf("expected 1, got %d", capture.count())
	}
}

func TestFHIRSource_SubscriptionNotificationGETFails(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir", Version: "R4"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Get("http://" + addr + "/fhir/subscription-notification")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestFHIRSource_PUTUpdate(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir", Version: "R4"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	req, _ := http.NewRequest("PUT", "http://"+addr+"/fhir/Patient/456",
		strings.NewReader(`{"resourceType":"Patient","id":"456"}`))
	req.Header.Set("Content-Type", "application/fhir+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for PUT, got %d", resp.StatusCode)
	}
	if capture.count() != 1 {
		t.Fatalf("expected 1 message, got %d", capture.count())
	}
	msg := capture.get(0)
	if msg.Metadata["resource_type"] != "Patient" {
		t.Fatalf("expected resource_type Patient, got %v", msg.Metadata["resource_type"])
	}
	if msg.Metadata["resource_id"] != "456" {
		t.Fatalf("expected resource_id 456, got %v", msg.Metadata["resource_id"])
	}
	if msg.Metadata["http_method"] != "PUT" {
		t.Fatalf("expected PUT, got %v", msg.Metadata["http_method"])
	}
}

func TestFHIRSource_HandlerErrorClassified(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir", Version: "R4"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	handler := func(_ context.Context, _ *message.Message) error {
		return fmt.Errorf("pipeline execute: validator: call validate in dist/v.js: Missing required field")
	}
	if err := src.Start(ctx, handler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	resp, err := http.Post("http://"+addr+"/fhir/Patient", "application/fhir+json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", resp.StatusCode)
	}
	var oo map[string]any
	json.Unmarshal(body, &oo)
	issues := oo["issue"].([]any)
	issue := issues[0].(map[string]any)
	if issue["code"] != "processing" {
		t.Fatalf("expected code 'processing', got %v", issue["code"])
	}
}

func TestFHIRSource_AddrBeforeStart(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0}
	src := NewFHIRSource(cfg, testLogger())
	if src.Addr() != "" {
		t.Fatal("expected empty addr before start")
	}
}

// ===================================================================
// DICOMSource additional coverage
// ===================================================================

func TestDICOMSource_ExtractCallingAETitle_Short(t *testing.T) {
	src := &DICOMSource{cfg: &config.DICOMListener{}, logger: testLogger()}
	result := src.extractCallingAETitle(make([]byte, 10))
	if result != "" {
		t.Fatalf("expected empty for short data, got %q", result)
	}
}

func TestDICOMSource_ExtractCallingAETitle_Normal(t *testing.T) {
	src := &DICOMSource{cfg: &config.DICOMListener{}, logger: testLogger()}
	data := make([]byte, 68)
	copy(data[20:36], []byte(fmt.Sprintf("%-16s", "MYSCU")))
	result := src.extractCallingAETitle(data)
	if result != "MYSCU" {
		t.Fatalf("expected 'MYSCU', got %q", result)
	}
}

func TestDICOMSource_ValidateCallingAETitle_Empty(t *testing.T) {
	src := &DICOMSource{cfg: &config.DICOMListener{CallingAETitles: nil}, logger: testLogger()}
	if !src.validateCallingAETitle("ANY") {
		t.Fatal("empty allow list should accept any AE title")
	}
}

func TestDICOMSource_ValidateCallingAETitle_CaseInsensitive(t *testing.T) {
	src := &DICOMSource{cfg: &config.DICOMListener{CallingAETitles: []string{"ALLOWED"}}, logger: testLogger()}
	if !src.validateCallingAETitle("allowed") {
		t.Fatal("should match case-insensitively")
	}
	if !src.validateCallingAETitle("ALLOWED") {
		t.Fatal("should match exact case")
	}
	if src.validateCallingAETitle("DENIED") {
		t.Fatal("should reject non-matching AE title")
	}
}

func TestDICOMSource_AddrBeforeStart(t *testing.T) {
	src := &DICOMSource{cfg: &config.DICOMListener{Port: 0}, logger: testLogger()}
	if src.Addr() != "" {
		t.Fatal("expected empty addr before start")
	}
}

func TestDICOMSource_StopBeforeStart(t *testing.T) {
	src := NewDICOMSource(&config.DICOMListener{Port: 0}, testLogger())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop before Start should not error: %v", err)
	}
}

// ===================================================================
// OAuth2 helper additional coverage
// ===================================================================

func TestOAuth2_ClearCache(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "cached-token",
			"expires_in":   3600,
		})
	}))
	defer server.Close()

	ClearOAuth2Cache()

	token1, _ := fetchOAuth2Token(server.URL, "c1", "s1", nil)
	if token1 != "cached-token" {
		t.Fatalf("expected 'cached-token', got %q", token1)
	}

	ClearOAuth2Cache()

	oauth2CacheMu.RLock()
	count := len(oauth2Cache)
	oauth2CacheMu.RUnlock()
	if count != 0 {
		t.Fatalf("expected empty cache after clear, got %d entries", count)
	}
}

func TestOAuth2_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid_client"}`))
	}))
	defer server.Close()

	ClearOAuth2Cache()

	_, err := fetchOAuth2Token(server.URL, "bad", "creds", nil)
	if err == nil {
		t.Fatal("expected error for server error response")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Fatalf("expected status code in error, got %v", err)
	}
}

func TestOAuth2_EmptyAccessToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "",
			"expires_in":   3600,
		})
	}))
	defer server.Close()

	ClearOAuth2Cache()

	_, err := fetchOAuth2Token(server.URL, "c", "s", nil)
	if err == nil {
		t.Fatal("expected error for empty access_token")
	}
	if !strings.Contains(err.Error(), "empty access_token") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOAuth2_MissingClientSecret(t *testing.T) {
	_, err := fetchOAuth2Token("http://localhost", "client", "", nil)
	if err == nil {
		t.Fatal("expected error for empty client_secret")
	}
}

func TestOAuth2_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	ClearOAuth2Cache()

	_, err := fetchOAuth2Token(server.URL, "c", "s", nil)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestOAuth2_ScopesIncluded(t *testing.T) {
	var scopeReceived string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		scopeReceived = r.Form.Get("scope")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "scoped-token",
			"expires_in":   3600,
		})
	}))
	defer server.Close()

	ClearOAuth2Cache()

	fetchOAuth2Token(server.URL, "c", "s", []string{"read", "write"})
	if scopeReceived != "read write" {
		t.Fatalf("expected 'read write', got %q", scopeReceived)
	}
}

func TestOAuth2_ExpiresInDefaultsTo3600(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "default-exp",
			"expires_in":   0,
		})
	}))
	defer server.Close()

	ClearOAuth2Cache()

	token, err := fetchOAuth2Token(server.URL, "c", "s", nil)
	if err != nil {
		t.Fatalf("fetchOAuth2Token: %v", err)
	}
	if token != "default-exp" {
		t.Fatalf("expected 'default-exp', got %q", token)
	}
}

// ===================================================================
// pathRouter additional coverage
// ===================================================================

func TestPathRouter_TrailingSlashIsDistinctPath(t *testing.T) {
	router := newPathRouter()

	router.Register("/api", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	router.Register("/api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
	}))

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest("GET", "/api", nil))
	if rec.Code != 200 {
		t.Fatalf("expected 200 for /api, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest("GET", "/api/", nil))
	if rec.Code != 201 {
		t.Fatalf("expected 201 for /api/, got %d", rec.Code)
	}
}

func TestPathRouter_DeregisterNonexistentPath(t *testing.T) {
	router := newPathRouter()
	router.Deregister("/nonexistent")
	if router.Len() != 0 {
		t.Fatalf("expected 0, got %d", router.Len())
	}
}

func TestPathRouter_RegisterAfterDeregister(t *testing.T) {
	router := newPathRouter()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	router.Register("/reuse", handler)
	router.Deregister("/reuse")

	if err := router.Register("/reuse", handler); err != nil {
		t.Fatalf("expected no error after deregister, got %v", err)
	}
	if router.Len() != 1 {
		t.Fatalf("expected 1, got %d", router.Len())
	}
}

// ===================================================================
// SharedHTTPListener TLS coverage
// ===================================================================

func TestSharedHTTPListener_AcquireSamePortReturnsSameInstance(t *testing.T) {
	ResetSharedHTTPListeners()
	defer ResetSharedHTTPListeners()

	// Use port 0 consistently so both acquisitions use the same map key
	sl1, err := acquireSharedHTTPListener(0, nil, testLogger())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	sl2, err := acquireSharedHTTPListener(0, nil, testLogger())
	if err != nil {
		t.Fatalf("acquire same key: %v", err)
	}

	if sl1 != sl2 {
		t.Fatal("expected same instance for same port key")
	}

	sl1.mu.Lock()
	if sl1.refs != 2 {
		t.Fatalf("expected refs=2, got %d", sl1.refs)
	}
	sl1.mu.Unlock()

	releaseSharedHTTPListener(0, context.Background())
	releaseSharedHTTPListener(0, context.Background())
}

func TestSharedHTTPListener_ReleaseClosesServer(t *testing.T) {
	ResetSharedHTTPListeners()
	defer ResetSharedHTTPListeners()

	sl, err := acquireSharedHTTPListener(0, nil, testLogger())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	port := sl.listener.Addr().(*net.TCPAddr).Port

	releaseSharedHTTPListener(port, context.Background())

	sharedMu.Lock()
	_, exists := sharedListeners[port]
	sharedMu.Unlock()

	if exists {
		t.Fatal("expected shared listener to be removed after last release")
	}
}

// ===================================================================
// HTTPDest method variations
// ===================================================================

func TestHTTPDest_PATCHMethod(t *testing.T) {
	var method string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{URL: server.URL, Method: "PATCH"}
	dest := NewHTTPDest("test", cfg, testLogger())

	msg := message.New("ch1", []byte("data"))
	dest.Send(context.Background(), msg)
	if method != "PATCH" {
		t.Fatalf("expected PATCH, got %s", method)
	}
}

func TestHTTPDest_DELETEMethod(t *testing.T) {
	var method string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.WriteHeader(204)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{URL: server.URL, Method: "DELETE"}
	dest := NewHTTPDest("test", cfg, testLogger())

	msg := message.New("ch1", []byte("data"))
	resp, _ := dest.Send(context.Background(), msg)
	if method != "DELETE" {
		t.Fatalf("expected DELETE, got %s", method)
	}
	if resp.StatusCode != 204 {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}
}

// ===================================================================
// TCPSource — raw mode with multiple messages
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
	if string(capture.get(2).Raw) != "line3" {
		t.Fatalf("expected 'line3', got %q", string(capture.get(2).Raw))
	}
}

func TestTCPSource_AddrBeforeStart(t *testing.T) {
	src := NewTCPSource(&config.TCPListener{Port: 0}, testLogger())
	if src.Addr() != "" {
		t.Fatal("expected empty addr before start")
	}
}

func TestTCPSource_StopBeforeStart(t *testing.T) {
	src := NewTCPSource(&config.TCPListener{Port: 0}, testLogger())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
}

func TestTCPSource_TCPMeta(t *testing.T) {
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
	conn.Write([]byte("check-meta\n"))
	conn.Close()

	time.Sleep(100 * time.Millisecond)

	if capture.count() != 1 {
		t.Fatalf("expected 1, got %d", capture.count())
	}
	msg := capture.get(0)
	if msg.Transport != "tcp" {
		t.Fatalf("expected transport 'tcp', got %q", msg.Transport)
	}
	if msg.TCP == nil {
		t.Fatal("expected TCP meta to be set")
	}
	if msg.TCP.RemoteAddr == "" {
		t.Fatal("expected non-empty remote addr")
	}
}

// ===================================================================
// TCPSource MLLP — multiple messages on one connection
// ===================================================================

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
// Concurrent access stress tests
// ===================================================================

func TestSharedHTTPListener_ConcurrentAcquireRelease(t *testing.T) {
	ResetSharedHTTPListeners()
	defer ResetSharedHTTPListeners()

	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sl, err := acquireSharedHTTPListener(port, nil, testLogger())
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			time.Sleep(10 * time.Millisecond)
			_ = sl
			releaseSharedHTTPListener(port, context.Background())
		}()
	}
	wg.Wait()
}

func TestHTTPDest_ConcurrentSend(t *testing.T) {
	var mu sync.Mutex
	count := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{URL: server.URL}
	dest := NewHTTPDest("test", cfg, testLogger())

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msg := message.New("ch1", []byte(fmt.Sprintf("msg-%d", idx)))
			dest.Send(context.Background(), msg)
		}(i)
	}
	wg.Wait()

	mu.Lock()
	if count != 20 {
		t.Fatalf("expected 20 requests, got %d", count)
	}
	mu.Unlock()
}

// ===================================================================
// SOAPSource metadata on message
// ===================================================================

func TestSOAPSource_MessageMetadata(t *testing.T) {
	cfg := &config.SOAPListener{Port: 0, ServiceName: "MetaService"}
	src := NewSOAPSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	soapBody := `<?xml version="1.0"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><test/></soap:Body></soap:Envelope>`
	req, _ := http.NewRequest("POST", "http://"+addr+"/", strings.NewReader(soapBody))
	req.Header.Set("Content-Type", "application/soap+xml")
	req.Header.Set("SOAPAction", `"urn:process"`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if capture.count() != 1 {
		t.Fatalf("expected 1, got %d", capture.count())
	}
	msg := capture.get(0)
	if msg.Metadata["source"] != "soap" {
		t.Fatalf("expected source 'soap', got %v", msg.Metadata["source"])
	}
	if msg.Metadata["service_name"] != "MetaService" {
		t.Fatalf("expected service_name 'MetaService', got %v", msg.Metadata["service_name"])
	}
	if msg.Metadata["soap_action"] != "urn:process" {
		t.Fatalf("expected soap_action 'urn:process', got %v", msg.Metadata["soap_action"])
	}
	if msg.Transport != "soap" {
		t.Fatalf("expected transport 'soap', got %q", msg.Transport)
	}
	if msg.ContentType != "xml" {
		t.Fatalf("expected contentType 'xml', got %q", msg.ContentType)
	}
}

// ===================================================================
// FHIRSource - default basePath
// ===================================================================

func TestFHIRSource_DefaultBasePath(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, Version: "R4"}
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
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for /fhir/metadata, got %d", resp.StatusCode)
	}
}

// ===================================================================
// IHESource — message metadata verification
// ===================================================================

func TestIHESource_MessageTransportAndContentType(t *testing.T) {
	cfg := &config.IHEListener{Profile: "pix", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	http.Post("http://"+addr+"/pix/query", "text/xml", strings.NewReader("<q/>"))

	time.Sleep(50 * time.Millisecond)

	if capture.count() != 1 {
		t.Fatalf("expected 1, got %d", capture.count())
	}
	msg := capture.get(0)
	if msg.Transport != "ihe" {
		t.Fatalf("expected transport 'ihe', got %q", msg.Transport)
	}
	if msg.ContentType != "xml" {
		t.Fatalf("expected contentType 'xml', got %q", msg.ContentType)
	}
	if msg.Metadata["request_path"] != "/pix/query" {
		t.Fatalf("expected request_path '/pix/query', got %v", msg.Metadata["request_path"])
	}
}
