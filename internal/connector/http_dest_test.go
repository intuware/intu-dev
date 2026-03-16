package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// HTTPDest — path param URL encoding and special characters
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

func TestHTTPDest_PathParams_FromMessage(t *testing.T) {
	var receivedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.HTTPDestConfig{
		URL:        server.URL + "/api/{kind}",
		PathParams: map[string]string{"kind": "default"},
	}
	dest := NewHTTPDest("test-path-msg", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	msg.HTTP = &message.HTTPMeta{
		PathParams: map[string]string{"kind": "overridden"},
	}
	dest.Send(context.Background(), msg)

	if !strings.Contains(receivedPath, "overridden") {
		t.Fatalf("expected message path param to override config, got %q", receivedPath)
	}
}

func TestHTTPDest_CustomTimeout(t *testing.T) {
	cfg := &config.HTTPDestConfig{URL: "http://localhost", TimeoutMs: 500}
	dest := NewHTTPDest("test-timeout", cfg, testLogger())

	if dest.client.Timeout.Milliseconds() != 500 {
		t.Fatalf("expected 500ms timeout, got %v", dest.client.Timeout)
	}
}

func TestHTTPDest_DefaultTimeout(t *testing.T) {
	cfg := &config.HTTPDestConfig{URL: "http://localhost"}
	dest := NewHTTPDest("test-timeout", cfg, testLogger())

	if dest.client.Timeout.Seconds() != 30 {
		t.Fatalf("expected 30s default timeout, got %v", dest.client.Timeout)
	}
}

// ===================================================================
// FHIRDest — resource type from metadata precedence, update no ID
// ===================================================================

func TestFHIRDest_UpdateResource_IDFromBody(t *testing.T) {
	var receivedPath, receivedMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		receivedMethod = r.Method
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.FHIRDestMapConfig{BaseURL: server.URL + "/fhir"}
	dest := NewFHIRDest("test-fhir-upd", cfg, testLogger())

	msg := message.New("ch1", []byte(`{"resourceType":"Patient","id":"789"}`))
	msg.Metadata["fhir_operation"] = "update"
	resp, _ := dest.Send(context.Background(), msg)

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if receivedMethod != "PUT" {
		t.Fatalf("expected PUT, got %s", receivedMethod)
	}
	if receivedPath != "/fhir/Patient/789" {
		t.Fatalf("expected /fhir/Patient/789, got %s", receivedPath)
	}
}

func TestFHIRDest_DefaultCreateOperation(t *testing.T) {
	var receivedMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		w.WriteHeader(201)
	}))
	defer server.Close()

	cfg := &config.FHIRDestMapConfig{BaseURL: server.URL + "/fhir"}
	dest := NewFHIRDest("test-fhir-def", cfg, testLogger())

	msg := message.New("ch1", []byte(`{"resourceType":"Observation"}`))
	dest.Send(context.Background(), msg)

	if receivedMethod != "POST" {
		t.Fatalf("expected POST for default create, got %s", receivedMethod)
	}
}

func TestFHIRDest_Stop(t *testing.T) {
	cfg := &config.FHIRDestMapConfig{BaseURL: "http://localhost/fhir"}
	dest := NewFHIRDest("test-fhir-stop", cfg, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestFHIRDest_CustomTimeout(t *testing.T) {
	cfg := &config.FHIRDestMapConfig{BaseURL: "http://localhost", TimeoutMs: 750}
	dest := NewFHIRDest("test-fhir-to", cfg, testLogger())

	if dest.client.Timeout.Milliseconds() != 750 {
		t.Fatalf("expected 750ms timeout, got %v", dest.client.Timeout)
	}
}

// ===================================================================
// FHIRSource — PUT update and subscription notification
// ===================================================================

func TestFHIRSource_UpdateResource(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()
	req, _ := http.NewRequest("PUT", "http://"+addr+"/fhir/Patient/123", strings.NewReader(`{"resourceType":"Patient","id":"123"}`))
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
	if capture.get(0).Metadata["http_method"] != "PUT" {
		t.Fatalf("expected http_method=PUT, got %v", capture.get(0).Metadata["http_method"])
	}
}

func TestFHIRSource_SubscriptionNotification_Boost(t *testing.T) {
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
	if capture.get(0).Metadata["subscription_type"] != "rest-hook" {
		t.Fatalf("expected subscription_type=rest-hook, got %v", capture.get(0).Metadata["subscription_type"])
	}
}

func TestFHIRSource_ReadOperationRejected(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()
	resp, err := http.Get("http://" + addr + "/fhir/Patient/123")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 (with error body), got %d", resp.StatusCode)
	}

	var outcome map[string]any
	json.Unmarshal(body, &outcome)
	if outcome["resourceType"] != "OperationOutcome" {
		t.Fatalf("expected OperationOutcome, got %v", outcome["resourceType"])
	}
}

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

func TestFHIRSource_PipelineError_Transformer(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	errHandler := func(_ context.Context, _ *message.Message) error {
		return fmt.Errorf("pipeline execute: transformer: call transform in dist/ch/transformer.js: Mapping failed")
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

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 for transformer error, got %d", resp.StatusCode)
	}
}

// ===================================================================
// SOAPSource — handler error returns SOAP fault
// ===================================================================

func TestSOAPSource_MethodNotAllowedOnWSdl(t *testing.T) {
	cfg := &config.SOAPListener{Port: 0, WSDLPath: "/wsdl"}
	src := NewSOAPSource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.listener.Addr().String()
	req, _ := http.NewRequest("POST", "http://"+addr+"/wsdl", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for POST on WSDL, got %d", resp.StatusCode)
	}
}

func TestSOAPSource_DefaultWSDLPath(t *testing.T) {
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

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "IntuService") {
		t.Fatal("default service name should be IntuService")
	}
}

func TestSOAPSource_Type(t *testing.T) {
	src := NewSOAPSource(&config.SOAPListener{Port: 0}, testLogger())
	if src.Type() != "soap" {
		t.Fatalf("expected type 'soap', got %q", src.Type())
	}
}

func TestSOAPSource_AddrBeforeStart(t *testing.T) {
	src := NewSOAPSource(&config.SOAPListener{Port: 0}, testLogger())
	if src.Addr() != "" {
		t.Fatalf("expected empty addr before start, got %q", src.Addr())
	}
}

func TestSOAPSource_StopBeforeStart(t *testing.T) {
	src := NewSOAPSource(&config.SOAPListener{Port: 0}, testLogger())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop before start should not error: %v", err)
	}
}

// ===================================================================
// IHESource — additional profiles and endpoints
// ===================================================================

func TestIHESource_XDSRegistry_Boost(t *testing.T) {
	cfg := &config.IHEListener{Profile: "xds_registry", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()

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

func TestIHESource_GenericProfile_Boost(t *testing.T) {
	cfg := &config.IHEListener{Profile: "custom_profile", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()

	resp, err := http.Post("http://"+addr+"/", "text/xml", strings.NewReader("<msg/>"))
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
	if capture.get(0).Metadata["ihe_profile"] != "custom_profile" {
		t.Fatalf("expected custom_profile, got %v", capture.get(0).Metadata["ihe_profile"])
	}
}

func TestIHESource_GenericGETReturnsProfileStatus(t *testing.T) {
	cfg := &config.IHEListener{Profile: "custom_profile", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	if err := src.Start(ctx, noopHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()
	resp, err := http.Get("http://" + addr + "/")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var status map[string]string
	json.Unmarshal(body, &status)
	if status["profile"] != "custom_profile" {
		t.Fatalf("expected custom_profile, got %q", status["profile"])
	}
}

func TestIHESource_Type_WithProfile(t *testing.T) {
	src := NewIHESource(&config.IHEListener{Profile: "PDQ", Port: 0}, testLogger())
	if src.Type() != "ihe/pdq" {
		t.Fatalf("expected type 'ihe/pdq', got %q", src.Type())
	}
}

func TestIHESource_AddrBeforeStart(t *testing.T) {
	src := NewIHESource(&config.IHEListener{Profile: "pix", Port: 0}, testLogger())
	if src.Addr() != "" {
		t.Fatalf("expected empty addr before start, got %q", src.Addr())
	}
}

func TestIHESource_StopBeforeStart(t *testing.T) {
	src := NewIHESource(&config.IHEListener{Profile: "pix", Port: 0}, testLogger())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop before start should not error: %v", err)
	}
}

func TestIHESource_HandlerErrorReturnsXML(t *testing.T) {
	cfg := &config.IHEListener{Profile: "pix", Port: 0}
	src := NewIHESource(cfg, testLogger())

	ctx := context.Background()
	errHandler := func(_ context.Context, _ *message.Message) error {
		return fmt.Errorf("processing failed")
	}
	if err := src.Start(ctx, errHandler); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Post("http://"+src.Addr()+"/pix/query", "text/xml", strings.NewReader("<q/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "processing failed") {
		t.Fatalf("expected error message in body, got %q", string(body))
	}
}

// ===================================================================
// JMSDest — content type mapping
// ===================================================================

func TestJMSDest_ContentTypeXML(t *testing.T) {
	var contentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.JMSDestMapConfig{URL: server.URL, Queue: "q"}
	dest := NewJMSDest("test-jms-xml", cfg, testLogger())

	msg := message.New("ch1", []byte("<xml/>"))
	msg.ContentType = message.ContentTypeXML
	dest.Send(context.Background(), msg)

	if contentType != "text/xml" {
		t.Fatalf("expected text/xml, got %q", contentType)
	}
}

func TestJMSDest_ContentTypeHL7v2(t *testing.T) {
	var contentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.JMSDestMapConfig{URL: server.URL, Queue: "q"}
	dest := NewJMSDest("test-jms-hl7", cfg, testLogger())

	msg := message.New("ch1", []byte("MSH|^~\\&|..."))
	msg.ContentType = message.ContentTypeHL7v2
	dest.Send(context.Background(), msg)

	if contentType != "x-application/hl7-v2+er7" {
		t.Fatalf("expected x-application/hl7-v2+er7, got %q", contentType)
	}
}

func TestJMSDest_ContentTypeDefault(t *testing.T) {
	var contentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.JMSDestMapConfig{URL: server.URL, Queue: "q"}
	dest := NewJMSDest("test-jms-def", cfg, testLogger())

	msg := message.New("ch1", []byte("binary data"))
	dest.Send(context.Background(), msg)

	if contentType != "application/octet-stream" {
		t.Fatalf("expected application/octet-stream, got %q", contentType)
	}
}

func TestJMSDest_TransportStamping_Boost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.JMSDestMapConfig{URL: server.URL, Queue: "q"}
	dest := NewJMSDest("test-jms-stamp", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	msg.Transport = "http"
	msg.HTTP = &message.HTTPMeta{Headers: map[string]string{"old": "header"}}
	dest.Send(context.Background(), msg)

	if msg.Transport != "jms" {
		t.Fatalf("expected transport=jms, got %q", msg.Transport)
	}
	if msg.HTTP == nil {
		t.Fatal("expected HTTP meta to be set for JMS transport")
	}
	if msg.HTTP.Method != "POST" {
		t.Fatalf("expected method=POST, got %q", msg.HTTP.Method)
	}
}

func TestJMSDest_APIKeyAuth(t *testing.T) {
	var keyHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keyHeader = r.Header.Get("X-JMS-Key")
		w.WriteHeader(200)
	}))
	defer server.Close()

	cfg := &config.JMSDestMapConfig{
		URL:   server.URL,
		Queue: "q",
		Auth:  &config.HTTPAuthConfig{Type: "api_key", Key: "jms-key-val", Header: "X-JMS-Key"},
	}
	dest := NewJMSDest("test-jms-apikey", cfg, testLogger())

	msg := message.New("ch1", []byte("test"))
	dest.Send(context.Background(), msg)

	if keyHeader != "jms-key-val" {
		t.Fatalf("expected API key header, got %q", keyHeader)
	}
}

// ===================================================================
// SMTPDest — constructor and type
// ===================================================================

func TestSMTPDest_Constructor_Boost(t *testing.T) {
	cfg := &config.SMTPDestMapConfig{Host: "smtp.example.com", From: "a@b.com", To: []string{"c@d.com"}}
	dest := NewSMTPDest("test-smtp", cfg, testLogger())
	if dest == nil {
		t.Fatal("expected non-nil SMTPDest")
	}
	if dest.name != "test-smtp" {
		t.Fatalf("expected name 'test-smtp', got %q", dest.name)
	}
}

func TestSMTPDest_Stop(t *testing.T) {
	cfg := &config.SMTPDestMapConfig{Host: "localhost"}
	dest := NewSMTPDest("test-smtp", cfg, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ===================================================================
// DirectDest — constructor, type, stop
// ===================================================================

func TestDirectDest_Constructor_Boost(t *testing.T) {
	cfg := &config.DirectDestMapConfig{To: "a@direct.com", From: "b@direct.com"}
	dest := NewDirectDest("test-direct", cfg, testLogger())
	if dest == nil {
		t.Fatal("expected non-nil DirectDest")
	}
	if dest.name != "test-direct" {
		t.Fatalf("expected name 'test-direct', got %q", dest.name)
	}
}

func TestDirectDest_Stop(t *testing.T) {
	cfg := &config.DirectDestMapConfig{To: "a@b.com", From: "c@d.com"}
	dest := NewDirectDest("test-direct", cfg, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestDirectDest_MissingBothAddresses(t *testing.T) {
	dest := NewDirectDest("test", &config.DirectDestMapConfig{}, testLogger())
	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestDirectDest_SMTPHostFromToAddress(t *testing.T) {
	dest := NewDirectDest("test", &config.DirectDestMapConfig{
		To:   "user@direct.hospital.org",
		From: "sender@lab.org",
	}, testLogger())
	msg := message.New("ch1", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.Error == nil {
		t.Fatal("expected connection error (no real SMTP server), but got nil error")
	}
}

// ===================================================================
// classifyPipelineError
// ===================================================================

func TestClassifyPipelineError_ValidatorError(t *testing.T) {
	err := fmt.Errorf("pipeline execute: validator: call validate in dist/ch/validator.js: Missing required field")
	status, severity, code, diag := classifyPipelineError(err)
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", status)
	}
	if severity != "error" {
		t.Fatalf("expected severity=error, got %q", severity)
	}
	if code != "processing" {
		t.Fatalf("expected code=processing, got %q", code)
	}
	if diag != "Missing required field" {
		t.Fatalf("expected 'Missing required field', got %q", diag)
	}
}

func TestClassifyPipelineError_TransformerError(t *testing.T) {
	err := fmt.Errorf("pipeline execute: transformer: call transform in dist/ch/transformer.js: Mapping error")
	status, _, code, diag := classifyPipelineError(err)
	if status != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", status)
	}
	if code != "exception" {
		t.Fatalf("expected code=exception, got %q", code)
	}
	if diag != "Mapping error" {
		t.Fatalf("expected 'Mapping error', got %q", diag)
	}
}

func TestClassifyPipelineError_GenericError(t *testing.T) {
	err := fmt.Errorf("some other error")
	status, _, code, diag := classifyPipelineError(err)
	if status != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", status)
	}
	if code != "exception" {
		t.Fatalf("expected code=exception, got %q", code)
	}
	if diag != "some other error" {
		t.Fatalf("expected 'some other error', got %q", diag)
	}
}

func TestClassifyPipelineError_TSFileExtension(t *testing.T) {
	err := fmt.Errorf("pipeline execute: validator: call validate in channels/ch/validator.ts: Bad data")
	_, _, _, diag := classifyPipelineError(err)
	if diag != "Bad data" {
		t.Fatalf("expected 'Bad data', got %q", diag)
	}
}

// ===================================================================
// writeOperationOutcome
// ===================================================================

func TestWriteOperationOutcome(t *testing.T) {
	w := httptest.NewRecorder()
	writeOperationOutcome(w, "error", "not-supported", "Test diagnostic")

	var result map[string]any
	json.Unmarshal(w.Body.Bytes(), &result)

	if result["resourceType"] != "OperationOutcome" {
		t.Fatalf("expected OperationOutcome, got %v", result["resourceType"])
	}

	issues := result["issue"].([]any)
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue, got %d", len(issues))
	}

	issue := issues[0].(map[string]any)
	if issue["severity"] != "error" {
		t.Fatalf("expected severity=error, got %v", issue["severity"])
	}
	if issue["code"] != "not-supported" {
		t.Fatalf("expected code=not-supported, got %v", issue["code"])
	}
}

// ===================================================================
// soapFaultResponse / soapSuccessResponse
// ===================================================================

func TestSoapFaultResponse(t *testing.T) {
	resp := soapFaultResponse("Client", "Bad request")
	if !strings.Contains(resp, "soap:Client") {
		t.Fatal("expected soap:Client in fault response")
	}
	if !strings.Contains(resp, "Bad request") {
		t.Fatal("expected 'Bad request' in fault string")
	}
}

func TestSoapSuccessResponse(t *testing.T) {
	resp := soapSuccessResponse()
	if !strings.Contains(resp, "<status>accepted</status>") {
		t.Fatal("expected accepted status in success response")
	}
	if !strings.Contains(resp, "ProcessResponse") {
		t.Fatal("expected ProcessResponse in success response")
	}
}

// ===================================================================
// FHIRSource — case-insensitive path
// ===================================================================

func TestFHIRSource_CaseInsensitivePath(t *testing.T) {
	cfg := &config.FHIRListener{Port: 0, BasePath: "/fhir"}
	src := NewFHIRSource(cfg, testLogger())

	ctx := context.Background()
	capture := &msgCapture{}
	if err := src.Start(ctx, capture.handler()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()
	resp, err := http.Post("http://"+addr+"/FHIR/Patient", "application/fhir+json", strings.NewReader(`{"resourceType":"Patient"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 for case-insensitive path, got %d", resp.StatusCode)
	}
}
