package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHTTPDest_SendBasic(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		w.Header().Set("X-Response", "ok")
		w.WriteHeader(200)
		fmt.Fprint(w, `{"status":"received"}`)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:    ts.URL,
		Method: "POST",
	}, testLogger())

	msg := message.New("", []byte(`{"test":"data"}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if !strings.Contains(string(resp.Body), "received") {
		t.Errorf("expected 'received' in body, got %q", string(resp.Body))
	}
}

func TestHTTPDest_SendWithHeaders(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Custom") != "test-value" {
			t.Errorf("expected X-Custom header, got %q", r.Header.Get("X-Custom"))
		}
		if r.Header.Get("Content-Type") != "application/hl7-v2" {
			t.Errorf("expected custom content type, got %q", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:     ts.URL,
		Method:  "PUT",
		Headers: map[string]string{"X-Custom": "test-value", "Content-Type": "application/hl7-v2"},
	}, testLogger())

	msg := message.New("", []byte("MSH|..."))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_SendWithQueryParams(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") != "json" {
			t.Errorf("expected format=json, got %q", r.URL.Query().Get("format"))
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:         ts.URL,
		Method:      "POST",
		QueryParams: map[string]string{"format": "json"},
	}, testLogger())

	msg := message.New("", []byte(`{}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_SendWithPathParams(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "patient-123") {
			t.Errorf("expected path to contain patient-123, got %q", r.URL.Path)
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:        ts.URL + "/patients/{id}",
		Method:     "GET",
		PathParams: map[string]string{"id": "patient-123"},
	}, testLogger())

	msg := message.New("", []byte(`{}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_SendWithAuthBearer(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer test-token" {
			t.Errorf("expected Bearer auth, got %q", auth)
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:    ts.URL,
		Method: "POST",
		Auth: &config.HTTPAuthConfig{
			Type:  "bearer",
			Token: "test-token",
		},
	}, testLogger())

	msg := message.New("", []byte(`{}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_SendWithAuthBasic(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "admin" || pass != "secret" {
			t.Errorf("expected basic auth admin:secret, got %s:%s (ok=%v)", user, pass, ok)
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:    ts.URL,
		Method: "POST",
		Auth: &config.HTTPAuthConfig{
			Type:     "basic",
			Username: "admin",
			Password: "secret",
		},
	}, testLogger())

	msg := message.New("", []byte(`{}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_SendWithAuthAPIKey_Header(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "my-api-key" {
			t.Errorf("expected API key header, got %q", r.Header.Get("X-Api-Key"))
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:    ts.URL,
		Method: "POST",
		Auth: &config.HTTPAuthConfig{
			Type:   "api_key",
			Header: "X-Api-Key",
			Key:    "my-api-key",
		},
	}, testLogger())

	msg := message.New("", []byte(`{}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_SendWithAuthAPIKey_QueryParam(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("api_key") != "my-api-key" {
			t.Errorf("expected api_key query param, got %q", r.URL.Query().Get("api_key"))
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:    ts.URL,
		Method: "POST",
		Auth: &config.HTTPAuthConfig{
			Type:       "api_key",
			QueryParam: "api_key",
			Key:        "my-api-key",
		},
	}, testLogger())

	msg := message.New("", []byte(`{}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_ErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		fmt.Fprint(w, `{"error":"internal server error"}`)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:    ts.URL,
		Method: "POST",
	}, testLogger())

	msg := message.New("", []byte(`{}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 500 {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_Timeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:       ts.URL,
		Method:    "POST",
		TimeoutMs: 50,
	}, testLogger())

	msg := message.New("", []byte(`{}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected timeout error in response")
	}
}

func TestHTTPDest_ConnectionRefused(t *testing.T) {
	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:       "http://127.0.0.1:1",
		Method:    "POST",
		TimeoutMs: 1000,
	}, testLogger())

	msg := message.New("", []byte(`{}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected connection error")
	}
}

func TestHTTPDest_Stop(t *testing.T) {
	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL: "http://localhost:8080",
	}, testLogger())

	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestHTTPDest_Type(t *testing.T) {
	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{}, testLogger())
	if dest.Type() != "http" {
		t.Errorf("expected 'http', got %q", dest.Type())
	}
}

func TestHTTPDest_DefaultMethod(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected default POST, got %s", r.Method)
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL: ts.URL,
	}, testLogger())

	msg := message.New("", []byte(`{}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPDest_MergesMessageHTTPMeta(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-From-Cfg") != "cfg-val" {
			t.Errorf("expected cfg header")
		}
		if r.Header.Get("X-From-Msg") != "msg-val" {
			t.Errorf("expected msg header")
		}
		w.WriteHeader(200)
	}))
	defer ts.Close()

	dest := NewHTTPDest("test-dest", &config.HTTPDestConfig{
		URL:     ts.URL,
		Headers: map[string]string{"X-From-Cfg": "cfg-val"},
	}, testLogger())

	msg := message.New("", []byte(`{}`))
	msg.HTTP = &message.HTTPMeta{
		Headers: map[string]string{"X-From-Msg": "msg-val"},
	}
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestFHIRSource_CapabilityStatement(t *testing.T) {
	logger := testLogger()
	src := NewFHIRSource(&config.FHIRListener{
		Port:      0,
		Resources: []string{"Patient", "Observation"},
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	addr := src.Addr()
	if addr == "" {
		t.Fatal("expected non-empty addr")
	}

	resp, err := http.Get("http://" + addr + "/fhir/metadata")
	if err != nil {
		t.Fatalf("GET /fhir/metadata: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "fhir+json") {
		t.Errorf("expected fhir+json content type, got %q", ct)
	}

	var cap map[string]any
	json.NewDecoder(resp.Body).Decode(&cap)
	if cap["resourceType"] != "CapabilityStatement" {
		t.Error("expected CapabilityStatement resourceType")
	}
}

func TestFHIRSource_CreateResource(t *testing.T) {
	logger := testLogger()
	var received *message.Message

	src := NewFHIRSource(&config.FHIRListener{
		Port:      0,
		Resources: []string{"Patient"},
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		received = msg
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	body := `{"resourceType":"Patient","name":[{"family":"Smith"}]}`
	resp, err := http.Post("http://"+src.Addr()+"/fhir/Patient", "application/fhir+json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		t.Errorf("expected 201, got %d", resp.StatusCode)
	}
	if received == nil {
		t.Fatal("expected message to be received")
	}
	if received.Metadata["resource_type"] != "Patient" {
		t.Errorf("expected resource_type 'Patient', got %v", received.Metadata["resource_type"])
	}
}

func TestFHIRSource_ResourceNotAllowed(t *testing.T) {
	logger := testLogger()
	src := NewFHIRSource(&config.FHIRListener{
		Port:      0,
		Resources: []string{"Patient"},
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Post("http://"+src.Addr()+"/fhir/Observation", "application/fhir+json",
		strings.NewReader(`{"resourceType":"Observation"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 404 {
		t.Errorf("expected 404 for unsupported resource, got %d", resp.StatusCode)
	}
}

func TestFHIRSource_ReadNotSupported(t *testing.T) {
	logger := testLogger()
	src := NewFHIRSource(&config.FHIRListener{
		Port: 0,
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Get("http://" + src.Addr() + "/fhir/Patient/123")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Logf("read returned status %d", resp.StatusCode)
	}

	var oo map[string]any
	json.NewDecoder(resp.Body).Decode(&oo)
	if oo["resourceType"] != "OperationOutcome" {
		t.Error("expected OperationOutcome for read")
	}
}

func TestFHIRSource_HandlerError(t *testing.T) {
	logger := testLogger()
	src := NewFHIRSource(&config.FHIRListener{
		Port: 0,
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return fmt.Errorf("pipeline execute: validator: call validate in dist/ch/validator.js: HL7 parse error")
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Post("http://"+src.Addr()+"/fhir/Patient", "application/fhir+json",
		strings.NewReader(`{"resourceType":"Patient"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 422 {
		t.Errorf("expected 422, got %d", resp.StatusCode)
	}
}

func TestFHIRSource_Type(t *testing.T) {
	src := NewFHIRSource(&config.FHIRListener{}, testLogger())
	if src.Type() != "fhir" {
		t.Errorf("expected 'fhir', got %q", src.Type())
	}
}

func TestSOAPSource_BasicSOAPRequest(t *testing.T) {
	logger := testLogger()
	var received *message.Message

	src := NewSOAPSource(&config.SOAPListener{
		Port:        0,
		ServiceName: "TestService",
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		received = msg
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	soapBody := `<?xml version="1.0"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><process/></soap:Body></soap:Envelope>`
	req, _ := http.NewRequest("POST", "http://"+src.Addr()+"/", strings.NewReader(soapBody))
	req.Header.Set("Content-Type", "text/xml")
	req.Header.Set("SOAPAction", "process")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if received == nil {
		t.Fatal("expected message")
	}
	if received.Metadata["soap_action"] != "process" {
		t.Errorf("expected soap_action 'process', got %v", received.Metadata["soap_action"])
	}
}

func TestSOAPSource_WSDLEndpoint(t *testing.T) {
	logger := testLogger()
	src := NewSOAPSource(&config.SOAPListener{
		Port:        0,
		ServiceName: "TestService",
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Get("http://" + src.Addr() + "/wsdl")
	if err != nil {
		t.Fatalf("GET /wsdl: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "TestService") {
		t.Error("expected service name in WSDL")
	}
	if !strings.Contains(string(body), "definitions") {
		t.Error("expected definitions element in WSDL")
	}
}

func TestSOAPSource_MethodNotAllowed(t *testing.T) {
	logger := testLogger()
	src := NewSOAPSource(&config.SOAPListener{Port: 0}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Get("http://" + src.Addr() + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 405 {
		t.Errorf("expected 405, got %d", resp.StatusCode)
	}
}

func TestSOAPSource_WrongContentType(t *testing.T) {
	logger := testLogger()
	src := NewSOAPSource(&config.SOAPListener{Port: 0}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Post("http://"+src.Addr()+"/", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 415 {
		t.Errorf("expected 415, got %d", resp.StatusCode)
	}
}

func TestSOAPSource_HandlerError(t *testing.T) {
	logger := testLogger()
	src := NewSOAPSource(&config.SOAPListener{Port: 0}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return fmt.Errorf("test error")
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	req, _ := http.NewRequest("POST", "http://"+src.Addr()+"/", strings.NewReader("<xml/>"))
	req.Header.Set("Content-Type", "text/xml")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 500 {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Fault") {
		t.Error("expected SOAP Fault in response")
	}
}

func TestSOAPSource_Type(t *testing.T) {
	src := NewSOAPSource(&config.SOAPListener{}, testLogger())
	if src.Type() != "soap" {
		t.Errorf("expected 'soap', got %q", src.Type())
	}
}

func TestIHESource_XDSRepository(t *testing.T) {
	logger := testLogger()
	var received *message.Message

	src := NewIHESource(&config.IHEListener{
		Port:    0,
		Profile: "xds_repository",
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		received = msg
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Post("http://"+src.Addr()+"/xds/repository/provide", "text/xml",
		strings.NewReader("<ProvideAndRegisterDocumentSet/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if received == nil {
		t.Fatal("expected message")
	}
	if received.Metadata["ihe_transaction"] != "ProvideAndRegisterDocumentSet" {
		t.Errorf("expected transaction, got %v", received.Metadata["ihe_transaction"])
	}
}

func TestIHESource_XDSRegistry(t *testing.T) {
	logger := testLogger()
	src := NewIHESource(&config.IHEListener{
		Port:    0,
		Profile: "xds_registry",
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	// Test register
	resp, err := http.Post("http://"+src.Addr()+"/xds/registry/register", "text/xml",
		strings.NewReader("<RegisterDocumentSet/>"))
	if err != nil {
		t.Fatalf("POST register: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("register: expected 200, got %d", resp.StatusCode)
	}

	// Test query
	resp2, err := http.Post("http://"+src.Addr()+"/xds/registry/query", "text/xml",
		strings.NewReader("<RegistryStoredQuery/>"))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Errorf("query: expected 200, got %d", resp2.StatusCode)
	}
}

func TestIHESource_PIX(t *testing.T) {
	logger := testLogger()
	src := NewIHESource(&config.IHEListener{
		Port:    0,
		Profile: "pix",
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Post("http://"+src.Addr()+"/pix/query", "text/xml",
		strings.NewReader("<PIXQuery/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	resp2, err := http.Post("http://"+src.Addr()+"/pix/feed", "text/xml",
		strings.NewReader("<PIXFeed/>"))
	if err != nil {
		t.Fatalf("POST feed: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp2.StatusCode)
	}
}

func TestIHESource_PDQ(t *testing.T) {
	logger := testLogger()
	src := NewIHESource(&config.IHEListener{
		Port:    0,
		Profile: "pdq",
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Post("http://"+src.Addr()+"/pdq/query", "text/xml",
		strings.NewReader("<PDQQuery/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestIHESource_GenericProfile(t *testing.T) {
	logger := testLogger()
	src := NewIHESource(&config.IHEListener{
		Port:    0,
		Profile: "custom_profile",
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Post("http://"+src.Addr()+"/", "text/xml",
		strings.NewReader("<GenericRequest/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestIHESource_StatusEndpoint(t *testing.T) {
	logger := testLogger()
	src := NewIHESource(&config.IHEListener{
		Port:    0,
		Profile: "xds_repository",
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Get("http://" + src.Addr() + "/ihe/status")
	if err != nil {
		t.Fatalf("GET /ihe/status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var status map[string]any
	json.NewDecoder(resp.Body).Decode(&status)
	if status["status"] != "running" {
		t.Error("expected status 'running'")
	}
}

func TestIHESource_Type(t *testing.T) {
	src := NewIHESource(&config.IHEListener{Profile: "pix"}, testLogger())
	if src.Type() != "ihe/pix" {
		t.Errorf("expected 'ihe/pix', got %q", src.Type())
	}
}

func TestIHESource_MethodNotAllowed(t *testing.T) {
	logger := testLogger()
	src := NewIHESource(&config.IHEListener{
		Port:    0,
		Profile: "pdq",
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Get("http://" + src.Addr() + "/pdq/query")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Errorf("expected 405, got %d", resp.StatusCode)
	}
}

func TestIHESource_HandlerError(t *testing.T) {
	logger := testLogger()
	src := NewIHESource(&config.IHEListener{
		Port:    0,
		Profile: "pix",
	}, logger)

	ctx := context.Background()
	err := src.Start(ctx, func(ctx context.Context, msg *message.Message) error {
		return fmt.Errorf("handler failed")
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer src.Stop(ctx)

	resp, err := http.Post("http://"+src.Addr()+"/pix/query", "text/xml",
		strings.NewReader("<PIXQuery/>"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 500 {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}
}

func TestClassifyPipelineError_Validator(t *testing.T) {
	err := fmt.Errorf("pipeline execute: validator: call validate in dist/ch/validator.js: invalid HL7 message")
	status, severity, code, diag := classifyPipelineError(err)
	if status != 422 {
		t.Errorf("expected 422, got %d", status)
	}
	if severity != "error" {
		t.Errorf("expected 'error', got %q", severity)
	}
	if code != "processing" {
		t.Errorf("expected 'processing', got %q", code)
	}
	if diag != "invalid HL7 message" {
		t.Errorf("expected cleaned diagnostics, got %q", diag)
	}
}

func TestClassifyPipelineError_Transformer(t *testing.T) {
	err := fmt.Errorf("pipeline execute: transformer: call transform in dist/ch/transformer.js: field not found")
	status, _, code, diag := classifyPipelineError(err)
	if status != 500 {
		t.Errorf("expected 500, got %d", status)
	}
	if code != "exception" {
		t.Errorf("expected 'exception', got %q", code)
	}
	if diag != "field not found" {
		t.Errorf("expected 'field not found', got %q", diag)
	}
}

func TestClassifyPipelineError_TSFile(t *testing.T) {
	err := fmt.Errorf("pipeline execute: validator: call validate in src/ch/validator.ts: type error")
	_, _, _, diag := classifyPipelineError(err)
	if diag != "type error" {
		t.Errorf("expected 'type error', got %q", diag)
	}
}

func TestMergeMaps(t *testing.T) {
	base := map[string]string{"a": "1", "b": "2"}
	override := map[string]string{"b": "3", "c": "4"}
	merged := mergeMaps(base, override)
	if merged["a"] != "1" {
		t.Error("expected a=1")
	}
	if merged["b"] != "3" {
		t.Error("expected b=3 (overridden)")
	}
	if merged["c"] != "4" {
		t.Error("expected c=4")
	}
}

func TestMergeMaps_NilInputs(t *testing.T) {
	merged := mergeMaps(nil, nil)
	if merged == nil {
		t.Error("expected non-nil map")
	}
	if len(merged) != 0 {
		t.Error("expected empty map")
	}
}

func TestWriteOperationOutcome(t *testing.T) {
	rec := httptest.NewRecorder()
	writeOperationOutcome(rec, "error", "not-found", "Resource not found")

	var oo map[string]any
	json.NewDecoder(rec.Body).Decode(&oo)
	if oo["resourceType"] != "OperationOutcome" {
		t.Error("expected OperationOutcome")
	}
}

func TestSoapFaultResponse(t *testing.T) {
	resp := soapFaultResponse("Server", "test error")
	if !strings.Contains(resp, "Fault") {
		t.Error("expected Fault in response")
	}
	if !strings.Contains(resp, "test error") {
		t.Error("expected error message in response")
	}
}

func TestSoapSuccessResponse(t *testing.T) {
	resp := soapSuccessResponse()
	if !strings.Contains(resp, "accepted") {
		t.Error("expected 'accepted' in response")
	}
}
