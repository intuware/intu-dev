package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// FHIRDest — extractResourceType
// ===================================================================

func TestFHIRDest_ExtractResourceType_FromMetadata(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	msg.Metadata["resource_type"] = "Observation"
	got := dest.extractResourceType(msg)
	if got != "Observation" {
		t.Fatalf("expected Observation, got %q", got)
	}
}

func TestFHIRDest_ExtractResourceType_FromJSON(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Encounter","id":"1"}`))
	got := dest.extractResourceType(msg)
	if got != "Encounter" {
		t.Fatalf("expected Encounter, got %q", got)
	}
}

func TestFHIRDest_ExtractResourceType_InvalidJSON(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
	msg := message.New("ch", []byte(`not json`))
	got := dest.extractResourceType(msg)
	if got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestFHIRDest_ExtractResourceType_NoField(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
	msg := message.New("ch", []byte(`{"id":"123"}`))
	got := dest.extractResourceType(msg)
	if got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestFHIRDest_ExtractResourceType_MetadataTakesPrecedence(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Patient"}`))
	msg.Metadata["resource_type"] = "Condition"
	got := dest.extractResourceType(msg)
	if got != "Condition" {
		t.Fatalf("expected Condition (from metadata), got %q", got)
	}
}

// ===================================================================
// FHIRDest — determineOperation
// ===================================================================

func TestFHIRDest_DetermineOperation_FromMetadata(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x", Operations: []string{"create"}}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	msg.Metadata["fhir_operation"] = "delete"
	got := dest.determineOperation(msg)
	if got != "delete" {
		t.Fatalf("expected delete, got %q", got)
	}
}

func TestFHIRDest_DetermineOperation_FromConfig(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x", Operations: []string{"update", "create"}}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	got := dest.determineOperation(msg)
	if got != "update" {
		t.Fatalf("expected update (first in config), got %q", got)
	}
}

func TestFHIRDest_DetermineOperation_Default(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	got := dest.determineOperation(msg)
	if got != "create" {
		t.Fatalf("expected create (default), got %q", got)
	}
}

func TestFHIRDest_DetermineOperation_TransactionBundle_AutoDetected(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Bundle","type":"transaction","entry":[]}`))
	got := dest.determineOperation(msg)
	if got != "transaction" {
		t.Fatalf("expected transaction (auto-detected), got %q", got)
	}
}

func TestFHIRDest_DetermineOperation_BatchBundle_AutoDetected(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Bundle","type":"batch","entry":[]}`))
	got := dest.determineOperation(msg)
	if got != "batch" {
		t.Fatalf("expected batch (auto-detected), got %q", got)
	}
}

func TestFHIRDest_DetermineOperation_PlainBundle_DefaultsToCreate(t *testing.T) {
	cases := []string{"document", "collection", "message", "searchset", "history", ""}
	for _, bundleType := range cases {
		t.Run("type="+bundleType, func(t *testing.T) {
			dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
			body := `{"resourceType":"Bundle","type":"` + bundleType + `"}`
			msg := message.New("ch", []byte(body))
			got := dest.determineOperation(msg)
			if got != "create" {
				t.Fatalf("type=%q: expected create, got %q", bundleType, got)
			}
		})
	}
}

func TestFHIRDest_DetermineOperation_MetadataOverridesAutoDetect(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Bundle","type":"transaction"}`))
	msg.Metadata["fhir_operation"] = "create"
	got := dest.determineOperation(msg)
	if got != "create" {
		t.Fatalf("expected metadata 'create' to win over auto-detect, got %q", got)
	}
}

func TestFHIRDest_DetermineOperation_ConfigOperationsOverridesAutoDetect(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x", Operations: []string{"create"}}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Bundle","type":"transaction"}`))
	got := dest.determineOperation(msg)
	if got != "create" {
		t.Fatalf("expected cfg.Operations 'create' to win over auto-detect, got %q", got)
	}
}

func TestFHIRDest_DetermineOperation_TransactionBundle_CaseInsensitive(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Bundle","type":"TRANSACTION"}`))
	got := dest.determineOperation(msg)
	if got != "transaction" {
		t.Fatalf("expected transaction (case-insensitive), got %q", got)
	}
}

func TestFHIRDest_DetermineOperation_NonBundleIgnored(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Patient","type":"transaction"}`))
	got := dest.determineOperation(msg)
	if got != "create" {
		t.Fatalf("expected create (non-Bundle resource), got %q", got)
	}
}

func TestFHIRDest_BuildURL_AutoDetectedTransaction_TargetsBase(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Bundle","type":"transaction","entry":[]}`))
	op := dest.determineOperation(msg)
	got := dest.buildURL("Bundle", op, msg)
	if op != "transaction" {
		t.Fatalf("expected auto-detected op 'transaction', got %q", op)
	}
	if got != "http://fhir.test/r4" {
		t.Fatalf("expected base URL, got %q", got)
	}
}

// ===================================================================
// FHIRDest — buildURL
// ===================================================================

func TestFHIRDest_BuildURL_Create_WithResource(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	got := dest.buildURL("Patient", "create", msg)
	if got != "http://fhir.test/r4/Patient" {
		t.Fatalf("got %q", got)
	}
}

func TestFHIRDest_BuildURL_Create_NoResource(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	got := dest.buildURL("", "create", msg)
	if got != "http://fhir.test/r4" {
		t.Fatalf("got %q", got)
	}
}

func TestFHIRDest_BuildURL_Update_WithID_FromMetadata(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Patient"}`))
	msg.Metadata["resource_id"] = "abc"
	got := dest.buildURL("Patient", "update", msg)
	if got != "http://fhir.test/r4/Patient/abc" {
		t.Fatalf("got %q", got)
	}
}

func TestFHIRDest_BuildURL_Update_WithID_FromJSON(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Patient","id":"xyz"}`))
	got := dest.buildURL("Patient", "update", msg)
	if got != "http://fhir.test/r4/Patient/xyz" {
		t.Fatalf("got %q", got)
	}
}

func TestFHIRDest_BuildURL_Update_NoID(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Patient"}`))
	got := dest.buildURL("Patient", "update", msg)
	if got != "http://fhir.test/r4/Patient" {
		t.Fatalf("got %q", got)
	}
}

func TestFHIRDest_BuildURL_Update_NoResource(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	got := dest.buildURL("", "update", msg)
	if got != "http://fhir.test/r4" {
		t.Fatalf("got %q", got)
	}
}

func TestFHIRDest_BuildURL_Transaction(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4/"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	got := dest.buildURL("Bundle", "transaction", msg)
	if got != "http://fhir.test/r4" {
		t.Fatalf("got %q", got)
	}
}

func TestFHIRDest_BuildURL_Batch(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4/"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	got := dest.buildURL("Bundle", "batch", msg)
	if got != "http://fhir.test/r4" {
		t.Fatalf("got %q", got)
	}
}

func TestFHIRDest_BuildURL_UnknownOp_WithResource(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	got := dest.buildURL("Observation", "custom", msg)
	if got != "http://fhir.test/r4/Observation" {
		t.Fatalf("got %q", got)
	}
}

func TestFHIRDest_BuildURL_UnknownOp_NoResource(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	got := dest.buildURL("", "custom", msg)
	if got != "http://fhir.test/r4" {
		t.Fatalf("got %q", got)
	}
}

func TestFHIRDest_BuildURL_TrailingSlashStripped(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4///"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	got := dest.buildURL("Patient", "create", msg)
	if got != "http://fhir.test/r4/Patient" {
		t.Fatalf("got %q", got)
	}
}

// ===================================================================
// FHIRDest — httpMethod
// ===================================================================

func TestFHIRDest_HttpMethod_AllOps(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x"}, testLogger())

	cases := []struct {
		op     string
		method string
	}{
		{"create", "POST"},
		{"update", "PUT"},
		{"delete", "DELETE"},
		{"read", "GET"},
		{"transaction", "POST"},
		{"batch", "POST"},
		{"unknown", "POST"},
		{"", "POST"},
	}
	for _, tc := range cases {
		got := dest.httpMethod(tc.op)
		if got != tc.method {
			t.Errorf("httpMethod(%q) = %q, want %q", tc.op, got, tc.method)
		}
	}
}

// ===================================================================
// FHIRDest — Send with httptest (read/delete body handling)
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

func TestFHIRDest_Send_ReadHasNoBody(t *testing.T) {
	var receivedMethod string
	var bodyLen int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		b, _ := io.ReadAll(r.Body)
		bodyLen = len(b)
		w.WriteHeader(200)
		w.Write([]byte(`{"resourceType":"Patient"}`))
	}))
	defer srv.Close()

	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: srv.URL + "/fhir"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	msg.Metadata["fhir_operation"] = "read"
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if receivedMethod != "GET" {
		t.Fatalf("expected GET, got %s", receivedMethod)
	}
	if bodyLen != 0 {
		t.Fatalf("expected empty body for GET, got %d bytes", bodyLen)
	}
}

func TestFHIRDest_Send_TransportStamped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
	}))
	defer srv.Close()

	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: srv.URL + "/fhir"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Patient"}`))
	msg.Transport = "http"
	msg.HTTP = &message.HTTPMeta{Method: "POST"}

	_, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if msg.Transport != "fhir" {
		t.Fatalf("expected transport 'fhir', got %q", msg.Transport)
	}
	if msg.HTTP == nil {
		t.Fatal("expected HTTP meta to be set for FHIR")
	}
	if msg.HTTP.Method != "POST" {
		t.Fatalf("expected method POST in meta, got %q", msg.HTTP.Method)
	}
}

func TestFHIRDest_Send_APIKeyHeader(t *testing.T) {
	var keyVal string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keyVal = r.Header.Get("X-FHIR-Key")
		w.WriteHeader(201)
	}))
	defer srv.Close()

	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{
		BaseURL: srv.URL + "/fhir",
		Auth:    &config.HTTPAuthConfig{Type: "api_key", Key: "secret-key", Header: "X-FHIR-Key"},
	}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Patient"}`))
	dest.Send(context.Background(), msg)
	if keyVal != "secret-key" {
		t.Fatalf("expected secret-key, got %q", keyVal)
	}
}

func TestFHIRDest_Send_APIKeyQueryParam(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.URL.Query().Get("apikey")
		w.WriteHeader(201)
	}))
	defer srv.Close()

	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{
		BaseURL: srv.URL + "/fhir",
		Auth:    &config.HTTPAuthConfig{Type: "api_key", Key: "qp-val", QueryParam: "apikey"},
	}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Patient"}`))
	dest.Send(context.Background(), msg)
	if gotKey != "qp-val" {
		t.Fatalf("expected qp-val, got %q", gotKey)
	}
}

func TestFHIRDest_Send_ContentTypeSetToFHIR(t *testing.T) {
	var ct, accept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ct = r.Header.Get("Content-Type")
		accept = r.Header.Get("Accept")
		w.WriteHeader(201)
	}))
	defer srv.Close()

	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: srv.URL + "/fhir"}, testLogger())
	msg := message.New("ch", []byte(`{"resourceType":"Patient"}`))
	dest.Send(context.Background(), msg)
	if ct != "application/fhir+json" {
		t.Fatalf("expected fhir+json content type, got %q", ct)
	}
	if accept != "application/fhir+json" {
		t.Fatalf("expected fhir+json accept, got %q", accept)
	}
}

func TestFHIRDest_Send_CustomTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
	}))
	defer srv.Close()

	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: srv.URL, TimeoutMs: 5000}, testLogger())
	if dest.client.Timeout.Milliseconds() != 5000 {
		t.Fatalf("expected 5s timeout, got %v", dest.client.Timeout)
	}
}

// ===================================================================
// JMSDest — buildEndpoint
// ===================================================================

func TestJMSDest_BuildEndpoint_ActiveMQ(t *testing.T) {
	dest := NewJMSDest("t", &config.JMSDestMapConfig{
		Provider: "activemq", URL: "http://mq.local:8161", Queue: "hl7-in",
	}, testLogger())
	got := dest.buildEndpoint()
	if got != "http://mq.local:8161/api/message/hl7-in?type=queue" {
		t.Fatalf("got %q", got)
	}
}

func TestJMSDest_BuildEndpoint_DefaultProvider(t *testing.T) {
	dest := NewJMSDest("t", &config.JMSDestMapConfig{
		Provider: "rabbitmq", URL: "http://rabbit:15672", Queue: "events",
	}, testLogger())
	got := dest.buildEndpoint()
	if got != "http://rabbit:15672/api/message/events?type=queue" {
		t.Fatalf("got %q", got)
	}
}

func TestJMSDest_BuildEndpoint_EmptyProvider(t *testing.T) {
	dest := NewJMSDest("t", &config.JMSDestMapConfig{
		URL: "http://broker:8080", Queue: "q",
	}, testLogger())
	got := dest.buildEndpoint()
	if got != "http://broker:8080/api/message/q?type=queue" {
		t.Fatalf("got %q", got)
	}
}

// ===================================================================
// JMSDest — applyAuth
// ===================================================================

func TestJMSDest_ApplyAuth_APIKey(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-JMS-Key")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewJMSDest("t", &config.JMSDestMapConfig{
		URL:   srv.URL,
		Queue: "q",
		Auth:  &config.HTTPAuthConfig{Type: "api_key", Key: "jms-api-key", Header: "X-JMS-Key"},
	}, testLogger())
	msg := message.New("ch", []byte("body"))
	dest.Send(context.Background(), msg)
	if gotKey != "jms-api-key" {
		t.Fatalf("expected jms-api-key, got %q", gotKey)
	}
}

func TestJMSDest_ApplyAuth_Nil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Errorf("expected no auth header")
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewJMSDest("t", &config.JMSDestMapConfig{URL: srv.URL, Queue: "q"}, testLogger())
	dest.Send(context.Background(), message.New("ch", []byte("body")))
}

// ===================================================================
// JMSDest — content type mapping
// ===================================================================

func TestJMSDest_ContentTypeMapping(t *testing.T) {
	cases := []struct {
		name   string
		ct     message.ContentType
		wantCT string
	}{
		{"json", message.ContentTypeJSON, "application/json"},
		{"xml", message.ContentTypeXML, "text/xml"},
		{"hl7v2", message.ContentTypeHL7v2, "x-application/hl7-v2+er7"},
		{"default", message.ContentTypeBinary, "application/octet-stream"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotCT string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotCT = r.Header.Get("Content-Type")
				w.WriteHeader(200)
			}))
			defer srv.Close()

			dest := NewJMSDest("t", &config.JMSDestMapConfig{URL: srv.URL, Queue: "q"}, testLogger())
			msg := message.New("ch", []byte("body"))
			msg.ContentType = tc.ct
			dest.Send(context.Background(), msg)
			if gotCT != tc.wantCT {
				t.Fatalf("expected %q, got %q", tc.wantCT, gotCT)
			}
		})
	}
}

// ===================================================================
// JMSDest — transport stamping
// ===================================================================

func TestJMSDest_TransportStamping(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewJMSDest("t", &config.JMSDestMapConfig{URL: srv.URL, Queue: "q"}, testLogger())
	msg := message.New("ch", []byte("body"))
	msg.Transport = "http"
	dest.Send(context.Background(), msg)
	if msg.Transport != "jms" {
		t.Fatalf("expected transport 'jms', got %q", msg.Transport)
	}
	if msg.HTTP == nil || msg.HTTP.Method != "POST" {
		t.Fatal("expected HTTP meta with POST method")
	}
}

func TestJMSDest_Stop(t *testing.T) {
	dest := NewJMSDest("t", &config.JMSDestMapConfig{URL: "http://x", Queue: "q"}, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ===================================================================
// DirectDest — constructor and helpers
// ===================================================================

func TestDirectDest_Constructor(t *testing.T) {
	cfg := &config.DirectDestMapConfig{To: "a@direct.test", From: "b@direct.test"}
	dest := NewDirectDest("dd", cfg, testLogger())
	if dest.name != "dd" {
		t.Fatalf("expected name 'dd', got %q", dest.name)
	}
	if dest.cfg != cfg {
		t.Fatal("cfg mismatch")
	}
}

func TestDirectDest_StopIsNoop(t *testing.T) {
	dest := NewDirectDest("t", &config.DirectDestMapConfig{To: "a@b", From: "c@d"}, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestDirectDest_SMTPHostDerivedFromTo(t *testing.T) {
	dest := NewDirectDest("t", &config.DirectDestMapConfig{
		To:   "user@direct.hospital.org",
		From: "sender@lab.org",
	}, testLogger())

	msg := message.New("ch", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	// Connection will fail since the derived host doesn't exist, but it proves
	// the code path for deriving SMTPHost from the To address is exercised
	if resp.Error == nil {
		t.Fatal("expected connection error to derived host")
	}
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502, got %d", resp.StatusCode)
	}
}

func TestDirectDest_DefaultPort465(t *testing.T) {
	dest := NewDirectDest("t", &config.DirectDestMapConfig{
		To:       "user@direct.example.com",
		From:     "sender@example.com",
		SMTPHost: "127.0.0.1",
		// SMTPPort left at 0 → should default to 465
	}, testLogger())
	msg := message.New("ch", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.Error == nil {
		t.Fatal("expected connection error")
	}
}

// ===================================================================
// SMTPDest — constructor and helpers
// ===================================================================

func TestSMTPDest_Constructor(t *testing.T) {
	cfg := &config.SMTPDestMapConfig{Host: "smtp.test", From: "a@b", To: []string{"c@d"}}
	dest := NewSMTPDest("sd", cfg, testLogger())
	if dest.name != "sd" {
		t.Fatalf("expected name 'sd', got %q", dest.name)
	}
	if dest.cfg != cfg {
		t.Fatal("cfg mismatch")
	}
}

func TestSMTPDest_StopIsNoop(t *testing.T) {
	dest := NewSMTPDest("t", &config.SMTPDestMapConfig{Host: "x"}, testLogger())
	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSMTPDest_DefaultPort25(t *testing.T) {
	// No TLS, port 0 → should default to 25
	dest := NewSMTPDest("t", &config.SMTPDestMapConfig{
		Host: "127.0.0.1",
		Port: 0,
		From: "a@test.com",
		To:   []string{"b@test.com"},
	}, testLogger())
	msg := message.New("ch", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	// Will fail to connect, but exercises the default port path
	if resp.Error == nil {
		t.Fatal("expected connection error")
	}
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502, got %d", resp.StatusCode)
	}
}

func TestSMTPDest_DefaultPort465_WithTLS(t *testing.T) {
	dest := NewSMTPDest("t", &config.SMTPDestMapConfig{
		Host: "127.0.0.1",
		Port: 0,
		From: "a@test.com",
		To:   []string{"b@test.com"},
		TLS:  &config.TLSMapConfig{Enabled: true, InsecureSkipVerify: true},
	}, testLogger())
	msg := message.New("ch", []byte("test"))
	resp, _ := dest.Send(context.Background(), msg)
	if resp.Error == nil {
		t.Fatal("expected connection error")
	}
}

// ===================================================================
// DatabaseDest — driverName (additional edge cases)
// ===================================================================

func TestDatabaseDest_DriverName_CaseInsensitive(t *testing.T) {
	cases := []struct {
		input, want string
	}{
		{"POSTGRES", "pgx"},
		{"PostgreSQL", "pgx"},
		{"MYSQL", "mysql"},
		{"MSSQL", "sqlserver"},
		{"SQLServer", "sqlserver"},
		{"SQLITE", "sqlite"},
		{"SQLITE3", "sqlite"},
		{"oracle", "oracle"},
	}

	for _, tc := range cases {
		dest := NewDatabaseDest("t", &config.DBDestMapConfig{Driver: tc.input, DSN: "x"}, testLogger())
		got := dest.driverName()
		if got != tc.want {
			t.Errorf("driverName(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// ===================================================================
// DatabaseSource — driverName
// ===================================================================

func TestDatabaseSource_DriverName(t *testing.T) {
	cases := []struct {
		input, want string
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

	for _, tc := range cases {
		src := NewDatabaseSource(&config.DBListener{Driver: tc.input, DSN: "x"}, testLogger())
		got := src.driverName()
		if got != tc.want {
			t.Errorf("driverName(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// ===================================================================
// DatabaseSource — Type and Stop
// ===================================================================

func TestDatabaseSource_TypeReturnsDatabase(t *testing.T) {
	src := NewDatabaseSource(&config.DBListener{Driver: "postgres", DSN: "x"}, testLogger())
	if src.Type() != "database" {
		t.Fatalf("expected 'database', got %q", src.Type())
	}
}

func TestDatabaseSource_StopBeforeStart(t *testing.T) {
	src := NewDatabaseSource(&config.DBListener{Driver: "postgres", DSN: "x"}, testLogger())
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// ===================================================================
// Kafka helpers — appendInt16, appendInt32, appendInt64, appendKafkaString
// ===================================================================

func TestAppendInt16(t *testing.T) {
	buf := appendInt16(nil, 0x0102)
	if len(buf) != 2 || buf[0] != 0x01 || buf[1] != 0x02 {
		t.Fatalf("expected [01 02], got %v", buf)
	}
}

func TestAppendInt16_Zero(t *testing.T) {
	buf := appendInt16(nil, 0)
	if len(buf) != 2 || buf[0] != 0 || buf[1] != 0 {
		t.Fatalf("expected [00 00], got %v", buf)
	}
}

func TestAppendInt16_Negative(t *testing.T) {
	buf := appendInt16(nil, -1)
	if len(buf) != 2 || buf[0] != 0xFF || buf[1] != 0xFF {
		t.Fatalf("expected [FF FF], got %02X %02X", buf[0], buf[1])
	}
}

func TestAppendInt32(t *testing.T) {
	buf := appendInt32(nil, 0x01020304)
	if len(buf) != 4 || buf[0] != 1 || buf[1] != 2 || buf[2] != 3 || buf[3] != 4 {
		t.Fatalf("expected [01 02 03 04], got %v", buf)
	}
}

func TestAppendInt32_Negative(t *testing.T) {
	buf := appendInt32(nil, -1)
	if len(buf) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(buf))
	}
	for i, b := range buf {
		if b != 0xFF {
			t.Fatalf("byte %d: expected 0xFF, got 0x%02X", i, b)
		}
	}
}

func TestAppendInt64(t *testing.T) {
	buf := appendInt64(nil, 0x0102030405060708)
	if len(buf) != 8 {
		t.Fatalf("expected 8 bytes, got %d", len(buf))
	}
	expected := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	for i, b := range buf {
		if b != expected[i] {
			t.Fatalf("byte %d: expected %d, got %d", i, expected[i], b)
		}
	}
}

func TestAppendInt64_Zero(t *testing.T) {
	buf := appendInt64(nil, 0)
	if len(buf) != 8 {
		t.Fatalf("expected 8 bytes")
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("byte %d should be 0", i)
		}
	}
}

func TestAppendKafkaString(t *testing.T) {
	buf := appendKafkaString(nil, "hello")
	// 2 bytes length (0, 5) + 5 bytes "hello"
	if len(buf) != 7 {
		t.Fatalf("expected 7 bytes, got %d", len(buf))
	}
	if buf[0] != 0 || buf[1] != 5 {
		t.Fatalf("expected length [0 5], got [%d %d]", buf[0], buf[1])
	}
	if string(buf[2:]) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(buf[2:]))
	}
}

func TestAppendKafkaString_Empty(t *testing.T) {
	buf := appendKafkaString(nil, "")
	if len(buf) != 2 {
		t.Fatalf("expected 2 bytes, got %d", len(buf))
	}
	if buf[0] != 0 || buf[1] != 0 {
		t.Fatalf("expected [0 0], got %v", buf)
	}
}

func TestAppendKafkaString_Appended(t *testing.T) {
	initial := []byte{0xAA, 0xBB}
	buf := appendKafkaString(initial, "ab")
	if len(buf) != 6 {
		t.Fatalf("expected 6 bytes, got %d", len(buf))
	}
	if buf[0] != 0xAA || buf[1] != 0xBB {
		t.Fatal("initial bytes corrupted")
	}
	if string(buf[4:]) != "ab" {
		t.Fatalf("expected 'ab', got %q", string(buf[4:]))
	}
}

// ===================================================================
// KafkaSource — buildDebugInfo, debugJSON
// ===================================================================

func TestKafkaSource_BuildDebugInfo_Basic(t *testing.T) {
	src := NewKafkaSource(&config.KafkaListener{
		Brokers: []string{"b1:9092", "b2:9092"},
		Topic:   "my-topic",
	}, testLogger())

	info := src.buildDebugInfo()
	if info["type"] != "kafka" {
		t.Fatalf("expected type 'kafka', got %v", info["type"])
	}
	brokers := info["brokers"].([]string)
	if len(brokers) != 2 || brokers[0] != "b1:9092" {
		t.Fatalf("unexpected brokers: %v", brokers)
	}
	if info["topic"] != "my-topic" {
		t.Fatalf("expected topic 'my-topic', got %v", info["topic"])
	}
	if _, ok := info["group_id"]; ok {
		t.Fatal("group_id should be absent when empty")
	}
	if _, ok := info["offset"]; ok {
		t.Fatal("offset should be absent when empty")
	}
}

func TestKafkaSource_BuildDebugInfo_WithGroupAndOffset(t *testing.T) {
	src := NewKafkaSource(&config.KafkaListener{
		Brokers: []string{"b:9092"},
		Topic:   "t",
		GroupID: "g1",
		Offset:  "earliest",
	}, testLogger())

	info := src.buildDebugInfo()
	if info["group_id"] != "g1" {
		t.Fatalf("expected group_id 'g1', got %v", info["group_id"])
	}
	if info["offset"] != "earliest" {
		t.Fatalf("expected offset 'earliest', got %v", info["offset"])
	}
}

func TestKafkaSource_DebugJSON(t *testing.T) {
	src := NewKafkaSource(&config.KafkaListener{
		Brokers: []string{"localhost:9092"},
		Topic:   "test-topic",
		GroupID: "grp",
	}, testLogger())

	js := src.debugJSON()
	var parsed map[string]any
	if err := json.Unmarshal([]byte(js), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if parsed["topic"] != "test-topic" {
		t.Fatalf("expected topic in JSON, got %v", parsed["topic"])
	}
	if parsed["group_id"] != "grp" {
		t.Fatalf("expected group_id in JSON, got %v", parsed["group_id"])
	}
}

// ===================================================================
// KafkaDest — clientID
// ===================================================================

func TestKafkaDest_ClientID_Custom(t *testing.T) {
	dest := NewKafkaDest("t", &config.KafkaDestConfig{
		Brokers:  []string{"b:9092"},
		Topic:    "t",
		ClientID: "my-client",
	}, testLogger())
	if dest.clientID() != "my-client" {
		t.Fatalf("expected 'my-client', got %q", dest.clientID())
	}
}

func TestKafkaDest_ClientID_Default(t *testing.T) {
	dest := NewKafkaDest("t", &config.KafkaDestConfig{
		Brokers: []string{"b:9092"},
		Topic:   "t",
	}, testLogger())
	if dest.clientID() != "intu-kafka-dest" {
		t.Fatalf("expected 'intu-kafka-dest', got %q", dest.clientID())
	}
}

// ===================================================================
// KafkaSource — parseMessageSet
// ===================================================================

func TestKafkaSource_ParseMessageSet_Empty(t *testing.T) {
	src := NewKafkaSource(&config.KafkaListener{Brokers: []string{"b"}, Topic: "t"}, testLogger())
	msgs := src.parseMessageSet(nil)
	if len(msgs) != 0 {
		t.Fatalf("expected 0, got %d", len(msgs))
	}
}

func TestKafkaSource_ParseMessageSet_TooShort(t *testing.T) {
	src := NewKafkaSource(&config.KafkaListener{Brokers: []string{"b"}, Topic: "t"}, testLogger())
	msgs := src.parseMessageSet([]byte{0, 0, 0, 1})
	if len(msgs) != 0 {
		t.Fatalf("expected 0, got %d", len(msgs))
	}
}

// ===================================================================
// FHIRPollSource — buildQueries
// ===================================================================

func TestFHIRPollSource_BuildQueries_ResourcesOnly(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "http://fhir.test/r4",
		Resources: []string{"Patient", "Observation"},
	}, testLogger())

	queries := src.buildQueries("http://fhir.test/r4", "", "")
	if len(queries) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(queries))
	}
	if !strings.Contains(queries[0], "/Patient?") {
		t.Fatalf("expected Patient query, got %q", queries[0])
	}
	if !strings.Contains(queries[0], "_count=200") {
		t.Fatalf("expected _count in query, got %q", queries[0])
	}
	if !strings.Contains(queries[1], "/Observation?") {
		t.Fatalf("expected Observation query, got %q", queries[1])
	}
}

func TestFHIRPollSource_BuildQueries_ResourcesWithTime(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "http://fhir.test/r4",
		Resources: []string{"Patient"},
	}, testLogger())

	queries := src.buildQueries("http://fhir.test/r4", "2025-01-01T00:00:00Z", "_lastUpdated")
	if len(queries) != 1 {
		t.Fatalf("expected 1 query, got %d", len(queries))
	}
	if !strings.Contains(queries[0], "_lastUpdated=ge") {
		t.Fatalf("expected date filter, got %q", queries[0])
	}
	if !strings.Contains(queries[0], "_count=200") {
		t.Fatalf("expected _count, got %q", queries[0])
	}
}

func TestFHIRPollSource_BuildQueries_SearchQueries(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:       "http://fhir.test/r4",
		SearchQueries: []string{"Patient?name=Smith", "/Observation?code=1234"},
	}, testLogger())

	queries := src.buildQueries("http://fhir.test/r4", "", "")
	if len(queries) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(queries))
	}
	if queries[0] != "http://fhir.test/r4/Patient?name=Smith" {
		t.Fatalf("unexpected query: %q", queries[0])
	}
	if queries[1] != "http://fhir.test/r4/Observation?code=1234" {
		t.Fatalf("unexpected query: %q", queries[1])
	}
}

func TestFHIRPollSource_BuildQueries_SearchQueriesWithTime(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:       "http://fhir.test/r4",
		SearchQueries: []string{"Patient?name=Smith", "Encounter"},
	}, testLogger())

	queries := src.buildQueries("http://fhir.test/r4", "2025-01-01T00:00:00Z", "_lastUpdated")
	if len(queries) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(queries))
	}
	// query with ? already present → date appended with &
	if !strings.Contains(queries[0], "name=Smith") || !strings.Contains(queries[0], "&_lastUpdated=ge") {
		t.Fatalf("unexpected query: %q", queries[0])
	}
	// query without ? → date appended with ?
	if !strings.Contains(queries[1], "?_lastUpdated=ge") {
		t.Fatalf("unexpected query: %q", queries[1])
	}
}

func TestFHIRPollSource_BuildQueries_EmptyResourceSkipped(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "http://fhir.test/r4",
		Resources: []string{"Patient", "", "  "},
	}, testLogger())

	queries := src.buildQueries("http://fhir.test/r4", "", "")
	if len(queries) != 1 {
		t.Fatalf("expected 1 (empty strings skipped), got %d", len(queries))
	}
}

func TestFHIRPollSource_BuildQueries_EmptySearchQuerySkipped(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:       "http://fhir.test/r4",
		SearchQueries: []string{"Patient", "", "  "},
	}, testLogger())

	queries := src.buildQueries("http://fhir.test/r4", "", "")
	if len(queries) != 1 {
		t.Fatalf("expected 1 (empty strings skipped), got %d", len(queries))
	}
}

func TestFHIRPollSource_BuildQueries_NoTimeNoQueryParam(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:       "http://fhir.test/r4",
		SearchQueries: []string{"Patient"},
	}, testLogger())

	queries := src.buildQueries("http://fhir.test/r4", "", "")
	if len(queries) != 1 {
		t.Fatalf("expected 1 query, got %d", len(queries))
	}
	// No time → no date param, but _count should be added for queries without ?
	if !strings.Contains(queries[0], "_count=200") {
		t.Fatalf("expected _count for query without ?, got %q", queries[0])
	}
}

// ===================================================================
// FHIRPollSource — emitFromResponse
// ===================================================================

func TestFHIRPollSource_EmitFromResponse_Bundle(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "http://fhir.test",
		Resources: []string{"Patient"},
		Version:   "R4",
	}, testLogger())

	body := `{
		"resourceType": "Bundle",
		"type": "searchset",
		"entry": [
			{"resource": {"resourceType": "Patient", "id": "1", "name": [{"family": "Doe"}]}},
			{"resource": {"resourceType": "Patient", "id": "2", "name": [{"family": "Smith"}]}}
		]
	}`

	capture := &msgCapture{}
	err := src.emitFromResponse(context.Background(), capture.handler(), []byte(body))
	if err != nil {
		t.Fatalf("emitFromResponse: %v", err)
	}
	if capture.count() != 2 {
		t.Fatalf("expected 2 messages, got %d", capture.count())
	}
	if capture.get(0).Metadata["resource_type"] != "Patient" {
		t.Fatalf("expected resource_type Patient, got %v", capture.get(0).Metadata["resource_type"])
	}
	if capture.get(0).Metadata["resource_id"] != "1" {
		t.Fatalf("expected resource_id 1, got %v", capture.get(0).Metadata["resource_id"])
	}
	if capture.get(0).Transport != "fhir_poll" {
		t.Fatalf("expected transport fhir_poll, got %q", capture.get(0).Transport)
	}
	if capture.get(0).Metadata["fhir_version"] != "R4" {
		t.Fatalf("expected fhir_version R4, got %v", capture.get(0).Metadata["fhir_version"])
	}
}

func TestFHIRPollSource_EmitFromResponse_SingleResource(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "http://fhir.test",
		Resources: []string{"Patient"},
	}, testLogger())

	body := `{"resourceType": "Patient", "id": "42"}`

	capture := &msgCapture{}
	err := src.emitFromResponse(context.Background(), capture.handler(), []byte(body))
	if err != nil {
		t.Fatalf("emitFromResponse: %v", err)
	}
	if capture.count() != 1 {
		t.Fatalf("expected 1, got %d", capture.count())
	}
	if capture.get(0).Metadata["resource_type"] != "Patient" {
		t.Fatalf("expected resource_type Patient, got %v", capture.get(0).Metadata["resource_type"])
	}
	if capture.get(0).Metadata["resource_id"] != "42" {
		t.Fatalf("expected resource_id 42, got %v", capture.get(0).Metadata["resource_id"])
	}
}

func TestFHIRPollSource_EmitFromResponse_EmptyBundle(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "http://fhir.test",
		Resources: []string{"Patient"},
	}, testLogger())

	body := `{"resourceType": "Bundle", "type": "searchset", "entry": []}`
	capture := &msgCapture{}
	err := src.emitFromResponse(context.Background(), capture.handler(), []byte(body))
	if err != nil {
		t.Fatalf("emitFromResponse: %v", err)
	}
	if capture.count() != 0 {
		t.Fatalf("expected 0 for empty bundle, got %d", capture.count())
	}
}

func TestFHIRPollSource_EmitFromResponse_InvalidJSON(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "http://fhir.test",
		Resources: []string{"Patient"},
	}, testLogger())

	err := src.emitFromResponse(context.Background(), noopHandler, []byte("not json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestFHIRPollSource_EmitFromResponse_BundleWithNullResource(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "http://fhir.test",
		Resources: []string{"Patient"},
	}, testLogger())

	body := `{
		"resourceType": "Bundle",
		"type": "searchset",
		"entry": [
			{"resource": null},
			{"resource": {"resourceType": "Patient", "id": "1"}}
		]
	}`
	capture := &msgCapture{}
	err := src.emitFromResponse(context.Background(), capture.handler(), []byte(body))
	if err != nil {
		t.Fatalf("emitFromResponse: %v", err)
	}
	if capture.count() != 1 {
		t.Fatalf("expected 1 (null resource skipped), got %d", capture.count())
	}
}

func TestFHIRPollSource_EmitFromResponse_NoResourceType(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "http://fhir.test",
		Resources: []string{"Patient"},
	}, testLogger())

	body := `{"id": "123", "data": "value"}`
	capture := &msgCapture{}
	err := src.emitFromResponse(context.Background(), capture.handler(), []byte(body))
	if err != nil {
		t.Fatalf("emitFromResponse: %v", err)
	}
	if capture.count() != 1 {
		t.Fatalf("expected 1, got %d", capture.count())
	}
	if capture.get(0).Metadata["resource_type"] != nil && capture.get(0).Metadata["resource_type"] != "" {
		t.Fatalf("expected empty resource_type, got %v", capture.get(0).Metadata["resource_type"])
	}
}

// ===================================================================
// FHIRPollSource — httptest integration
// ===================================================================

func TestFHIRPollSource_EmitFromResponse_HandlerError(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "http://fhir.test",
		Resources: []string{"Patient"},
	}, testLogger())

	body := `{"resourceType": "Patient", "id": "1"}`
	handler := func(_ context.Context, _ *message.Message) error {
		return io.ErrUnexpectedEOF
	}
	err := src.emitFromResponse(context.Background(), handler, []byte(body))
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected handler error to propagate, got %v", err)
	}
}

func TestFHIRPollSource_EmitFromResponse_BundleHandlerError(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:   "http://fhir.test",
		Resources: []string{"Patient"},
	}, testLogger())

	body := `{
		"resourceType": "Bundle",
		"type": "searchset",
		"entry": [
			{"resource": {"resourceType": "Patient", "id": "1"}},
			{"resource": {"resourceType": "Patient", "id": "2"}}
		]
	}`
	callCount := 0
	handler := func(_ context.Context, _ *message.Message) error {
		callCount++
		if callCount == 2 {
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	err := src.emitFromResponse(context.Background(), handler, []byte(body))
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected handler error on second entry, got %v", err)
	}
	if callCount != 2 {
		t.Fatalf("expected 2 calls, got %d", callCount)
	}
}

// ===================================================================
// FHIRPollSource — SearchQueries with existing ? and time param
// ===================================================================

func TestFHIRPollSource_BuildQueries_Mixed(t *testing.T) {
	src := NewFHIRPollSource(&config.FHIRPollListener{
		BaseURL:       "http://fhir.test/r4",
		Resources:     []string{"Patient"},
		SearchQueries: []string{"Observation?code=vital-signs"},
	}, testLogger())

	queries := src.buildQueries("http://fhir.test/r4", "2025-06-01T00:00:00Z", "_lastUpdated")
	if len(queries) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(queries))
	}
	// Resource query
	if !strings.HasPrefix(queries[0], "http://fhir.test/r4/Patient?") {
		t.Fatalf("unexpected resource query: %q", queries[0])
	}
	// Search query with existing ?
	if !strings.Contains(queries[1], "code=vital-signs") {
		t.Fatalf("expected code param preserved: %q", queries[1])
	}
	if !strings.Contains(queries[1], "&_lastUpdated=ge") {
		t.Fatalf("expected date appended with &: %q", queries[1])
	}
}
