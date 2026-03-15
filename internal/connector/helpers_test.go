package connector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// mergeMaps
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
// readMLLP / readRawTCP
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
	buf.WriteByte(0x0A)
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
// buildMLLPACK
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
// extractHL7ControlID / extractHL7AckCode edge cases
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
// appendInt16, appendInt32, appendInt64, appendKafkaString
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

func TestAppendKafkaString(t *testing.T) {
	buf := appendKafkaString(nil, "hello")
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

// ===================================================================
// KafkaSource — parseMessageSet, buildDebugInfo, debugJSON
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
}

// ===================================================================
// DatabaseSource / DatabaseDest — driverName
// ===================================================================

func TestDatabaseSource_DriverName(t *testing.T) {
	cases := []struct {
		input, want string
	}{
		{"postgres", "postgres"},
		{"postgresql", "postgres"},
		{"mysql", "mysql"},
		{"mssql", "sqlserver"},
		{"sqlserver", "sqlserver"},
		{"sqlite", "sqlite3"},
		{"sqlite3", "sqlite3"},
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

func TestDatabaseDest_DriverName(t *testing.T) {
	tests := []struct {
		driver   string
		expected string
	}{
		{"postgres", "postgres"},
		{"postgresql", "postgres"},
		{"mysql", "mysql"},
		{"mssql", "sqlserver"},
		{"sqlserver", "sqlserver"},
		{"sqlite", "sqlite3"},
		{"sqlite3", "sqlite3"},
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
// SFTPSource / SFTPDest — authUsername, buildAuthMethods, sortEntries
// ===================================================================

func TestSFTPSource_AuthUsername_WithAuth(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{
		Auth: &config.AuthConfig{Username: "myuser"},
	}, testLogger())
	if got := s.authUsername(); got != "myuser" {
		t.Fatalf("expected 'myuser', got %q", got)
	}
}

func TestSFTPSource_AuthUsername_NilAuth(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{}, testLogger())
	if got := s.authUsername(); got != "anonymous" {
		t.Fatalf("expected 'anonymous', got %q", got)
	}
}

func TestSFTPDest_AuthUsername_WithAuth(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{
		Auth: &config.HTTPAuthConfig{Username: "sftpuser"},
	}, testLogger())
	if got := d.authUsername(); got != "sftpuser" {
		t.Fatalf("expected 'sftpuser', got %q", got)
	}
}

func TestSFTPDest_AuthUsername_NilAuth(t *testing.T) {
	d := NewSFTPDest("test", &config.SFTPDestMapConfig{}, testLogger())
	if got := d.authUsername(); got != "anonymous" {
		t.Fatalf("expected 'anonymous', got %q", got)
	}
}

type mockFileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (m mockFileInfo) Name() string      { return m.name }
func (m mockFileInfo) Size() int64       { return m.size }
func (m mockFileInfo) ModTime() time.Time { return m.modTime }
func (m mockFileInfo) Mode() os.FileMode  { return 0o644 }
func (m mockFileInfo) IsDir() bool        { return false }
func (m mockFileInfo) Sys() any           { return nil }

func TestSFTPSource_SortEntries_ByName(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{SortBy: "name"}, testLogger())
	entries := []os.FileInfo{
		mockFileInfo{name: "c.txt"},
		mockFileInfo{name: "a.txt"},
		mockFileInfo{name: "b.txt"},
	}
	s.sortEntries(entries)
	if entries[0].Name() != "a.txt" || entries[1].Name() != "b.txt" || entries[2].Name() != "c.txt" {
		t.Fatalf("expected sorted by name a,b,c got %s,%s,%s", entries[0].Name(), entries[1].Name(), entries[2].Name())
	}
}

func TestSFTPSource_SortEntries_ByModified(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{SortBy: "modified"}, testLogger())
	t3 := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	entries := []os.FileInfo{
		mockFileInfo{name: "c", modTime: t3},
		mockFileInfo{name: "a", modTime: t1},
		mockFileInfo{name: "b", modTime: t2},
	}
	s.sortEntries(entries)
	if entries[0].Name() != "a" || entries[1].Name() != "b" || entries[2].Name() != "c" {
		t.Fatalf("sort by modified wrong order: %s,%s,%s", entries[0].Name(), entries[1].Name(), entries[2].Name())
	}
}

func TestSFTPSource_SortEntries_BySize(t *testing.T) {
	s := NewSFTPSource(&config.SFTPListener{SortBy: "size"}, testLogger())
	entries := []os.FileInfo{
		mockFileInfo{name: "big", size: 1000},
		mockFileInfo{name: "small", size: 10},
		mockFileInfo{name: "medium", size: 500},
	}
	s.sortEntries(entries)
	if entries[0].Name() != "small" || entries[1].Name() != "medium" || entries[2].Name() != "big" {
		t.Fatalf("sort by size wrong order: %s,%s,%s", entries[0].Name(), entries[1].Name(), entries[2].Name())
	}
}

// ===================================================================
// classifyPipelineError, writeOperationOutcome, soapFaultResponse
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
// FHIRDest — extractResourceType, determineOperation, buildURL, httpMethod
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

func TestFHIRDest_DetermineOperation_FromMetadata(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://x", Operations: []string{"create"}}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	msg.Metadata["fhir_operation"] = "delete"
	got := dest.determineOperation(msg)
	if got != "delete" {
		t.Fatalf("expected delete, got %q", got)
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

func TestFHIRDest_BuildURL_Create_WithResource(t *testing.T) {
	dest := NewFHIRDest("t", &config.FHIRDestMapConfig{BaseURL: "http://fhir.test/r4"}, testLogger())
	msg := message.New("ch", []byte(`{}`))
	got := dest.buildURL("Patient", "create", msg)
	if got != "http://fhir.test/r4/Patient" {
		t.Fatalf("got %q", got)
	}
}

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

// ===================================================================
// FHIRPollSource — buildQueries, emitFromResponse
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
}

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

// ===================================================================
// pathRouter
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

// ===================================================================
// authenticateHTTP — additional edge-case tests
// ===================================================================

func TestAuthenticateHTTP_EmptyType(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	if !authenticateHTTP(req, &config.AuthConfig{Type: ""}) {
		t.Fatal("empty type should allow all")
	}
}

func TestAuthenticateHTTP_APIKeyNoHeaderNoQueryParam(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	if authenticateHTTP(req, &config.AuthConfig{Type: "api_key", Key: "k"}) {
		t.Fatal("api_key with no header and no query_param should fail")
	}
}

func TestAuthenticateHTTP_APIKeyWrongHeaderValue(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://localhost/", nil)
	req.Header.Set("X-Key", "wrong")
	if authenticateHTTP(req, &config.AuthConfig{Type: "api_key", Key: "correct", Header: "X-Key"}) {
		t.Fatal("wrong API key value should fail")
	}
}
