package message

import (
	"encoding/json"
	"testing"
	"time"
)

func TestToIntuJSON_HTTPMessage(t *testing.T) {
	msg := New("ch-1", []byte("hello world"))
	msg.Transport = "http"
	msg.ContentType = ContentTypeJSON
	msg.HTTP = &HTTPMeta{
		Headers:     map[string]string{"Content-Type": "application/json", "X-Custom": "val"},
		QueryParams: map[string]string{"patient": "123"},
		PathParams:  map[string]string{"id": "abc"},
		Method:      "POST",
		StatusCode:  0,
	}

	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}

	var im map[string]any
	if err := json.Unmarshal(data, &im); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if im["body"] != "hello world" {
		t.Fatalf("expected body='hello world', got %v", im["body"])
	}
	if im["transport"] != "http" {
		t.Fatalf("expected transport=http, got %v", im["transport"])
	}
	if im["contentType"] != "json" {
		t.Fatalf("expected contentType=json, got %v", im["contentType"])
	}

	httpMeta, ok := im["http"].(map[string]any)
	if !ok {
		t.Fatal("expected http metadata")
	}
	headers, ok := httpMeta["headers"].(map[string]any)
	if !ok {
		t.Fatal("expected http.headers")
	}
	if headers["Content-Type"] != "application/json" {
		t.Fatalf("expected Content-Type header, got %v", headers["Content-Type"])
	}
	if headers["X-Custom"] != "val" {
		t.Fatalf("expected X-Custom header, got %v", headers["X-Custom"])
	}
	qp, ok := httpMeta["queryParams"].(map[string]any)
	if !ok {
		t.Fatal("expected queryParams")
	}
	if qp["patient"] != "123" {
		t.Fatalf("expected patient=123, got %v", qp["patient"])
	}
	if httpMeta["method"] != "POST" {
		t.Fatalf("expected method=POST, got %v", httpMeta["method"])
	}
}

func TestToIntuJSON_MinimalMessage(t *testing.T) {
	msg := New("ch-1", []byte("raw data"))
	msg.Transport = "tcp"
	msg.TCP = &TCPMeta{RemoteAddr: "127.0.0.1:5000"}

	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}

	var im map[string]any
	json.Unmarshal(data, &im)

	if im["transport"] != "tcp" {
		t.Fatalf("expected transport=tcp, got %v", im["transport"])
	}
	if im["http"] != nil {
		t.Fatal("did not expect http metadata for TCP message")
	}
	tcpMeta, ok := im["tcp"].(map[string]any)
	if !ok {
		t.Fatal("expected tcp metadata")
	}
	if tcpMeta["remoteAddr"] != "127.0.0.1:5000" {
		t.Fatalf("expected remoteAddr, got %v", tcpMeta["remoteAddr"])
	}
}

func TestFromIntuJSON_Roundtrip(t *testing.T) {
	original := New("ch-1", []byte("test payload"))
	original.Transport = "http"
	original.ContentType = ContentTypeHL7v2
	original.HTTP = &HTTPMeta{
		Headers:     map[string]string{"Authorization": "Bearer token"},
		QueryParams: map[string]string{"format": "hl7"},
		PathParams:  map[string]string{},
		Method:      "POST",
		StatusCode:  0,
	}

	data, err := original.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}

	restored, err := FromIntuJSON(data, "ch-1")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}

	if string(restored.Raw) != "test payload" {
		t.Fatalf("expected raw='test payload', got %s", string(restored.Raw))
	}
	if restored.Transport != "http" {
		t.Fatalf("expected transport=http, got %s", restored.Transport)
	}
	if restored.ContentType != ContentTypeHL7v2 {
		t.Fatalf("expected contentType=hl7v2, got %s", restored.ContentType)
	}
	if restored.HTTP == nil {
		t.Fatal("expected HTTP metadata")
	}
	if restored.HTTP.Headers["Authorization"] != "Bearer token" {
		t.Fatalf("expected Authorization header, got %v", restored.HTTP.Headers)
	}
	if restored.HTTP.QueryParams["format"] != "hl7" {
		t.Fatalf("expected format query param, got %v", restored.HTTP.QueryParams)
	}
	if restored.HTTP.Method != "POST" {
		t.Fatalf("expected POST method, got %s", restored.HTTP.Method)
	}
}

func TestFromIntuJSON_KafkaMeta(t *testing.T) {
	original := New("ch-1", []byte("kafka msg"))
	original.Transport = "kafka"
	original.Kafka = &KafkaMeta{
		Headers:   map[string]string{"key": "val"},
		Topic:     "orders",
		Key:       "order-1",
		Partition: 3,
		Offset:    42,
	}

	data, _ := original.ToIntuJSON()
	restored, err := FromIntuJSON(data, "ch-1")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if restored.Kafka == nil {
		t.Fatal("expected Kafka metadata")
	}
	if restored.Kafka.Topic != "orders" {
		t.Fatalf("expected topic=orders, got %s", restored.Kafka.Topic)
	}
	if restored.Kafka.Partition != 3 {
		t.Fatalf("expected partition=3, got %d", restored.Kafka.Partition)
	}
	if restored.Kafka.Offset != 42 {
		t.Fatalf("expected offset=42, got %d", restored.Kafka.Offset)
	}
}

func TestFromIntuJSON_FileMeta(t *testing.T) {
	original := New("ch-1", []byte("file content"))
	original.Transport = "file"
	original.File = &FileMeta{Filename: "test.hl7", Directory: "/data/incoming"}

	data, _ := original.ToIntuJSON()
	restored, err := FromIntuJSON(data, "ch-1")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if restored.File == nil {
		t.Fatal("expected File metadata")
	}
	if restored.File.Filename != "test.hl7" {
		t.Fatalf("expected filename=test.hl7, got %s", restored.File.Filename)
	}
}

func TestFromIntuJSON_InvalidJSON(t *testing.T) {
	_, err := FromIntuJSON([]byte("not json"), "ch-1")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestFromIntuJSON_BackwardCompatRawPayload(t *testing.T) {
	_, err := FromIntuJSON([]byte("plain hl7 payload"), "ch-1")
	if err == nil {
		t.Fatal("expected error for non-JSON content (backward compat handled by caller)")
	}
}

func TestResponseToIntuJSON(t *testing.T) {
	resp := &Response{
		StatusCode: 201,
		Body:       []byte(`{"id": "created"}`),
		Headers:    map[string]string{"Content-Type": "application/json", "X-Request-Id": "abc"},
	}

	data, err := ResponseToIntuJSON(resp)
	if err != nil {
		t.Fatalf("ResponseToIntuJSON failed: %v", err)
	}

	var im map[string]any
	json.Unmarshal(data, &im)

	if im["body"] != `{"id": "created"}` {
		t.Fatalf("unexpected body: %v", im["body"])
	}
	if im["transport"] != "http" {
		t.Fatalf("expected transport=http, got %v", im["transport"])
	}

	httpMeta, ok := im["http"].(map[string]any)
	if !ok {
		t.Fatal("expected http metadata")
	}
	if httpMeta["statusCode"].(float64) != 201 {
		t.Fatalf("expected statusCode=201, got %v", httpMeta["statusCode"])
	}
	headers := httpMeta["headers"].(map[string]any)
	if headers["Content-Type"] != "application/json" {
		t.Fatalf("expected Content-Type header, got %v", headers["Content-Type"])
	}
}

func TestResponseToIntuJSON_NilResponse(t *testing.T) {
	data, err := ResponseToIntuJSON(nil)
	if err != nil {
		t.Fatalf("ResponseToIntuJSON(nil) failed: %v", err)
	}

	var im map[string]any
	json.Unmarshal(data, &im)
	if im["body"] != "" {
		t.Fatalf("expected empty body for nil response, got %v", im["body"])
	}
}

func TestResponseToIntuJSON_EmptyResponse(t *testing.T) {
	resp := &Response{
		StatusCode: 204,
		Body:       nil,
		Headers:    nil,
	}

	data, err := ResponseToIntuJSON(resp)
	if err != nil {
		t.Fatalf("ResponseToIntuJSON failed: %v", err)
	}

	var im map[string]any
	json.Unmarshal(data, &im)
	if im["body"] != "" {
		t.Fatalf("expected empty body, got %v", im["body"])
	}
}

func TestCloneWithRaw(t *testing.T) {
	msg := New("ch-1", []byte("original"))
	msg.Transport = "http"
	msg.HTTP = &HTTPMeta{Headers: map[string]string{"X": "1"}}

	clone := msg.CloneWithRaw([]byte("new payload"))

	if string(clone.Raw) != "new payload" {
		t.Fatalf("expected clone raw='new payload', got %s", string(clone.Raw))
	}
	if string(msg.Raw) != "original" {
		t.Fatal("original message should not be modified")
	}
	if clone.Transport != "http" {
		t.Fatal("clone should preserve transport")
	}
	if clone.HTTP == nil || clone.HTTP.Headers["X"] != "1" {
		t.Fatal("clone should preserve HTTP metadata")
	}
}

func TestClearTransportMeta(t *testing.T) {
	msg := New("ch-1", []byte("test"))
	msg.Transport = "sftp"
	msg.HTTP = &HTTPMeta{Headers: map[string]string{"X": "1"}}
	msg.File = &FileMeta{Filename: "f.txt"}
	msg.FTP = &FTPMeta{Filename: "f.txt"}
	msg.Kafka = &KafkaMeta{Topic: "t"}
	msg.TCP = &TCPMeta{RemoteAddr: "1.2.3.4:80"}
	msg.SMTP = &SMTPMeta{From: "a@b.com"}
	msg.DICOM = &DICOMMeta{CallingAE: "SCU"}
	msg.Database = &DatabaseMeta{Query: "SELECT 1"}

	msg.ClearTransportMeta()

	if msg.HTTP != nil || msg.File != nil || msg.FTP != nil ||
		msg.Kafka != nil || msg.TCP != nil || msg.SMTP != nil ||
		msg.DICOM != nil || msg.Database != nil {
		t.Fatal("ClearTransportMeta should nil all transport meta fields")
	}
	if msg.Transport != "sftp" {
		t.Fatal("ClearTransportMeta should not change the Transport string")
	}
}

func TestFromIntuJSON_SMTPMeta(t *testing.T) {
	original := New("ch-1", []byte("email body"))
	original.Transport = "smtp"
	original.SMTP = &SMTPMeta{
		From:    "sender@example.com",
		To:      []string{"recipient@example.com"},
		Subject: "Test",
		CC:      []string{"cc@example.com"},
	}

	data, _ := original.ToIntuJSON()
	restored, err := FromIntuJSON(data, "ch-1")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if restored.SMTP == nil {
		t.Fatal("expected SMTP metadata")
	}
	if restored.SMTP.From != "sender@example.com" {
		t.Fatalf("expected from=sender@example.com, got %s", restored.SMTP.From)
	}
	if len(restored.SMTP.To) != 1 || restored.SMTP.To[0] != "recipient@example.com" {
		t.Fatalf("unexpected to: %v", restored.SMTP.To)
	}
}

func TestFromIntuJSON_DICOMMeta(t *testing.T) {
	original := New("ch-1", []byte("dicom data"))
	original.Transport = "dicom"
	original.DICOM = &DICOMMeta{CallingAE: "SCU", CalledAE: "SCP"}

	data, _ := original.ToIntuJSON()
	restored, err := FromIntuJSON(data, "ch-1")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if restored.DICOM == nil {
		t.Fatal("expected DICOM metadata")
	}
	if restored.DICOM.CallingAE != "SCU" {
		t.Fatalf("expected CallingAE=SCU, got %s", restored.DICOM.CallingAE)
	}
}

func TestFromIntuJSON_DatabaseMeta(t *testing.T) {
	original := New("ch-1", []byte("db row"))
	original.Transport = "database"
	original.Database = &DatabaseMeta{
		Query:  "SELECT * FROM patients",
		Params: map[string]any{"limit": float64(10)},
	}

	data, _ := original.ToIntuJSON()
	restored, err := FromIntuJSON(data, "ch-1")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if restored.Database == nil {
		t.Fatal("expected Database metadata")
	}
	if restored.Database.Query != "SELECT * FROM patients" {
		t.Fatalf("expected query, got %s", restored.Database.Query)
	}
}

func TestToIntuJSON_IncludesVersion(t *testing.T) {
	msg := New("ch-1", []byte("test"))
	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}
	var im map[string]any
	json.Unmarshal(data, &im)
	if im["version"] != IntuJSONVersion {
		t.Fatalf("expected version=%s, got %v", IntuJSONVersion, im["version"])
	}
}

func TestToIntuJSON_IncludesIdentityFields(t *testing.T) {
	msg := New("ch-1", []byte("test"))
	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}
	var im map[string]any
	json.Unmarshal(data, &im)
	if im["id"] != msg.ID {
		t.Fatalf("expected id=%s, got %v", msg.ID, im["id"])
	}
	if im["correlationId"] != msg.CorrelationID {
		t.Fatalf("expected correlationId=%s, got %v", msg.CorrelationID, im["correlationId"])
	}
	if im["channelId"] != "ch-1" {
		t.Fatalf("expected channelId=ch-1, got %v", im["channelId"])
	}
	if _, ok := im["timestamp"].(string); !ok || im["timestamp"] == "" {
		t.Fatal("expected non-empty timestamp string")
	}
}

func TestFromIntuJSON_RestoresIdentity(t *testing.T) {
	original := New("ch-1", []byte("test payload"))
	original.SourceCharset = "iso-8859-1"
	original.Metadata["filename"] = "test.hl7"
	original.Metadata["reprocessed"] = true

	data, err := original.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}

	restored, err := FromIntuJSON(data, "ch-1")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}

	if restored.ID != original.ID {
		t.Fatalf("expected ID=%s, got %s", original.ID, restored.ID)
	}
	if restored.CorrelationID != original.CorrelationID {
		t.Fatalf("expected CorrelationID=%s, got %s", original.CorrelationID, restored.CorrelationID)
	}
	if restored.ChannelID != "ch-1" {
		t.Fatalf("expected ChannelID=ch-1, got %s", restored.ChannelID)
	}
	if !restored.Timestamp.Equal(original.Timestamp) {
		t.Fatalf("expected Timestamp=%v, got %v", original.Timestamp, restored.Timestamp)
	}
	if restored.SourceCharset != "iso-8859-1" {
		t.Fatalf("expected SourceCharset=iso-8859-1, got %s", restored.SourceCharset)
	}
	if restored.Metadata["filename"] != "test.hl7" {
		t.Fatalf("expected metadata filename=test.hl7, got %v", restored.Metadata["filename"])
	}
	if restored.Metadata["reprocessed"] != true {
		t.Fatalf("expected metadata reprocessed=true, got %v", restored.Metadata["reprocessed"])
	}
}

func TestFromIntuJSON_BackwardCompatNoIdentity(t *testing.T) {
	legacyJSON := `{"body":"hello","transport":"http","contentType":"json"}`
	restored, err := FromIntuJSON([]byte(legacyJSON), "ch-1")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if restored.ID == "" {
		t.Fatal("expected non-empty ID for legacy envelope")
	}
	if restored.ChannelID != "ch-1" {
		t.Fatalf("expected ChannelID=ch-1, got %s", restored.ChannelID)
	}
	if string(restored.Raw) != "hello" {
		t.Fatalf("expected raw=hello, got %s", string(restored.Raw))
	}
}

func TestToIntuJSON_SourceCharsetAndMetadata(t *testing.T) {
	msg := New("ch-1", []byte("test"))
	msg.SourceCharset = "windows-1252"
	msg.Metadata["custom_key"] = "custom_val"

	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}
	var im map[string]any
	json.Unmarshal(data, &im)
	if im["sourceCharset"] != "windows-1252" {
		t.Fatalf("expected sourceCharset=windows-1252, got %v", im["sourceCharset"])
	}
	md, ok := im["metadata"].(map[string]any)
	if !ok {
		t.Fatal("expected metadata map")
	}
	if md["custom_key"] != "custom_val" {
		t.Fatalf("expected custom_key=custom_val, got %v", md["custom_key"])
	}
}

func TestToIntuJSON_OmitsEmptySourceCharsetAndMetadata(t *testing.T) {
	msg := New("ch-1", []byte("test"))

	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}
	var im map[string]any
	json.Unmarshal(data, &im)
	if _, exists := im["sourceCharset"]; exists {
		t.Fatal("sourceCharset should be omitted when empty")
	}
	if _, exists := im["metadata"]; exists {
		t.Fatal("metadata should be omitted when empty")
	}
}

func TestRebuild(t *testing.T) {
	original := New("ch-1", []byte("original payload"))
	original.Transport = "http"
	original.HTTP = &HTTPMeta{Method: "POST"}

	data, _ := original.ToIntuJSON()

	msg := Rebuild("orig-id", "orig-corr", "ch-1", data, original.Timestamp)
	if msg.CorrelationID != "orig-corr" {
		t.Fatalf("expected CorrelationID=orig-corr, got %s", msg.CorrelationID)
	}
	if msg.Metadata["reprocessed"] != true {
		t.Fatal("expected reprocessed metadata")
	}
	if msg.Metadata["original_message_id"] != "orig-id" {
		t.Fatalf("expected original_message_id=orig-id, got %v", msg.Metadata["original_message_id"])
	}
	if string(msg.Raw) != "original payload" {
		t.Fatalf("expected raw=original payload, got %s", string(msg.Raw))
	}
	if msg.Transport != "http" {
		t.Fatalf("expected transport=http, got %s", msg.Transport)
	}
}

func TestRebuild_RawFallback(t *testing.T) {
	msg := Rebuild("id-1", "", "ch-1", []byte("plain text, not JSON"), time.Now())
	if msg.CorrelationID != "id-1" {
		t.Fatalf("expected CorrelationID=id-1 (fallback), got %s", msg.CorrelationID)
	}
	if string(msg.Raw) != "plain text, not JSON" {
		t.Fatalf("expected raw fallback, got %s", string(msg.Raw))
	}
}
