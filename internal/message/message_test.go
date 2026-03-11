package message

import (
	"encoding/json"
	"testing"
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
