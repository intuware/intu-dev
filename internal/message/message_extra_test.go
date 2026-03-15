package message

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

func TestNew_FieldsPopulated(t *testing.T) {
	before := time.Now()
	msg := New("ch-42", []byte("payload"))
	after := time.Now()

	if msg.ID == "" {
		t.Fatal("expected non-empty ID")
	}
	if msg.CorrelationID != msg.ID {
		t.Fatalf("expected CorrelationID == ID, got %q vs %q", msg.CorrelationID, msg.ID)
	}
	if msg.ChannelID != "ch-42" {
		t.Fatalf("expected ChannelID=ch-42, got %s", msg.ChannelID)
	}
	if string(msg.Raw) != "payload" {
		t.Fatalf("expected raw=payload, got %s", string(msg.Raw))
	}
	if msg.ContentType != ContentTypeRaw {
		t.Fatalf("expected ContentType=raw, got %s", msg.ContentType)
	}
	if msg.Metadata == nil {
		t.Fatal("expected non-nil Metadata map")
	}
	if msg.Timestamp.Before(before) || msg.Timestamp.After(after) {
		t.Fatalf("Timestamp %v not in expected range [%v, %v]", msg.Timestamp, before, after)
	}
}

func TestNew_UniqueIDs(t *testing.T) {
	m1 := New("ch", nil)
	m2 := New("ch", nil)
	if m1.ID == m2.ID {
		t.Fatal("expected unique IDs for separate New calls")
	}
}

func TestNew_NilRaw(t *testing.T) {
	msg := New("ch", nil)
	if msg.Raw != nil {
		t.Fatalf("expected nil Raw, got %v", msg.Raw)
	}
}

func TestEnsureHTTP_CreatesWhenNil(t *testing.T) {
	msg := New("ch", nil)
	if msg.HTTP != nil {
		t.Fatal("expected nil HTTP initially")
	}
	http := msg.EnsureHTTP()
	if http == nil {
		t.Fatal("expected non-nil HTTP after EnsureHTTP")
	}
	if http.Headers == nil || http.QueryParams == nil || http.PathParams == nil {
		t.Fatal("expected initialized sub-maps")
	}
}

func TestEnsureHTTP_RetainsExisting(t *testing.T) {
	msg := New("ch", nil)
	msg.HTTP = &HTTPMeta{
		Headers: map[string]string{"X": "1"},
		Method:  "GET",
	}
	http := msg.EnsureHTTP()
	if http.Headers["X"] != "1" {
		t.Fatal("expected existing header preserved")
	}
	if http.Method != "GET" {
		t.Fatal("expected existing method preserved")
	}
}

func TestEnsureHTTP_Idempotent(t *testing.T) {
	msg := New("ch", nil)
	h1 := msg.EnsureHTTP()
	h1.Headers["key"] = "val"
	h2 := msg.EnsureHTTP()
	if h2.Headers["key"] != "val" {
		t.Fatal("expected same object returned on second call")
	}
}

func TestClearTransportMeta_AlreadyNil(t *testing.T) {
	msg := New("ch", []byte("data"))
	msg.ClearTransportMeta()
	if msg.HTTP != nil || msg.File != nil || msg.FTP != nil ||
		msg.Kafka != nil || msg.TCP != nil || msg.SMTP != nil ||
		msg.DICOM != nil || msg.Database != nil {
		t.Fatal("all transport fields should remain nil")
	}
}

func TestCloneWithRaw_IndependentRaw(t *testing.T) {
	msg := New("ch", []byte("orig"))
	msg.Metadata["k"] = "v"
	clone := msg.CloneWithRaw([]byte("new"))

	if string(clone.Raw) != "new" {
		t.Fatalf("expected clone raw=new, got %s", string(clone.Raw))
	}
	if string(msg.Raw) != "orig" {
		t.Fatal("original should be unchanged")
	}
	if clone.ID != msg.ID {
		t.Fatal("clone should share ID")
	}
	if clone.ChannelID != msg.ChannelID {
		t.Fatal("clone should share ChannelID")
	}
}

func TestCloneWithRaw_NilRaw(t *testing.T) {
	msg := New("ch", []byte("orig"))
	clone := msg.CloneWithRaw(nil)
	if clone.Raw != nil {
		t.Fatal("expected nil Raw in clone")
	}
}

func TestToIntuJSON_BinaryBody(t *testing.T) {
	binary := []byte{0x00, 0x01, 0xFF, 0xFE, 0x80}
	msg := New("ch", binary)

	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}

	var im map[string]any
	if err := json.Unmarshal(data, &im); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	bodyMap, ok := im["body"].(map[string]any)
	if !ok {
		t.Fatal("expected body to be a map for binary data")
	}
	encoded, ok := bodyMap["base64"].(string)
	if !ok {
		t.Fatal("expected base64 key in body map")
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("base64 decode failed: %v", err)
	}
	if string(decoded) != string(binary) {
		t.Fatal("decoded bytes do not match original")
	}
	if bodyMap["size"].(float64) != float64(len(binary)) {
		t.Fatalf("expected size=%d, got %v", len(binary), bodyMap["size"])
	}
}

func TestToIntuJSON_FTPMeta(t *testing.T) {
	msg := New("ch", []byte("ftp data"))
	msg.Transport = "ftp"
	msg.FTP = &FTPMeta{Filename: "data.csv", Directory: "/uploads"}

	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}
	var im map[string]any
	json.Unmarshal(data, &im)

	ftpMeta, ok := im["ftp"].(map[string]any)
	if !ok {
		t.Fatal("expected ftp metadata")
	}
	if ftpMeta["filename"] != "data.csv" {
		t.Fatalf("expected filename=data.csv, got %v", ftpMeta["filename"])
	}
	if ftpMeta["directory"] != "/uploads" {
		t.Fatalf("expected directory=/uploads, got %v", ftpMeta["directory"])
	}
}

func TestToIntuJSON_DatabaseMeta(t *testing.T) {
	msg := New("ch", []byte("db"))
	msg.Database = &DatabaseMeta{
		Query:  "INSERT INTO t VALUES(?)",
		Params: map[string]any{"id": float64(1)},
	}

	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}
	var im map[string]any
	json.Unmarshal(data, &im)

	dbMeta, ok := im["database"].(map[string]any)
	if !ok {
		t.Fatal("expected database metadata")
	}
	if dbMeta["query"] != "INSERT INTO t VALUES(?)" {
		t.Fatalf("unexpected query: %v", dbMeta["query"])
	}
}

func TestToIntuJSON_SMTPMeta(t *testing.T) {
	msg := New("ch", []byte("mail"))
	msg.SMTP = &SMTPMeta{
		From:    "a@b.com",
		To:      []string{"c@d.com", "e@f.com"},
		Subject: "Test",
		CC:      []string{"g@h.com"},
		BCC:     []string{"i@j.com"},
	}

	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}
	var im map[string]any
	json.Unmarshal(data, &im)

	smtpMeta, ok := im["smtp"].(map[string]any)
	if !ok {
		t.Fatal("expected smtp metadata")
	}
	if smtpMeta["from"] != "a@b.com" {
		t.Fatalf("expected from=a@b.com, got %v", smtpMeta["from"])
	}
	if smtpMeta["subject"] != "Test" {
		t.Fatalf("expected subject=Test, got %v", smtpMeta["subject"])
	}
	to, ok := smtpMeta["to"].([]any)
	if !ok || len(to) != 2 {
		t.Fatalf("expected to with 2 entries, got %v", smtpMeta["to"])
	}
}

func TestToIntuJSON_DICOMMeta(t *testing.T) {
	msg := New("ch", []byte("dcm"))
	msg.DICOM = &DICOMMeta{CallingAE: "SCU", CalledAE: "SCP"}

	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}
	var im map[string]any
	json.Unmarshal(data, &im)

	dicomMeta, ok := im["dicom"].(map[string]any)
	if !ok {
		t.Fatal("expected dicom metadata")
	}
	if dicomMeta["callingAE"] != "SCU" || dicomMeta["calledAE"] != "SCP" {
		t.Fatalf("unexpected dicom values: %v", dicomMeta)
	}
}

func TestToIntuJSON_KafkaMeta(t *testing.T) {
	msg := New("ch", []byte("k"))
	msg.Kafka = &KafkaMeta{
		Headers:   map[string]string{"h": "v"},
		Topic:     "t1",
		Key:       "k1",
		Partition: 2,
		Offset:    100,
	}

	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}
	var im map[string]any
	json.Unmarshal(data, &im)

	kafkaMeta, ok := im["kafka"].(map[string]any)
	if !ok {
		t.Fatal("expected kafka metadata")
	}
	if kafkaMeta["topic"] != "t1" {
		t.Fatalf("expected topic=t1, got %v", kafkaMeta["topic"])
	}
	if kafkaMeta["key"] != "k1" {
		t.Fatalf("expected key=k1, got %v", kafkaMeta["key"])
	}
	if kafkaMeta["partition"].(float64) != 2 {
		t.Fatalf("expected partition=2, got %v", kafkaMeta["partition"])
	}
	if kafkaMeta["offset"].(float64) != 100 {
		t.Fatalf("expected offset=100, got %v", kafkaMeta["offset"])
	}
	headers := kafkaMeta["headers"].(map[string]any)
	if headers["h"] != "v" {
		t.Fatalf("expected kafka header h=v, got %v", headers["h"])
	}
}

func TestFromIntuJSON_BinaryBody(t *testing.T) {
	binary := []byte{0x00, 0x01, 0xFF, 0xFE, 0x80}
	msg := New("ch", binary)
	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}

	restored, err := FromIntuJSON(data, "ch")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if string(restored.Raw) != string(binary) {
		t.Fatalf("binary roundtrip failed: got %v, want %v", restored.Raw, binary)
	}
}

func TestFromIntuJSON_FTPMeta(t *testing.T) {
	original := New("ch", []byte("ftp"))
	original.FTP = &FTPMeta{Filename: "a.txt", Directory: "/dir"}
	data, _ := original.ToIntuJSON()

	restored, err := FromIntuJSON(data, "ch")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if restored.FTP == nil {
		t.Fatal("expected FTP metadata")
	}
	if restored.FTP.Filename != "a.txt" || restored.FTP.Directory != "/dir" {
		t.Fatalf("unexpected FTP: %+v", restored.FTP)
	}
}

func TestFromIntuJSON_TCPMeta(t *testing.T) {
	original := New("ch", []byte("tcp"))
	original.TCP = &TCPMeta{RemoteAddr: "10.0.0.1:8080"}
	data, _ := original.ToIntuJSON()

	restored, err := FromIntuJSON(data, "ch")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if restored.TCP == nil || restored.TCP.RemoteAddr != "10.0.0.1:8080" {
		t.Fatalf("unexpected TCP: %+v", restored.TCP)
	}
}

func TestFromIntuJSON_AllTransportMeta(t *testing.T) {
	original := New("ch", []byte("all"))
	original.HTTP = &HTTPMeta{
		Headers: map[string]string{"H": "V"}, QueryParams: map[string]string{"q": "1"},
		PathParams: map[string]string{"p": "2"}, Method: "PUT", StatusCode: 200,
	}
	original.File = &FileMeta{Filename: "f.txt", Directory: "/d"}
	original.FTP = &FTPMeta{Filename: "ftp.txt", Directory: "/ftp"}
	original.Kafka = &KafkaMeta{Headers: map[string]string{"kh": "kv"}, Topic: "t", Key: "k", Partition: 1, Offset: 2}
	original.TCP = &TCPMeta{RemoteAddr: "1.2.3.4:80"}
	original.SMTP = &SMTPMeta{From: "f", To: []string{"t1", "t2"}, Subject: "s", CC: []string{"c"}, BCC: []string{"b"}}
	original.DICOM = &DICOMMeta{CallingAE: "A", CalledAE: "B"}
	original.Database = &DatabaseMeta{Query: "Q", Params: map[string]any{"x": float64(1)}}

	data, err := original.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}

	restored, err := FromIntuJSON(data, "ch")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}

	if restored.HTTP == nil || restored.HTTP.Method != "PUT" || restored.HTTP.StatusCode != 200 {
		t.Fatalf("HTTP roundtrip failed: %+v", restored.HTTP)
	}
	if restored.HTTP.Headers["H"] != "V" || restored.HTTP.QueryParams["q"] != "1" || restored.HTTP.PathParams["p"] != "2" {
		t.Fatal("HTTP sub-maps roundtrip failed")
	}
	if restored.File == nil || restored.File.Filename != "f.txt" {
		t.Fatal("File roundtrip failed")
	}
	if restored.FTP == nil || restored.FTP.Filename != "ftp.txt" {
		t.Fatal("FTP roundtrip failed")
	}
	if restored.Kafka == nil || restored.Kafka.Topic != "t" || restored.Kafka.Key != "k" || restored.Kafka.Partition != 1 || restored.Kafka.Offset != 2 {
		t.Fatal("Kafka roundtrip failed")
	}
	if restored.Kafka.Headers["kh"] != "kv" {
		t.Fatal("Kafka headers roundtrip failed")
	}
	if restored.TCP == nil || restored.TCP.RemoteAddr != "1.2.3.4:80" {
		t.Fatal("TCP roundtrip failed")
	}
	if restored.SMTP == nil || restored.SMTP.From != "f" || len(restored.SMTP.To) != 2 || restored.SMTP.Subject != "s" {
		t.Fatal("SMTP roundtrip failed")
	}
	if len(restored.SMTP.CC) != 1 || len(restored.SMTP.BCC) != 1 {
		t.Fatal("SMTP CC/BCC roundtrip failed")
	}
	if restored.DICOM == nil || restored.DICOM.CallingAE != "A" || restored.DICOM.CalledAE != "B" {
		t.Fatal("DICOM roundtrip failed")
	}
	if restored.Database == nil || restored.Database.Query != "Q" {
		t.Fatal("Database roundtrip failed")
	}
}

func TestFromIntuJSON_OverridesChannelID(t *testing.T) {
	data := `{"body":"test","channelId":"original-ch"}`
	restored, err := FromIntuJSON([]byte(data), "override-ch")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if restored.ChannelID != "original-ch" {
		t.Fatalf("expected ChannelID from JSON, got %s", restored.ChannelID)
	}
}

func TestFromIntuJSON_EmptyBody(t *testing.T) {
	data := `{"body":"","transport":"http"}`
	restored, err := FromIntuJSON([]byte(data), "ch")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if len(restored.Raw) != 0 {
		t.Fatalf("expected empty raw, got %q", string(restored.Raw))
	}
}

func TestFromIntuJSON_NoBody(t *testing.T) {
	data := `{"transport":"http"}`
	restored, err := FromIntuJSON([]byte(data), "ch")
	if err != nil {
		t.Fatalf("FromIntuJSON failed: %v", err)
	}
	if restored.Raw != nil {
		t.Fatalf("expected nil raw when no body, got %v", restored.Raw)
	}
}

func TestRebuild_WithValidIntuJSON(t *testing.T) {
	orig := New("ch", []byte("hello"))
	orig.Transport = "tcp"
	orig.TCP = &TCPMeta{RemoteAddr: "5.5.5.5:22"}
	data, _ := orig.ToIntuJSON()

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rebuilt := Rebuild("id-x", "corr-x", "ch", data, ts)

	if rebuilt.CorrelationID != "corr-x" {
		t.Fatalf("expected CorrelationID=corr-x, got %s", rebuilt.CorrelationID)
	}
	if rebuilt.Metadata["reprocessed"] != true {
		t.Fatal("expected reprocessed=true")
	}
	if rebuilt.Metadata["original_message_id"] != "id-x" {
		t.Fatalf("expected original_message_id=id-x, got %v", rebuilt.Metadata["original_message_id"])
	}
	if rebuilt.Metadata["original_timestamp"] != ts.Format(time.RFC3339) {
		t.Fatalf("expected original_timestamp, got %v", rebuilt.Metadata["original_timestamp"])
	}
}

func TestRebuild_EmptyCorrelationIDFallsBackToID(t *testing.T) {
	msg := Rebuild("fallback-id", "", "ch", []byte("not json"), time.Now())
	if msg.CorrelationID != "fallback-id" {
		t.Fatalf("expected CorrelationID=fallback-id, got %s", msg.CorrelationID)
	}
}

func TestResponseToIntuJSON_BinaryBody(t *testing.T) {
	binary := []byte{0x00, 0xFF, 0xAB}
	resp := &Response{StatusCode: 200, Body: binary, Headers: nil}

	data, err := ResponseToIntuJSON(resp)
	if err != nil {
		t.Fatalf("ResponseToIntuJSON failed: %v", err)
	}

	var im map[string]any
	json.Unmarshal(data, &im)

	bodyMap, ok := im["body"].(map[string]any)
	if !ok {
		t.Fatal("expected body map for binary response")
	}
	if _, ok := bodyMap["base64"].(string); !ok {
		t.Fatal("expected base64 encoded body")
	}
}

func TestResponseToIntuJSON_NilHeaders(t *testing.T) {
	resp := &Response{StatusCode: 200, Body: []byte("ok"), Headers: nil}
	data, err := ResponseToIntuJSON(resp)
	if err != nil {
		t.Fatalf("ResponseToIntuJSON failed: %v", err)
	}

	var im map[string]any
	json.Unmarshal(data, &im)

	httpMeta := im["http"].(map[string]any)
	headers := httpMeta["headers"].(map[string]any)
	if len(headers) != 0 {
		t.Fatalf("expected empty headers map, got %v", headers)
	}
}

func TestMarshalRaw_NoHTMLEscape(t *testing.T) {
	input := map[string]any{"body": "<html>&test</html>"}
	data, err := marshalRaw(input)
	if err != nil {
		t.Fatalf("marshalRaw failed: %v", err)
	}
	s := string(data)
	if s == "" {
		t.Fatal("expected non-empty output")
	}
	// HTML chars should NOT be escaped
	if json.Valid(data) == false {
		t.Fatal("expected valid JSON")
	}
	var decoded map[string]any
	json.Unmarshal(data, &decoded)
	if decoded["body"] != "<html>&test</html>" {
		t.Fatalf("expected unescaped HTML in body, got %v", decoded["body"])
	}
}

func TestEnsureMap_NilInput(t *testing.T) {
	result := ensureMap(nil)
	if result == nil || len(result) != 0 {
		t.Fatal("expected empty map for nil input")
	}
}

func TestEnsureMap_NonNilInput(t *testing.T) {
	m := map[string]string{"a": "1"}
	result := ensureMap(m)
	if result["a"] != "1" {
		t.Fatal("expected same map returned")
	}
}

func TestToStringMap_NonStringValues(t *testing.T) {
	m := map[string]any{
		"str": "hello",
		"num": float64(42),
		"bl":  true,
	}
	result := toStringMap(m)
	if result["str"] != "hello" {
		t.Fatalf("expected str=hello, got %s", result["str"])
	}
	if result["num"] != "42" {
		t.Fatalf("expected num=42, got %s", result["num"])
	}
	if result["bl"] != "true" {
		t.Fatalf("expected bl=true, got %s", result["bl"])
	}
}

func TestToStringMap_NotAMap(t *testing.T) {
	result := toStringMap("not a map")
	if len(result) != 0 {
		t.Fatal("expected empty map for non-map input")
	}
}

func TestToStringMap_Nil(t *testing.T) {
	result := toStringMap(nil)
	if result == nil || len(result) != 0 {
		t.Fatal("expected empty map for nil input")
	}
}

func TestToStringSlice_ValidSlice(t *testing.T) {
	arr := []any{"a", "b", "c"}
	result := toStringSlice(arr)
	if len(result) != 3 || result[0] != "a" || result[2] != "c" {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestToStringSlice_MixedTypes(t *testing.T) {
	arr := []any{"a", float64(1), "b"}
	result := toStringSlice(arr)
	if len(result) != 2 || result[0] != "a" || result[1] != "b" {
		t.Fatalf("expected only string elements, got %v", result)
	}
}

func TestToStringSlice_NotSlice(t *testing.T) {
	result := toStringSlice("not a slice")
	if result != nil {
		t.Fatal("expected nil for non-slice input")
	}
}

func TestToStringSlice_Nil(t *testing.T) {
	result := toStringSlice(nil)
	if result != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestParseHTTPMetaMap_MissingFields(t *testing.T) {
	data := map[string]any{}
	meta := parseHTTPMetaMap(data)
	if meta.Method != "" {
		t.Fatalf("expected empty method, got %s", meta.Method)
	}
	if meta.StatusCode != 0 {
		t.Fatalf("expected 0 statusCode, got %d", meta.StatusCode)
	}
	if len(meta.Headers) != 0 {
		t.Fatal("expected empty headers")
	}
}

func TestParseFileMetaMap_EmptyData(t *testing.T) {
	meta := parseFileMetaMap(map[string]any{})
	if meta.Filename != "" || meta.Directory != "" {
		t.Fatalf("expected empty FileMeta, got %+v", meta)
	}
}

func TestParseFTPMetaMap_EmptyData(t *testing.T) {
	meta := parseFTPMetaMap(map[string]any{})
	if meta.Filename != "" || meta.Directory != "" {
		t.Fatalf("expected empty FTPMeta, got %+v", meta)
	}
}

func TestParseKafkaMetaMap_EmptyData(t *testing.T) {
	meta := parseKafkaMetaMap(map[string]any{})
	if meta.Topic != "" || meta.Key != "" || meta.Partition != 0 || meta.Offset != 0 {
		t.Fatalf("expected zero-value KafkaMeta, got %+v", meta)
	}
}

func TestParseTCPMetaMap_EmptyData(t *testing.T) {
	meta := parseTCPMetaMap(map[string]any{})
	if meta.RemoteAddr != "" {
		t.Fatalf("expected empty RemoteAddr, got %s", meta.RemoteAddr)
	}
}

func TestParseSMTPMetaMap_EmptyData(t *testing.T) {
	meta := parseSMTPMetaMap(map[string]any{})
	if meta.From != "" || meta.Subject != "" {
		t.Fatalf("expected empty SMTPMeta, got %+v", meta)
	}
	if meta.To != nil || meta.CC != nil || meta.BCC != nil {
		t.Fatal("expected nil slices for empty SMTP data")
	}
}

func TestParseDICOMMetaMap_EmptyData(t *testing.T) {
	meta := parseDICOMMetaMap(map[string]any{})
	if meta.CallingAE != "" || meta.CalledAE != "" {
		t.Fatalf("expected empty DICOMMeta, got %+v", meta)
	}
}

func TestParseDatabaseMetaMap_EmptyData(t *testing.T) {
	meta := parseDatabaseMetaMap(map[string]any{})
	if meta.Query != "" || meta.Params != nil {
		t.Fatalf("expected empty DatabaseMeta, got %+v", meta)
	}
}

func TestParseDatabaseMetaMap_WithParams(t *testing.T) {
	data := map[string]any{
		"query":  "SELECT 1",
		"params": map[string]any{"a": float64(1), "b": "two"},
	}
	meta := parseDatabaseMetaMap(data)
	if meta.Query != "SELECT 1" {
		t.Fatalf("expected query, got %s", meta.Query)
	}
	if meta.Params["a"] != float64(1) || meta.Params["b"] != "two" {
		t.Fatalf("unexpected params: %v", meta.Params)
	}
}

func TestToIntuJSON_EmptyRaw(t *testing.T) {
	msg := New("ch", []byte(""))
	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}
	var im map[string]any
	json.Unmarshal(data, &im)
	if im["body"] != "" {
		t.Fatalf("expected empty string body, got %v", im["body"])
	}
}

func TestToIntuJSON_NilHTTPHeaders(t *testing.T) {
	msg := New("ch", []byte("x"))
	msg.HTTP = &HTTPMeta{
		Headers: nil,
		Method:  "GET",
	}

	data, err := msg.ToIntuJSON()
	if err != nil {
		t.Fatalf("ToIntuJSON failed: %v", err)
	}
	var im map[string]any
	json.Unmarshal(data, &im)

	httpMeta := im["http"].(map[string]any)
	headers := httpMeta["headers"].(map[string]any)
	if len(headers) != 0 {
		t.Fatalf("expected empty headers map for nil headers, got %v", headers)
	}
}
