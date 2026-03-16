package message

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// marshalRaw encodes v as JSON without escaping HTML characters (<, >, &).
func marshalRaw(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	// Encode appends a trailing newline; strip it for consistency with json.Marshal.
	b := buf.Bytes()
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	return b, nil
}

type ContentType string

const (
	ContentTypeRaw       ContentType = "raw"
	ContentTypeJSON      ContentType = "json"
	ContentTypeXML       ContentType = "xml"
	ContentTypeCSV       ContentType = "csv"
	ContentTypeHL7v2     ContentType = "hl7v2"
	ContentTypeHL7v3     ContentType = "hl7v3"
	ContentTypeFHIR      ContentType = "fhir_r4"
	ContentTypeX12       ContentType = "x12"
	ContentTypeDICOM     ContentType = "dicom"
	ContentTypeDelimited ContentType = "delimited"
	ContentTypeBinary    ContentType = "binary"
	ContentTypeCCDA      ContentType = "ccda"
)

type Message struct {
	ID            string
	CorrelationID string
	ChannelID     string
	Raw           []byte
	Transport     string
	ContentType   ContentType
	SourceCharset string // e.g. "iso-8859-1", "utf-16le"; empty means UTF-8
	HTTP          *HTTPMeta
	File          *FileMeta
	FTP           *FTPMeta
	Kafka         *KafkaMeta
	TCP           *TCPMeta
	SMTP          *SMTPMeta
	DICOM         *DICOMMeta
	Database      *DatabaseMeta
	Metadata      map[string]any
	Timestamp     time.Time
}

type HTTPMeta struct {
	Headers     map[string]string
	QueryParams map[string]string
	PathParams  map[string]string
	Method      string
	StatusCode  int
}

type FileMeta struct {
	Filename  string
	Directory string
}

type FTPMeta struct {
	Filename  string
	Directory string
}

type KafkaMeta struct {
	Headers   map[string]string
	Topic     string
	Key       string
	Partition int
	Offset    int64
}

type TCPMeta struct {
	RemoteAddr string
}

type SMTPMeta struct {
	From    string
	To      []string
	Subject string
	CC      []string
	BCC     []string
}

type DICOMMeta struct {
	CallingAE string
	CalledAE  string
}

type DatabaseMeta struct {
	Query  string
	Params map[string]any
}

type Response struct {
	StatusCode int
	Body       []byte
	Headers    map[string]string
	Error      error
}

func New(channelID string, raw []byte) *Message {
	id := uuid.New().String()
	return &Message{
		ID:            id,
		CorrelationID: id,
		ChannelID:     channelID,
		Raw:           raw,
		ContentType:   ContentTypeRaw,
		Metadata:      make(map[string]any),
		Timestamp:     time.Now(),
	}
}

// EnsureHTTP initializes the HTTP meta if nil and returns it.
func (m *Message) EnsureHTTP() *HTTPMeta {
	if m.HTTP == nil {
		m.HTTP = &HTTPMeta{
			Headers:     make(map[string]string),
			QueryParams: make(map[string]string),
			PathParams:  make(map[string]string),
		}
	}
	return m.HTTP
}

// ClearTransportMeta resets all transport-specific metadata fields so a
// destination connector can stamp fresh values without stale source metadata.
func (m *Message) ClearTransportMeta() {
	m.HTTP = nil
	m.File = nil
	m.FTP = nil
	m.Kafka = nil
	m.TCP = nil
	m.SMTP = nil
	m.DICOM = nil
	m.Database = nil
}

// CloneWithRaw returns a shallow copy of the message with Raw replaced.
func (m *Message) CloneWithRaw(raw []byte) *Message {
	c := *m
	c.Raw = raw
	return &c
}

// IntuJSONVersion is the envelope format version stamped into every
// serialized IntuMessage. Consumers can use it for forward compatibility.
const IntuJSONVersion = "1"

// ToIntuJSON serializes the Message as an IntuMessage JSON envelope.
// If Raw is valid UTF-8 the body is stored as a string; otherwise it is
// base64-encoded to avoid data corruption in JSON.
func (m *Message) ToIntuJSON() ([]byte, error) {
	var body any
	if utf8.Valid(m.Raw) {
		body = string(m.Raw)
	} else {
		body = map[string]any{
			"base64": base64.StdEncoding.EncodeToString(m.Raw),
			"size":   len(m.Raw),
		}
	}
	im := map[string]any{
		"version":       IntuJSONVersion,
		"id":            m.ID,
		"correlationId": m.CorrelationID,
		"channelId":     m.ChannelID,
		"timestamp":     m.Timestamp.Format(time.RFC3339Nano),
		"body":          body,
		"transport":     m.Transport,
		"contentType":   string(m.ContentType),
	}

	if m.SourceCharset != "" {
		im["sourceCharset"] = m.SourceCharset
	}
	if len(m.Metadata) > 0 {
		im["metadata"] = m.Metadata
	}

	if m.HTTP != nil {
		im["http"] = map[string]any{
			"headers":     ensureMap(m.HTTP.Headers),
			"queryParams": ensureMap(m.HTTP.QueryParams),
			"pathParams":  ensureMap(m.HTTP.PathParams),
			"method":      m.HTTP.Method,
			"statusCode":  m.HTTP.StatusCode,
		}
	}
	if m.File != nil {
		im["file"] = map[string]any{
			"filename":  m.File.Filename,
			"directory": m.File.Directory,
		}
	}
	if m.FTP != nil {
		im["ftp"] = map[string]any{
			"filename":  m.FTP.Filename,
			"directory": m.FTP.Directory,
		}
	}
	if m.Kafka != nil {
		im["kafka"] = map[string]any{
			"headers":   ensureMap(m.Kafka.Headers),
			"topic":     m.Kafka.Topic,
			"key":       m.Kafka.Key,
			"partition": m.Kafka.Partition,
			"offset":    m.Kafka.Offset,
		}
	}
	if m.TCP != nil {
		im["tcp"] = map[string]any{
			"remoteAddr": m.TCP.RemoteAddr,
		}
	}
	if m.SMTP != nil {
		im["smtp"] = map[string]any{
			"from":    m.SMTP.From,
			"to":      m.SMTP.To,
			"subject": m.SMTP.Subject,
			"cc":      m.SMTP.CC,
			"bcc":     m.SMTP.BCC,
		}
	}
	if m.DICOM != nil {
		im["dicom"] = map[string]any{
			"callingAE": m.DICOM.CallingAE,
			"calledAE":  m.DICOM.CalledAE,
		}
	}
	if m.Database != nil {
		im["database"] = map[string]any{
			"query":  m.Database.Query,
			"params": m.Database.Params,
		}
	}

	return marshalRaw(im)
}

// FromIntuJSON reconstructs a Message from IntuMessage JSON stored in content.
// Identity fields (id, correlationId, channelId, timestamp) are restored when
// present, making export/import/replay fully round-trippable. Falls back to
// generating fresh identity when the envelope predates version 1.
func FromIntuJSON(data []byte, channelID string) (*Message, error) {
	var im map[string]any
	if err := json.Unmarshal(data, &im); err != nil {
		return nil, err
	}

	var raw []byte
	switch b := im["body"].(type) {
	case string:
		raw = []byte(b)
	case map[string]any:
		if encoded, ok := b["base64"].(string); ok {
			decoded, err := base64.StdEncoding.DecodeString(encoded)
			if err == nil {
				raw = decoded
			}
		}
	}
	msg := New(channelID, raw)

	if id, ok := im["id"].(string); ok && id != "" {
		msg.ID = id
	}
	if cid, ok := im["correlationId"].(string); ok && cid != "" {
		msg.CorrelationID = cid
	}
	if ch, ok := im["channelId"].(string); ok && ch != "" {
		msg.ChannelID = ch
	}
	if ts, ok := im["timestamp"].(string); ok && ts != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			msg.Timestamp = parsed
		}
	}
	if sc, ok := im["sourceCharset"].(string); ok {
		msg.SourceCharset = sc
	}
	if md, ok := im["metadata"].(map[string]any); ok {
		for k, v := range md {
			msg.Metadata[k] = v
		}
	}

	if t, ok := im["transport"].(string); ok {
		msg.Transport = t
	}
	if ct, ok := im["contentType"].(string); ok && ct != "" {
		msg.ContentType = ContentType(ct)
	}

	if httpData, ok := im["http"].(map[string]any); ok {
		msg.HTTP = parseHTTPMetaMap(httpData)
	}
	if fileData, ok := im["file"].(map[string]any); ok {
		msg.File = parseFileMetaMap(fileData)
	}
	if ftpData, ok := im["ftp"].(map[string]any); ok {
		msg.FTP = parseFTPMetaMap(ftpData)
	}
	if kafkaData, ok := im["kafka"].(map[string]any); ok {
		msg.Kafka = parseKafkaMetaMap(kafkaData)
	}
	if tcpData, ok := im["tcp"].(map[string]any); ok {
		msg.TCP = parseTCPMetaMap(tcpData)
	}
	if smtpData, ok := im["smtp"].(map[string]any); ok {
		msg.SMTP = parseSMTPMetaMap(smtpData)
	}
	if dicomData, ok := im["dicom"].(map[string]any); ok {
		msg.DICOM = parseDICOMMetaMap(dicomData)
	}
	if dbData, ok := im["database"].(map[string]any); ok {
		msg.Database = parseDatabaseMetaMap(dbData)
	}

	return msg, nil
}

// Rebuild reconstructs a Message from a stored MessageRecord. It uses
// FromIntuJSON when the content is valid IntuMessage JSON, falling back to
// treating it as raw bytes. CorrelationID is always restored from the record.
// This is the canonical reprocess path used by both CLI and dashboard.
func Rebuild(id, correlationID, channelID string, content []byte, ts time.Time) *Message {
	msg, err := FromIntuJSON(content, channelID)
	if err != nil {
		msg = New(channelID, content)
	}
	msg.CorrelationID = correlationID
	if msg.CorrelationID == "" {
		msg.CorrelationID = id
	}
	msg.Metadata["reprocessed"] = true
	msg.Metadata["original_message_id"] = id
	msg.Metadata["original_timestamp"] = ts.Format(time.RFC3339)
	return msg
}

// ResponseToIntuJSON converts a destination Response into IntuMessage JSON.
func ResponseToIntuJSON(resp *Response) ([]byte, error) {
	if resp == nil {
		return marshalRaw(map[string]any{"body": "", "transport": "http", "contentType": "raw"})
	}
	var respBody any
	if utf8.Valid(resp.Body) {
		respBody = string(resp.Body)
	} else {
		respBody = map[string]any{
			"base64": base64.StdEncoding.EncodeToString(resp.Body),
			"size":   len(resp.Body),
		}
	}
	im := map[string]any{
		"body":        respBody,
		"transport":   "http",
		"contentType": "raw",
		"http": map[string]any{
			"headers":    ensureMap(resp.Headers),
			"statusCode": resp.StatusCode,
		},
	}
	return marshalRaw(im)
}

func ensureMap(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}
	return m
}

func parseHTTPMetaMap(data map[string]any) *HTTPMeta {
	meta := &HTTPMeta{
		Headers:     toStringMap(data["headers"]),
		QueryParams: toStringMap(data["queryParams"]),
		PathParams:  toStringMap(data["pathParams"]),
	}
	if v, ok := data["method"].(string); ok {
		meta.Method = v
	}
	if v, ok := data["statusCode"].(float64); ok {
		meta.StatusCode = int(v)
	}
	return meta
}

func parseFileMetaMap(data map[string]any) *FileMeta {
	meta := &FileMeta{}
	if v, ok := data["filename"].(string); ok {
		meta.Filename = v
	}
	if v, ok := data["directory"].(string); ok {
		meta.Directory = v
	}
	return meta
}

func parseFTPMetaMap(data map[string]any) *FTPMeta {
	meta := &FTPMeta{}
	if v, ok := data["filename"].(string); ok {
		meta.Filename = v
	}
	if v, ok := data["directory"].(string); ok {
		meta.Directory = v
	}
	return meta
}

func parseKafkaMetaMap(data map[string]any) *KafkaMeta {
	meta := &KafkaMeta{
		Headers: toStringMap(data["headers"]),
	}
	if v, ok := data["topic"].(string); ok {
		meta.Topic = v
	}
	if v, ok := data["key"].(string); ok {
		meta.Key = v
	}
	if v, ok := data["partition"].(float64); ok {
		meta.Partition = int(v)
	}
	if v, ok := data["offset"].(float64); ok {
		meta.Offset = int64(v)
	}
	return meta
}

func parseTCPMetaMap(data map[string]any) *TCPMeta {
	meta := &TCPMeta{}
	if v, ok := data["remoteAddr"].(string); ok {
		meta.RemoteAddr = v
	}
	return meta
}

func parseSMTPMetaMap(data map[string]any) *SMTPMeta {
	meta := &SMTPMeta{}
	if v, ok := data["from"].(string); ok {
		meta.From = v
	}
	if v, ok := data["to"]; ok {
		meta.To = toStringSlice(v)
	}
	if v, ok := data["subject"].(string); ok {
		meta.Subject = v
	}
	if v, ok := data["cc"]; ok {
		meta.CC = toStringSlice(v)
	}
	if v, ok := data["bcc"]; ok {
		meta.BCC = toStringSlice(v)
	}
	return meta
}

func parseDICOMMetaMap(data map[string]any) *DICOMMeta {
	meta := &DICOMMeta{}
	if v, ok := data["callingAE"].(string); ok {
		meta.CallingAE = v
	}
	if v, ok := data["calledAE"].(string); ok {
		meta.CalledAE = v
	}
	return meta
}

func parseDatabaseMetaMap(data map[string]any) *DatabaseMeta {
	meta := &DatabaseMeta{}
	if v, ok := data["query"].(string); ok {
		meta.Query = v
	}
	if v, ok := data["params"].(map[string]any); ok {
		meta.Params = v
	}
	return meta
}

func toStringMap(v any) map[string]string {
	result := make(map[string]string)
	m, ok := v.(map[string]any)
	if !ok {
		return result
	}
	for k, val := range m {
		if s, ok := val.(string); ok {
			result[k] = s
		} else {
			result[k] = fmt.Sprintf("%v", val)
		}
	}
	return result
}

func toStringSlice(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			result = append(result, s)
		}
	}
	return result
}
