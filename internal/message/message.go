package message

import (
	"time"

	"github.com/google/uuid"
)

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
