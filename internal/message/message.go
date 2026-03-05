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
	ContentType   ContentType
	Headers       map[string]string
	Metadata      map[string]any
	Timestamp     time.Time
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
		Headers:       make(map[string]string),
		Metadata:      make(map[string]any),
		Timestamp:     time.Now(),
	}
}
