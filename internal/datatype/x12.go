package datatype

import (
	"fmt"
	"strings"
)

type X12Parser struct{}

type X12Segment struct {
	ID       string   `json:"id"`
	Elements []string `json:"elements"`
}

func (x *X12Parser) Parse(raw []byte) (any, error) {
	text := strings.TrimSpace(string(raw))
	if len(text) < 106 {
		return nil, fmt.Errorf("X12 message too short")
	}

	elementSep := string(text[3])
	segmentSep := "~"

	if len(text) > 105 {
		segmentSep = string(text[105])
	}

	segments := strings.Split(text, segmentSep)
	result := make(map[string]any)
	var parsedSegments []X12Segment

	for _, seg := range segments {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		elements := strings.Split(seg, elementSep)
		if len(elements) == 0 {
			continue
		}
		x12Seg := X12Segment{
			ID:       elements[0],
			Elements: elements[1:],
		}
		parsedSegments = append(parsedSegments, x12Seg)
	}

	result["segments"] = parsedSegments
	result["raw"] = text
	return result, nil
}

func (x *X12Parser) Serialize(data any) ([]byte, error) {
	if msg, ok := data.(map[string]any); ok {
		if raw, ok := msg["raw"].(string); ok {
			return []byte(raw), nil
		}
	}
	return nil, fmt.Errorf("cannot serialize non-X12 data")
}

func (x *X12Parser) ContentType() string {
	return "x12"
}
