package datatype

import (
	"fmt"
	"strings"
)

type HL7v2Parser struct{}

type HL7v2Message struct {
	Segments []HL7v2Segment `json:"segments"`
	Raw      string         `json:"raw"`
}

type HL7v2Segment struct {
	Name   string     `json:"name"`
	Fields []HL7Field `json:"fields"`
}

type HL7Field struct {
	Value      string   `json:"value"`
	Components []string `json:"components,omitempty"`
}

func (h *HL7v2Parser) Parse(raw []byte) (any, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return nil, fmt.Errorf("empty HL7v2 message")
	}

	text = strings.ReplaceAll(text, "\r\n", "\r")
	text = strings.ReplaceAll(text, "\n", "\r")
	lines := strings.Split(text, "\r")

	msg := &HL7v2Message{Raw: text}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fieldSep := "|"
		compSep := "^"

		if strings.HasPrefix(line, "MSH") && len(line) > 3 {
			fieldSep = string(line[3])
			if len(line) > 4 {
				encodingChars := ""
				restFields := strings.SplitN(line[4:], fieldSep, 2)
				if len(restFields) > 0 {
					encodingChars = restFields[0]
				}
				if len(encodingChars) > 0 {
					compSep = string(encodingChars[0])
				}
			}
		}

		parts := strings.Split(line, fieldSep)
		seg := HL7v2Segment{Name: parts[0]}

		for _, part := range parts[1:] {
			field := HL7Field{Value: part}
			if strings.Contains(part, compSep) {
				field.Components = strings.Split(part, compSep)
			}
			seg.Fields = append(seg.Fields, field)
		}

		msg.Segments = append(msg.Segments, seg)
	}

	result := make(map[string]any)
	result["raw"] = text
	result["segments"] = msg.Segments

	for _, seg := range msg.Segments {
		segData := make(map[string]any)
		for i, f := range seg.Fields {
			key := fmt.Sprintf("%d", i+1)
			if len(f.Components) > 0 {
				compMap := make(map[string]any)
				for j, c := range f.Components {
					compMap[fmt.Sprintf("%d", j+1)] = c
				}
				segData[key] = compMap
			} else {
				segData[key] = f.Value
			}
		}

		if _, exists := result[seg.Name]; exists {
			switch existing := result[seg.Name].(type) {
			case []any:
				result[seg.Name] = append(existing, segData)
			default:
				result[seg.Name] = []any{existing, segData}
			}
		} else {
			result[seg.Name] = segData
		}
	}

	return result, nil
}

func (h *HL7v2Parser) Serialize(data any) ([]byte, error) {
	if msg, ok := data.(map[string]any); ok {
		if raw, ok := msg["raw"].(string); ok {
			return []byte(raw), nil
		}
	}
	return nil, fmt.Errorf("cannot serialize non-HL7v2 data")
}

func (h *HL7v2Parser) ContentType() string {
	return "hl7v2"
}
