package healthcare

import (
	"fmt"
	"strings"
	"time"
)

type HL7v2Builder struct {
	segments []string
}

func NewHL7v2Builder() *HL7v2Builder {
	return &HL7v2Builder{}
}

func (b *HL7v2Builder) AddSegment(name string, fields ...string) *HL7v2Builder {
	seg := name
	for _, f := range fields {
		seg += "|" + f
	}
	b.segments = append(b.segments, seg)
	return b
}

func (b *HL7v2Builder) Build() string {
	return strings.Join(b.segments, "\r")
}

func BuildACK(originalMSH map[string]any, ackCode, textMessage string) string {
	builder := NewHL7v2Builder()

	sendingApp := getField(originalMSH, "3")
	sendingFac := getField(originalMSH, "4")
	receivingApp := getField(originalMSH, "5")
	receivingFac := getField(originalMSH, "6")
	controlID := getField(originalMSH, "10")

	now := time.Now().Format("20060102150405")

	builder.AddSegment("MSH",
		"^~\\&",
		receivingApp, receivingFac,
		sendingApp, sendingFac,
		now,
		"",
		"ACK^"+getField(originalMSH, "9"),
		fmt.Sprintf("ACK%s", controlID),
		"P", "2.5.1",
	)

	builder.AddSegment("MSA",
		ackCode,
		controlID,
		textMessage,
	)

	return builder.Build()
}

func BuildNACK(originalMSH map[string]any, errorCode, errorMessage string) string {
	return BuildACK(originalMSH, errorCode, errorMessage)
}

func getField(msh map[string]any, key string) string {
	if val, ok := msh[key]; ok {
		switch v := val.(type) {
		case string:
			return v
		case map[string]any:
			if s, ok := v["1"]; ok {
				return fmt.Sprintf("%v", s)
			}
		}
	}
	return ""
}

func ParseHL7Path(msg map[string]any, path string) (string, error) {
	parts := strings.Split(path, ".")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid HL7 path: %s", path)
	}

	segName := parts[0]
	seg, ok := msg[segName]
	if !ok {
		return "", fmt.Errorf("segment %s not found", segName)
	}

	segMap, ok := seg.(map[string]any)
	if !ok {
		return "", fmt.Errorf("segment %s is not a map", segName)
	}

	fieldKey := parts[1]
	field, ok := segMap[fieldKey]
	if !ok {
		return "", fmt.Errorf("field %s.%s not found", segName, fieldKey)
	}

	if len(parts) == 2 {
		return fmt.Sprintf("%v", field), nil
	}

	compMap, ok := field.(map[string]any)
	if !ok {
		return fmt.Sprintf("%v", field), nil
	}

	compKey := parts[2]
	comp, ok := compMap[compKey]
	if !ok {
		return "", fmt.Errorf("component %s.%s.%s not found", segName, fieldKey, compKey)
	}

	return fmt.Sprintf("%v", comp), nil
}
