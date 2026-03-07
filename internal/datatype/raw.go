package datatype

import (
	"bytes"
	"encoding/json"
)

type RawParser struct{}

func (r *RawParser) Parse(raw []byte) (any, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
		var parsed any
		if err := json.Unmarshal(trimmed, &parsed); err == nil {
			return parsed, nil
		}
	}
	return string(raw), nil
}

func (r *RawParser) Serialize(data any) ([]byte, error) {
	switch v := data.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return []byte{}, nil
	}
}

func (r *RawParser) ContentType() string {
	return "raw"
}
