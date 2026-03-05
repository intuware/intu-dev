package datatype

import "encoding/base64"

type BinaryParser struct{}

func (b *BinaryParser) Parse(raw []byte) (any, error) {
	encoded := base64.StdEncoding.EncodeToString(raw)
	return map[string]any{
		"base64": encoded,
		"size":   len(raw),
	}, nil
}

func (b *BinaryParser) Serialize(data any) ([]byte, error) {
	if m, ok := data.(map[string]any); ok {
		if encoded, ok := m["base64"].(string); ok {
			return base64.StdEncoding.DecodeString(encoded)
		}
	}
	if s, ok := data.(string); ok {
		return base64.StdEncoding.DecodeString(s)
	}
	return nil, nil
}

func (b *BinaryParser) ContentType() string {
	return "binary"
}
