package datatype

type RawParser struct{}

func (r *RawParser) Parse(raw []byte) (any, error) {
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
