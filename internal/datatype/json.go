package datatype

import "encoding/json"

type JSONParser struct{}

func (j *JSONParser) Parse(raw []byte) (any, error) {
	var result any
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (j *JSONParser) Serialize(data any) ([]byte, error) {
	return json.Marshal(data)
}

func (j *JSONParser) ContentType() string {
	return "json"
}
