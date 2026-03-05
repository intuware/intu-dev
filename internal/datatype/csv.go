package datatype

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strings"
)

type CSVParser struct{}

func (c *CSVParser) Parse(raw []byte) (any, error) {
	reader := csv.NewReader(bytes.NewReader(raw))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("csv parse: %w", err)
	}

	if len(records) == 0 {
		return []map[string]string{}, nil
	}

	headers := records[0]
	var result []map[string]string

	for _, row := range records[1:] {
		record := make(map[string]string)
		for i, val := range row {
			if i < len(headers) {
				record[headers[i]] = val
			}
		}
		result = append(result, record)
	}

	return result, nil
}

func (c *CSVParser) Serialize(data any) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	switch v := data.(type) {
	case []map[string]string:
		if len(v) == 0 {
			return buf.Bytes(), nil
		}
		var headers []string
		for k := range v[0] {
			headers = append(headers, k)
		}
		writer.Write(headers)
		for _, row := range v {
			var vals []string
			for _, h := range headers {
				vals = append(vals, row[h])
			}
			writer.Write(vals)
		}
	case [][]string:
		for _, row := range v {
			writer.Write(row)
		}
	default:
		return nil, fmt.Errorf("csv serialize: unsupported type %T", data)
	}

	writer.Flush()
	return []byte(strings.TrimSpace(buf.String())), writer.Error()
}

func (c *CSVParser) ContentType() string {
	return "csv"
}
