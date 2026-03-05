package datatype

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type BatchSplitter interface {
	Split(raw []byte) ([][]byte, error)
}

func NewBatchSplitter(splitOn string) (BatchSplitter, error) {
	switch splitOn {
	case "newline", "":
		return &NewlineSplitter{}, nil
	case "hl7_batch":
		return &HL7BatchSplitter{}, nil
	case "fhir_bundle":
		return &FHIRBundleSplitter{}, nil
	case "xml_root":
		return &XMLRootSplitter{}, nil
	default:
		return nil, fmt.Errorf("unsupported batch split type: %s", splitOn)
	}
}

type NewlineSplitter struct{}

func (n *NewlineSplitter) Split(raw []byte) ([][]byte, error) {
	lines := bytes.Split(raw, []byte("\n"))
	var result [][]byte
	for _, line := range lines {
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 {
			result = append(result, trimmed)
		}
	}
	return result, nil
}

type HL7BatchSplitter struct{}

func (h *HL7BatchSplitter) Split(raw []byte) ([][]byte, error) {
	text := string(raw)
	text = strings.ReplaceAll(text, "\r\n", "\r")
	text = strings.ReplaceAll(text, "\n", "\r")

	var messages [][]byte
	var current strings.Builder

	for _, line := range strings.Split(text, "\r") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "MSH") {
			if current.Len() > 0 {
				messages = append(messages, []byte(current.String()))
				current.Reset()
			}
		}
		if current.Len() > 0 {
			current.WriteString("\r")
		}
		current.WriteString(line)
	}
	if current.Len() > 0 {
		messages = append(messages, []byte(current.String()))
	}

	return messages, nil
}

type FHIRBundleSplitter struct{}

func (f *FHIRBundleSplitter) Split(raw []byte) ([][]byte, error) {
	var bundle map[string]any
	if err := json.Unmarshal(raw, &bundle); err != nil {
		return [][]byte{raw}, nil
	}

	entries, ok := bundle["entry"].([]any)
	if !ok {
		return [][]byte{raw}, nil
	}

	var result [][]byte
	for _, entry := range entries {
		entryMap, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		resource, ok := entryMap["resource"]
		if !ok {
			resource = entry
		}
		data, err := json.Marshal(resource)
		if err != nil {
			continue
		}
		result = append(result, data)
	}

	if len(result) == 0 {
		return [][]byte{raw}, nil
	}
	return result, nil
}

type XMLRootSplitter struct{}

func (x *XMLRootSplitter) Split(raw []byte) ([][]byte, error) {
	return [][]byte{raw}, nil
}
