package healthcare

import (
	"fmt"
	"strings"
)

type X12Transaction struct {
	TransactionSet string
	Version        string
	Segments       []X12Segment
}

type X12Segment struct {
	ID       string
	Elements []string
}

func ParseX12(raw string) (*X12Transaction, error) {
	if len(raw) < 106 {
		return nil, fmt.Errorf("X12 message too short")
	}

	elementSep := string(raw[3])
	segmentSep := "~"
	if len(raw) > 105 {
		segmentSep = string(raw[105])
	}

	tx := &X12Transaction{}
	parts := strings.Split(raw, segmentSep)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		elements := strings.Split(part, elementSep)
		if len(elements) == 0 {
			continue
		}
		seg := X12Segment{
			ID:       elements[0],
			Elements: elements[1:],
		}
		tx.Segments = append(tx.Segments, seg)

		if seg.ID == "ST" && len(seg.Elements) > 0 {
			tx.TransactionSet = seg.Elements[0]
		}
		if seg.ID == "GS" && len(seg.Elements) > 7 {
			tx.Version = seg.Elements[7]
		}
	}

	return tx, nil
}

func (tx *X12Transaction) FindSegments(id string) []X12Segment {
	var result []X12Segment
	for _, seg := range tx.Segments {
		if seg.ID == id {
			result = append(result, seg)
		}
	}
	return result
}

func (tx *X12Transaction) GetElement(segID string, index int) string {
	segs := tx.FindSegments(segID)
	if len(segs) == 0 {
		return ""
	}
	if index >= len(segs[0].Elements) {
		return ""
	}
	return segs[0].Elements[index]
}

func (tx *X12Transaction) Serialize(elementSep, segmentSep string) string {
	var parts []string
	for _, seg := range tx.Segments {
		line := seg.ID
		for _, e := range seg.Elements {
			line += elementSep + e
		}
		parts = append(parts, line)
	}
	return strings.Join(parts, segmentSep)
}
