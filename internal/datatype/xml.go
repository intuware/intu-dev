package datatype

import (
	"encoding/xml"
	"fmt"
)

type XMLParser struct{}

type XMLNode struct {
	XMLName  xml.Name   `xml:"" json:"-"`
	Attrs    []xml.Attr `xml:"-" json:"attrs,omitempty"`
	Content  string     `xml:",chardata" json:"content,omitempty"`
	Children []*XMLNode `xml:",any" json:"children,omitempty"`
	Tag      string     `xml:"-" json:"tag"`
}

func (x *XMLParser) Parse(raw []byte) (any, error) {
	var node XMLNode
	if err := xml.Unmarshal(raw, &node); err != nil {
		return nil, fmt.Errorf("xml parse: %w", err)
	}
	return xmlToMap(&node), nil
}

func xmlToMap(node *XMLNode) map[string]any {
	result := make(map[string]any)
	result["_tag"] = node.XMLName.Local
	if node.Content != "" {
		result["_text"] = node.Content
	}
	for _, attr := range node.Attrs {
		result["@"+attr.Name.Local] = attr.Value
	}
	for _, child := range node.Children {
		childMap := xmlToMap(child)
		key := child.XMLName.Local
		if existing, ok := result[key]; ok {
			if arr, ok := existing.([]any); ok {
				result[key] = append(arr, childMap)
			} else {
				result[key] = []any{existing, childMap}
			}
		} else {
			result[key] = childMap
		}
	}
	return result
}

func (x *XMLParser) Serialize(data any) ([]byte, error) {
	return xml.MarshalIndent(data, "", "  ")
}

func (x *XMLParser) ContentType() string {
	return "xml"
}
