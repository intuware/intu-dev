package datatype

import "fmt"

type Parser interface {
	Parse(raw []byte) (any, error)
	Serialize(data any) ([]byte, error)
	ContentType() string
}

func NewParser(typeName string) (Parser, error) {
	switch typeName {
	case "raw", "":
		return &RawParser{}, nil
	case "json":
		return &JSONParser{}, nil
	case "xml":
		return &XMLParser{}, nil
	case "csv", "delimited":
		return &CSVParser{}, nil
	case "hl7v2":
		return &HL7v2Parser{}, nil
	case "hl7v3", "ccda":
		return &XMLParser{}, nil
	case "fhir_r4":
		return &JSONParser{}, nil
	case "x12":
		return &X12Parser{}, nil
	case "binary":
		return &BinaryParser{}, nil
	default:
		return nil, fmt.Errorf("unsupported data type: %s", typeName)
	}
}
