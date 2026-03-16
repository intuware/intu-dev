package datatype

import (
	"testing"
)

func TestCSVParser_UnicodePreserved(t *testing.T) {
	csv := []byte("name,city\nJosé,Montréal\n")

	p := &CSVParser{}
	result, err := p.Parse(csv)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	rows := result.([]map[string]string)
	if rows[0]["name"] != "José" {
		t.Errorf("expected name=José, got %q", rows[0]["name"])
	}
	if rows[0]["city"] != "Montréal" {
		t.Errorf("expected city=Montréal, got %q", rows[0]["city"])
	}
}

func TestCSVParser_RoundTrip(t *testing.T) {
	input := []byte("name,city\nJosé,Zürich\n")

	p := &CSVParser{}
	parsed, err := p.Parse(input)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	serialized, err := p.Serialize(parsed)
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}

	reparsed, err := p.Parse(serialized)
	if err != nil {
		t.Fatalf("Re-parse error: %v", err)
	}
	rows := reparsed.([]map[string]string)
	if rows[0]["name"] != "José" {
		t.Errorf("round-trip name: got %q, want José", rows[0]["name"])
	}
	if rows[0]["city"] != "Zürich" {
		t.Errorf("round-trip city: got %q, want Zürich", rows[0]["city"])
	}
}
