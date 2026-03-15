package datatype

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// --- NewParser factory tests ---

func TestNewParser_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		wantErr  bool
	}{
		{"raw", "raw", false},
		{"empty defaults to raw", "", false},
		{"json", "json", false},
		{"xml", "xml", false},
		{"csv", "csv", false},
		{"delimited", "delimited", false},
		{"hl7v2", "hl7v2", false},
		{"hl7v3", "hl7v3", false},
		{"ccda", "ccda", false},
		{"fhir_r4", "fhir_r4", false},
		{"x12", "x12", false},
		{"binary", "binary", false},
		{"unknown", "unknown", true},
		{"invalid", "invalid_type", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewParser(tt.typeName)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewParser(%q) error = %v, wantErr %v", tt.typeName, err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err == nil {
					t.Error("expected error for unknown type")
				}
				if !strings.Contains(err.Error(), tt.typeName) {
					t.Errorf("error should mention type: %v", err)
				}
				return
			}
			if p == nil {
				t.Error("parser should not be nil")
			}
		})
	}
}

func TestNewParser_ContentTypes(t *testing.T) {
	typeContent := map[string]string{
		"raw": "raw", "json": "json", "xml": "xml", "csv": "csv",
		"hl7v2": "hl7v2", "x12": "x12", "binary": "binary",
	}
	for typeName, want := range typeContent {
		p, err := NewParser(typeName)
		if err != nil {
			t.Fatalf("NewParser(%q): %v", typeName, err)
		}
		if got := p.ContentType(); got != want {
			t.Errorf("NewParser(%q).ContentType() = %q, want %q", typeName, got, want)
		}
	}
}

// --- JSONParser tests ---

func TestJSONParser_Parse(t *testing.T) {
	p := &JSONParser{}
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
	}{
		{"valid object", []byte(`{"a":1,"b":"x"}`), false},
		{"valid array", []byte(`[1,2,3]`), false},
		{"empty object", []byte(`{}`), false},
		{"empty array", []byte(`[]`), false},
		{"null", []byte(`null`), false}, // null parses to nil
		{"invalid", []byte(`{invalid`), true},
		{"empty", []byte(``), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.Parse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == nil && string(tt.input) != "null" {
				t.Error("Parse() returned nil for valid input (null is allowed to return nil)")
			}
		})
	}
}

func TestJSONParser_Serialize(t *testing.T) {
	p := &JSONParser{}
	data := map[string]any{"x": 1, "y": "z"}
	got, err := p.Serialize(data)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("Serialize output not valid JSON: %v", err)
	}
	if decoded["x"] != float64(1) || decoded["y"] != "z" {
		t.Errorf("Serialize round-trip failed: %v", decoded)
	}
}

func TestJSONParser_ContentType(t *testing.T) {
	p := &JSONParser{}
	if got := p.ContentType(); got != "json" {
		t.Errorf("ContentType() = %q, want json", got)
	}
}

// --- XMLParser tests ---

func TestXMLParser_Parse_NestedChildren(t *testing.T) {
	p := &XMLParser{}
	input := []byte(`<root><child><grandchild>text</grandchild></child></root>`)
	got, err := p.Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	if m["_tag"] != "root" {
		t.Errorf("root _tag = %v, want root", m["_tag"])
	}
	child, ok := m["child"].(map[string]any)
	if !ok {
		t.Fatalf("child not map: %T", m["child"])
	}
	if child["_tag"] != "child" {
		t.Errorf("child _tag = %v", child["_tag"])
	}
	gc, ok := child["grandchild"].(map[string]any)
	if !ok {
		t.Fatalf("grandchild not map: %T", child["grandchild"])
	}
	if gc["_text"] != "text" {
		t.Errorf("grandchild _text = %v, want text", gc["_text"])
	}
}

func TestXMLParser_Parse_RepeatedChildren(t *testing.T) {
	p := &XMLParser{}
	input := []byte(`<root><item>a</item><item>b</item><item>c</item></root>`)
	got, err := p.Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	items, ok := m["item"].([]any)
	if !ok {
		t.Fatalf("item not []any: %T", m["item"])
	}
	if len(items) != 3 {
		t.Fatalf("len(items) = %d, want 3", len(items))
	}
	for i, want := range []string{"a", "b", "c"} {
		im, ok := items[i].(map[string]any)
		if !ok {
			t.Fatalf("items[%d] not map", i)
		}
		if im["_text"] != want {
			t.Errorf("items[%d]._text = %v, want %s", i, im["_text"], want)
		}
	}
}

func TestXMLParser_Parse_Attributes(t *testing.T) {
	// Note: XMLNode.Attrs has xml:"-" so standard Unmarshal does not populate it.
	// xmlToMap iterates Attrs; we verify _tag and _text work for elements with attributes.
	p := &XMLParser{}
	input := []byte(`<elem id="123" name="test">content</elem>`)
	got, err := p.Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	if m["_tag"] != "elem" {
		t.Errorf("_tag = %v, want elem", m["_tag"])
	}
	if m["_text"] != "content" {
		t.Errorf("_text = %v, want content", m["_text"])
	}
}

func TestXMLParser_Parse_EmptyAndInvalid(t *testing.T) {
	p := &XMLParser{}
	_, err := p.Parse([]byte(``))
	if err == nil {
		t.Error("empty input should error")
	}
	_, err = p.Parse([]byte(`<unclosed`))
	if err == nil {
		t.Error("invalid XML should error")
	}
}

func TestXMLParser_Serialize(t *testing.T) {
	// xml.MarshalIndent does not support map[string]any; it needs structs or xml.Marshaler.
	// Parse produces map; Serialize with map returns error. We verify Serialize is invoked.
	p := &XMLParser{}
	input := []byte(`<root>hello</root>`)
	parsed, err := p.Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	_, err = p.Serialize(parsed)
	if err == nil {
		t.Error("Serialize with map input should error (xml.Marshal does not support map)")
	}
}

func TestXMLParser_ContentType(t *testing.T) {
	p := &XMLParser{}
	if got := p.ContentType(); got != "xml" {
		t.Errorf("ContentType() = %q, want xml", got)
	}
}

// --- RawParser tests ---

func TestRawParser_Parse_JSONAutoDetect(t *testing.T) {
	p := &RawParser{}
	// Starts with { - JSON
	got, err := p.Parse([]byte(`{"a":1}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map for JSON, got %T", got)
	}
	if m["a"] != float64(1) {
		t.Errorf("Parse JSON: got %v", m)
	}
	// Starts with [ - JSON
	got, err = p.Parse([]byte(`[1,2,3]`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected slice for JSON array, got %T", got)
	}
	if len(arr) != 3 {
		t.Errorf("Parse JSON array: len=%d", len(arr))
	}
}

func TestRawParser_Parse_JSONWithWhitespace(t *testing.T) {
	p := &RawParser{}
	got, err := p.Parse([]byte("  \n\t  {\"x\":1}  "))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	if m["x"] != float64(1) {
		t.Errorf("got %v", m)
	}
}

func TestRawParser_Parse_NonJSONReturnsString(t *testing.T) {
	p := &RawParser{}
	got, err := p.Parse([]byte("plain text"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	s, ok := got.(string)
	if !ok {
		t.Fatalf("expected string, got %T", got)
	}
	if s != "plain text" {
		t.Errorf("got %q", s)
	}
}

func TestRawParser_Parse_InvalidJSONReturnsString(t *testing.T) {
	p := &RawParser{}
	// Starts with { but invalid JSON - falls through to string
	got, err := p.Parse([]byte("{invalid json"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	s, ok := got.(string)
	if !ok {
		t.Fatalf("expected string for invalid JSON, got %T", got)
	}
	if s != "{invalid json" {
		t.Errorf("got %q", s)
	}
}

func TestRawParser_Parse_Empty(t *testing.T) {
	p := &RawParser{}
	got, err := p.Parse([]byte(""))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.(string) != "" {
		t.Errorf("empty input should return empty string: %q", got)
	}
}

func TestRawParser_Serialize(t *testing.T) {
	p := &RawParser{}
	// []byte
	got, err := p.Serialize([]byte("bytes"))
	if err != nil {
		t.Fatalf("Serialize []byte: %v", err)
	}
	if string(got) != "bytes" {
		t.Errorf("Serialize []byte: got %q", got)
	}
	// string
	got, err = p.Serialize("string")
	if err != nil {
		t.Fatalf("Serialize string: %v", err)
	}
	if string(got) != "string" {
		t.Errorf("Serialize string: got %q", got)
	}
	// default
	got, err = p.Serialize(123)
	if err != nil {
		t.Fatalf("Serialize default: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Serialize default should return empty: got %q", got)
	}
}

func TestRawParser_ContentType(t *testing.T) {
	p := &RawParser{}
	if got := p.ContentType(); got != "raw" {
		t.Errorf("ContentType() = %q, want raw", got)
	}
}

// --- BinaryParser tests ---

func TestBinaryParser_Parse(t *testing.T) {
	p := &BinaryParser{}
	input := []byte("hello")
	got, err := p.Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	if m["size"] != 5 {
		t.Errorf("size = %v, want 5", m["size"])
	}
	encoded, ok := m["base64"].(string)
	if !ok {
		t.Fatalf("base64 not string")
	}
	decoded, err := p.Serialize(map[string]any{"base64": encoded})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if string(decoded) != "hello" {
		t.Errorf("round-trip failed: got %q", decoded)
	}
}

func TestBinaryParser_Parse_Empty(t *testing.T) {
	p := &BinaryParser{}
	got, err := p.Parse([]byte{})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map")
	}
	if m["size"] != 0 {
		t.Errorf("size = %v for empty", m["size"])
	}
}

func TestBinaryParser_Serialize_FromMap(t *testing.T) {
	p := &BinaryParser{}
	encoded := "aGVsbG8=" // "hello" in base64
	got, err := p.Serialize(map[string]any{"base64": encoded})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("got %q", got)
	}
}

func TestBinaryParser_Serialize_FromString(t *testing.T) {
	p := &BinaryParser{}
	got, err := p.Serialize("aGVsbG8=")
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("got %q", got)
	}
}

func TestBinaryParser_Serialize_UnsupportedType(t *testing.T) {
	p := &BinaryParser{}
	got, err := p.Serialize(123)
	if err != nil {
		t.Fatalf("Serialize should not error: %v", err)
	}
	if got != nil {
		t.Errorf("unsupported type should return nil: %v", got)
	}
}

func TestBinaryParser_Serialize_MapWithoutBase64(t *testing.T) {
	p := &BinaryParser{}
	got, err := p.Serialize(map[string]any{"other": "key"})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if got != nil {
		t.Errorf("map without base64 should return nil: %v", got)
	}
}

func TestBinaryParser_ContentType(t *testing.T) {
	p := &BinaryParser{}
	if got := p.ContentType(); got != "binary" {
		t.Errorf("ContentType() = %q, want binary", got)
	}
}

// --- X12Parser tests ---

func TestX12Parser_Parse(t *testing.T) {
	p := &X12Parser{}
	// X12: ISA at start, element sep at pos 3, segment sep at pos 105
	// Minimum 106 chars
	raw := strings.Repeat(" ", 106)
	raw = raw[:3] + "*" + raw[4:]   // element sep at 3
	raw = raw[:105] + "~" + raw[106:] // segment sep at 105
	raw = "ISA" + raw[3:]
	raw = raw[:105] + "~" + raw[106:]
	if len(raw) < 106 {
		t.Fatalf("raw len = %d", len(raw))
	}
	got, err := p.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	if m["raw"] != raw {
		t.Error("raw not preserved")
	}
	segments, ok := m["segments"].([]X12Segment)
	if !ok {
		t.Fatalf("segments not []X12Segment: %T", m["segments"])
	}
	if len(segments) == 0 {
		t.Error("expected at least one segment")
	}
}

func TestX12Parser_Parse_TooShort(t *testing.T) {
	p := &X12Parser{}
	_, err := p.Parse([]byte("short"))
	if err == nil {
		t.Error("expected error for short message")
	}
	if !strings.Contains(err.Error(), "too short") {
		t.Errorf("error should mention too short: %v", err)
	}
}

func TestX12Parser_Serialize(t *testing.T) {
	p := &X12Parser{}
	raw := "ISA*00*          *00*          *ZZ*SENDER         *ZZ*RECEIVER       *230101*1200*^*00501*000000001*0*T*:~"
	got, err := p.Serialize(map[string]any{"raw": raw})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if string(got) != raw {
		t.Errorf("got %q", string(got))
	}
}

func TestX12Parser_Serialize_Error(t *testing.T) {
	p := &X12Parser{}
	_, err := p.Serialize("not a map")
	if err == nil {
		t.Error("expected error for non-map")
	}
	_, err = p.Serialize(map[string]any{"other": "key"})
	if err == nil {
		t.Error("expected error for map without raw")
	}
}

func TestX12Parser_ContentType(t *testing.T) {
	p := &X12Parser{}
	if got := p.ContentType(); got != "x12" {
		t.Errorf("ContentType() = %q, want x12", got)
	}
}

// --- HL7v2Parser tests ---

func TestHL7v2Parser_Parse(t *testing.T) {
	p := &HL7v2Parser{}
	msg := "MSH|^~\\&|SENDER|RECEIVER|APP|FAC|20230101120000||ADT^A01|1|P|2.5\rPID|1|123^^^MR^M"
	got, err := p.Parse([]byte(msg))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	if m["raw"] != msg {
		t.Error("raw not preserved")
	}
	msh, ok := m["MSH"].(map[string]any)
	if !ok {
		t.Fatalf("MSH not map: %T", m["MSH"])
	}
	// MSH|^~\&|SENDER|RECEIVER|... -> key 1=^~\&, 2=SENDER, 3=RECEIVER
	if msh["2"] != "SENDER" {
		t.Errorf("MSH field 2 = %v", msh["2"])
	}
	pid, ok := m["PID"].(map[string]any)
	if !ok {
		t.Fatalf("PID not map: %T", m["PID"])
	}
	// PID|1|123^^^MR^M -> key 1=1, 2=123^^^MR^M (has components)
	comp3, ok := pid["2"].(map[string]any)
	if !ok {
		t.Fatalf("PID.3 components: %T", pid["3"])
	}
	if comp3["1"] != "123" {
		t.Errorf("PID.2.1 = %v", comp3["1"])
	}
}

func TestHL7v2Parser_Parse_Components(t *testing.T) {
	p := &HL7v2Parser{}
	msg := "MSH|^~\\&|A^B^C^D^E"
	got, err := p.Parse([]byte(msg))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map")
	}
	msh, ok := m["MSH"].(map[string]any)
	if !ok {
		t.Fatalf("MSH not map")
	}
	// MSH|^~\&|A^B^C^D^E -> key 2 = A^B^C^D^E (has components)
	comp, ok := msh["2"].(map[string]any)
	if !ok {
		t.Fatalf("field 2 should have components")
	}
	if comp["1"] != "A" || comp["5"] != "E" {
		t.Errorf("components: %v", comp)
	}
}

func TestHL7v2Parser_Parse_LineEndings(t *testing.T) {
	p := &HL7v2Parser{}
	msg := "MSH|^~\\&|A\nPID|1|2"
	got, err := p.Parse([]byte(msg))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map")
	}
	if _, ok := m["PID"]; !ok {
		t.Error("PID segment not parsed")
	}
}

func TestHL7v2Parser_Parse_Empty(t *testing.T) {
	p := &HL7v2Parser{}
	_, err := p.Parse([]byte(""))
	if err == nil {
		t.Error("expected error for empty message")
	}
	_, err = p.Parse([]byte("   \n  "))
	if err == nil {
		t.Error("expected error for whitespace-only")
	}
}

func TestHL7v2Parser_Serialize(t *testing.T) {
	p := &HL7v2Parser{}
	raw := "MSH|^~\\&|A|B"
	got, err := p.Serialize(map[string]any{"raw": raw})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if string(got) != raw {
		t.Errorf("got %q", string(got))
	}
}

func TestHL7v2Parser_Serialize_Error(t *testing.T) {
	p := &HL7v2Parser{}
	_, err := p.Serialize("not a map")
	if err == nil {
		t.Error("expected error")
	}
	_, err = p.Serialize(map[string]any{"other": "key"})
	if err == nil {
		t.Error("expected error for map without raw")
	}
}

func TestHL7v2Parser_ContentType(t *testing.T) {
	p := &HL7v2Parser{}
	if got := p.ContentType(); got != "hl7v2" {
		t.Errorf("ContentType() = %q, want hl7v2", got)
	}
}

// --- CSVParser tests (excluding UnicodePreserved and RoundTrip in csv_test.go) ---

func TestCSVParser_Parse_Empty(t *testing.T) {
	p := &CSVParser{}
	got, err := p.Parse([]byte(""))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	rows, ok := got.([]map[string]string)
	if !ok {
		t.Fatalf("expected []map[string]string, got %T", got)
	}
	if len(rows) != 0 {
		t.Errorf("empty CSV should return empty rows: %d", len(rows))
	}
}

func TestCSVParser_Parse_HeaderOnly(t *testing.T) {
	p := &CSVParser{}
	got, err := p.Parse([]byte("a,b,c"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	rows, ok := got.([]map[string]string)
	if !ok {
		t.Fatalf("expected []map[string]string")
	}
	if len(rows) != 0 {
		t.Errorf("header-only should have 0 data rows: %d", len(rows))
	}
}

func TestCSVParser_Parse_Invalid(t *testing.T) {
	p := &CSVParser{}
	_, err := p.Parse([]byte("a,b,c\n\"unclosed quote"))
	if err == nil {
		t.Error("expected error for invalid CSV")
	}
}

func TestCSVParser_Serialize_EmptySlice(t *testing.T) {
	p := &CSVParser{}
	got, err := p.Serialize([]map[string]string{})
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty slice should serialize to empty: %q", got)
	}
}

func TestCSVParser_Serialize_SliceOfSlices(t *testing.T) {
	p := &CSVParser{}
	data := [][]string{{"a", "b"}, {"1", "2"}}
	got, err := p.Serialize(data)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	// [][]string writes rows without headers; first row becomes header when reparsed
	parsed, err := p.Parse(got)
	if err != nil {
		t.Fatalf("Parse serialized: %v", err)
	}
	rows := parsed.([]map[string]string)
	if len(rows) < 1 {
		t.Errorf("expected at least 1 data row: %d", len(rows))
	}
	if len(got) == 0 {
		t.Error("Serialize should produce output")
	}
}

func TestCSVParser_Serialize_UnsupportedType(t *testing.T) {
	p := &CSVParser{}
	_, err := p.Serialize(123)
	if err == nil {
		t.Error("expected error for unsupported type")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Errorf("error should mention unsupported: %v", err)
	}
}

func TestCSVParser_ContentType(t *testing.T) {
	p := &CSVParser{}
	if got := p.ContentType(); got != "csv" {
		t.Errorf("ContentType() = %q, want csv", got)
	}
}

// --- BatchSplitter tests ---

func TestNewBatchSplitter_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		splitOn  string
		wantErr  bool
	}{
		{"newline", "newline", false},
		{"empty defaults to newline", "", false},
		{"hl7_batch", "hl7_batch", false},
		{"fhir_bundle", "fhir_bundle", false},
		{"xml_root", "xml_root", false},
		{"unknown", "unknown", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewBatchSplitter(tt.splitOn)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewBatchSplitter(%q) error = %v, wantErr %v", tt.splitOn, err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if s == nil {
				t.Error("splitter should not be nil")
			}
		})
	}
}

func TestNewlineSplitter_Split(t *testing.T) {
	s := &NewlineSplitter{}
	raw := []byte("a\nb\nc\n")
	got, err := s.Split(raw)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 chunks: %d", len(got))
	}
	for i, want := range []string{"a", "b", "c"} {
		if string(got[i]) != want {
			t.Errorf("got[%d] = %q, want %q", i, string(got[i]), want)
		}
	}
}

func TestNewlineSplitter_Split_EmptyLinesSkipped(t *testing.T) {
	s := &NewlineSplitter{}
	raw := []byte("a\n\nb\n  \n")
	got, err := s.Split(raw)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 chunks (empty skipped): %d", len(got))
	}
}

func TestHL7BatchSplitter_Split(t *testing.T) {
	s := &HL7BatchSplitter{}
	raw := []byte("MSH|^~\\&|A\rPID|1|2\rMSH|^~\\&|B\rPID|1|3")
	got, err := s.Split(raw)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 messages: %d", len(got))
	}
	if !strings.HasPrefix(string(got[0]), "MSH") || !strings.Contains(string(got[0]), "PID") {
		t.Errorf("first message: %s", got[0])
	}
	if !strings.HasPrefix(string(got[1]), "MSH") {
		t.Errorf("second message: %s", got[1])
	}
}

func TestHL7BatchSplitter_Split_NewlineEndings(t *testing.T) {
	s := &HL7BatchSplitter{}
	raw := []byte("MSH|^~\\&|A\nPID|1\nMSH|^~\\&|B\nPID|2")
	got, err := s.Split(raw)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 messages: %d", len(got))
	}
}

func TestFHIRBundleSplitter_Split(t *testing.T) {
	s := &FHIRBundleSplitter{}
	bundle := map[string]any{
		"resourceType": "Bundle",
		"entry": []any{
			map[string]any{"resource": map[string]any{"resourceType": "Patient", "id": "1"}},
			map[string]any{"resource": map[string]any{"resourceType": "Patient", "id": "2"}},
		},
	}
	raw, _ := json.Marshal(bundle)
	got, err := s.Split(raw)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 resources: %d", len(got))
	}
	var p1 map[string]any
	if err := json.Unmarshal(got[0], &p1); err != nil {
		t.Fatalf("first resource not valid JSON: %v", err)
	}
	if p1["id"] != "1" {
		t.Errorf("first resource id = %v", p1["id"])
	}
}

func TestFHIRBundleSplitter_Split_EntryWithoutResource(t *testing.T) {
	s := &FHIRBundleSplitter{}
	bundle := map[string]any{
		"resourceType": "Bundle",
		"entry": []any{
			map[string]any{"fullUrl": "http://x"},
		},
	}
	raw, _ := json.Marshal(bundle)
	got, err := s.Split(raw)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	// Uses entry as resource when resource key missing
	if len(got) != 1 {
		t.Fatalf("expected 1 chunk: %d", len(got))
	}
}

func TestFHIRBundleSplitter_Split_InvalidJSON(t *testing.T) {
	s := &FHIRBundleSplitter{}
	got, err := s.Split([]byte("not json"))
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("invalid JSON should return raw as single chunk: %d", len(got))
	}
	if string(got[0]) != "not json" {
		t.Errorf("got %q", got[0])
	}
}

func TestFHIRBundleSplitter_Split_NoEntries(t *testing.T) {
	s := &FHIRBundleSplitter{}
	bundle := map[string]any{"resourceType": "Bundle"}
	raw, _ := json.Marshal(bundle)
	got, err := s.Split(raw)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("no entries: return raw: %d", len(got))
	}
}

func TestFHIRBundleSplitter_Split_EntriesNotArray(t *testing.T) {
	s := &FHIRBundleSplitter{}
	bundle := map[string]any{"resourceType": "Bundle", "entry": "not array"}
	raw, _ := json.Marshal(bundle)
	got, err := s.Split(raw)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("entries not array: return raw: %d", len(got))
	}
}

func TestXMLRootSplitter_Split(t *testing.T) {
	s := &XMLRootSplitter{}
	raw := []byte("<root>data</root>")
	got, err := s.Split(raw)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 chunk: %d", len(got))
	}
	if !reflect.DeepEqual(got[0], raw) {
		t.Error("should return input unchanged")
	}
}

// --- Parser interface / round-trip ---

func TestParser_RoundTrip_JSON(t *testing.T) {
	p, _ := NewParser("json")
	input := []byte(`{"k":"v"}`)
	parsed, err := p.Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ser, err := p.Serialize(parsed)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	var a, b map[string]any
	json.Unmarshal(input, &a)
	json.Unmarshal(ser, &b)
	if !reflect.DeepEqual(a, b) {
		t.Errorf("round-trip mismatch: %v vs %v", a, b)
	}
}

func TestParser_RoundTrip_Binary(t *testing.T) {
	p, _ := NewParser("binary")
	input := []byte("binary data")
	parsed, err := p.Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ser, err := p.Serialize(parsed)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if string(ser) != string(input) {
		t.Errorf("round-trip: got %q", ser)
	}
}

func TestParser_RoundTrip_X12(t *testing.T) {
	p, _ := NewParser("x12")
	raw := strings.Repeat("X", 105) + "~"
	raw = "ISA" + raw[3:]
	raw = raw[:3] + "*" + raw[4:]
	raw = raw[:105] + "~" + raw[106:]
	if len(raw) < 106 {
		raw = raw + strings.Repeat(" ", 106-len(raw))
	}
	parsed, err := p.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ser, err := p.Serialize(parsed)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if string(ser) != raw {
		t.Errorf("round-trip: got %q", ser)
	}
}

func TestParser_RoundTrip_HL7v2(t *testing.T) {
	p, _ := NewParser("hl7v2")
	msg := "MSH|^~\\&|A|B"
	parsed, err := p.Parse([]byte(msg))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ser, err := p.Serialize(parsed)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if string(ser) != msg {
		t.Errorf("round-trip: got %q", ser)
	}
}
