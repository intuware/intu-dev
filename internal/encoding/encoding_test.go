package encoding

import (
	"testing"
)

func TestExtractCharset(t *testing.T) {
	tests := []struct {
		ct   string
		want string
	}{
		{"text/csv; charset=iso-8859-1", "iso-8859-1"},
		{"text/csv; charset=UTF-8", "utf-8"},
		{"text/csv; charset=windows-1252", "windows-1252"},
		{"text/csv", ""},
		{"application/json", ""},
		{"", ""},
		{"text/html; charset=utf-8; boundary=something", "utf-8"},
	}
	for _, tt := range tests {
		got := ExtractCharset(tt.ct)
		if got != tt.want {
			t.Errorf("ExtractCharset(%q) = %q, want %q", tt.ct, got, tt.want)
		}
	}
}

func TestNormalizeCharset(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"utf-8", ""},
		{"UTF-8", ""},
		{"utf8", ""},
		{"iso-8859-1", "iso-8859-1"},
		{"Latin1", "iso-8859-1"},
		{"LATIN-1", "iso-8859-1"},
		{"windows-1252", "windows-1252"},
		{"CP1252", "windows-1252"},
		{"utf-16", "utf-16"},
		{"UTF-16LE", "utf-16le"},
		{"shift_jis", "shift_jis"},
	}
	for _, tt := range tests {
		got := NormalizeCharset(tt.in)
		if got != tt.want {
			t.Errorf("NormalizeCharset(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestDetectBOM(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		charset string
		bomLen  int
	}{
		{"UTF-8 BOM", []byte{0xEF, 0xBB, 0xBF, 'h', 'i'}, "", 3},
		{"UTF-16 BE BOM", []byte{0xFE, 0xFF, 0, 'A'}, "utf-16be", 2},
		{"UTF-16 LE BOM", []byte{0xFF, 0xFE, 'A', 0}, "utf-16le", 2},
		{"No BOM", []byte("hello"), "", 0},
		{"Empty", []byte{}, "", 0},
	}
	for _, tt := range tests {
		cs, bl := DetectBOM(tt.data)
		if cs != tt.charset || bl != tt.bomLen {
			t.Errorf("DetectBOM(%s) = (%q, %d), want (%q, %d)", tt.name, cs, bl, tt.charset, tt.bomLen)
		}
	}
}

func TestToUTF8_Latin1(t *testing.T) {
	// "café" in ISO-8859-1: c=0x63, a=0x61, f=0x66, é=0xe9
	latin1 := []byte{0x63, 0x61, 0x66, 0xe9}
	got, err := ToUTF8(latin1, "iso-8859-1")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	want := "café"
	if string(got) != want {
		t.Errorf("ToUTF8(latin1, iso-8859-1) = %q, want %q", string(got), want)
	}
}

func TestToUTF8_Windows1252(t *testing.T) {
	// Smart quotes in Windows-1252: left " = 0x93, right " = 0x94
	win1252 := []byte{0x93, 'h', 'i', 0x94}
	got, err := ToUTF8(win1252, "windows-1252")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	want := "\u201chi\u201d"
	if string(got) != want {
		t.Errorf("ToUTF8(win1252) = %q, want %q", string(got), want)
	}
}

func TestToUTF8_AlreadyUTF8(t *testing.T) {
	utf8Data := []byte("hello world café")
	got, err := ToUTF8(utf8Data, "")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != string(utf8Data) {
		t.Errorf("ToUTF8(utf8) = %q, want %q", string(got), string(utf8Data))
	}
}

func TestToUTF8_UTF8BOM(t *testing.T) {
	data := append([]byte{0xEF, 0xBB, 0xBF}, []byte("hello")...)
	got, err := ToUTF8(data, "")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("ToUTF8(bom+hello) = %q, want %q", string(got), "hello")
	}
}

func TestToUTF8_InvalidUTF8_NoCharset(t *testing.T) {
	// 0xe9 alone is invalid UTF-8; should fallback to Windows-1252 decoding
	data := []byte{0x63, 0x61, 0x66, 0xe9}
	got, err := ToUTF8(data, "")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	want := "café"
	if string(got) != want {
		t.Errorf("ToUTF8(invalid-utf8, no charset) = %q, want %q", string(got), want)
	}
}

func TestToUTF8_UTF16LE(t *testing.T) {
	// "Hi" in UTF-16LE: H=0x48,0x00 i=0x69,0x00
	data := []byte{0x48, 0x00, 0x69, 0x00}
	got, err := ToUTF8(data, "utf-16le")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "Hi" {
		t.Errorf("ToUTF8(utf16le) = %q, want %q", string(got), "Hi")
	}
}

func TestToUTF8_CSVWithLatin1(t *testing.T) {
	// Simulates a CSV with Latin-1 encoded accented characters
	csv := []byte("name,city\nJos\xe9,Montr\xe9al\n")
	got, err := ToUTF8(csv, "iso-8859-1")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	want := "name,city\nJos\u00e9,Montr\u00e9al\n"
	if string(got) != want {
		t.Errorf("ToUTF8(csv-latin1) = %q, want %q", string(got), want)
	}
}
