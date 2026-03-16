package encoding

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestExtractCharset_InvalidContentType(t *testing.T) {
	got := ExtractCharset("not a valid content-type;;;")
	if got != "" {
		t.Fatalf("expected empty for invalid content type, got %q", got)
	}
}

func TestExtractCharset_CharsetWithWhitespace(t *testing.T) {
	got := ExtractCharset("text/html; charset= UTF-8 ")
	if got != "utf-8" {
		t.Fatalf("expected utf-8, got %q", got)
	}
}

func TestExtractCharset_MultipleParams(t *testing.T) {
	got := ExtractCharset("multipart/form-data; boundary=abc; charset=iso-8859-1")
	if got != "iso-8859-1" {
		t.Fatalf("expected iso-8859-1, got %q", got)
	}
}

func TestNormalizeCharset_ISO8859Aliases(t *testing.T) {
	aliases := []string{"iso_8859-1", "iso8859-1", "latin1", "latin-1", "LATIN1"}
	for _, a := range aliases {
		got := NormalizeCharset(a)
		if got != "iso-8859-1" {
			t.Errorf("NormalizeCharset(%q) = %q, want iso-8859-1", a, got)
		}
	}
}

func TestNormalizeCharset_WindowsAliases(t *testing.T) {
	aliases := []string{"cp1252", "win1252", "CP1252", "WIN1252"}
	for _, a := range aliases {
		got := NormalizeCharset(a)
		if got != "windows-1252" {
			t.Errorf("NormalizeCharset(%q) = %q, want windows-1252", a, got)
		}
	}
}

func TestNormalizeCharset_UTF16Variants(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"utf-16", "utf-16"},
		{"utf16", "utf-16"},
		{"UTF-16", "utf-16"},
		{"utf-16le", "utf-16le"},
		{"utf16le", "utf-16le"},
		{"UTF-16LE", "utf-16le"},
		{"utf-16be", "utf-16be"},
		{"utf16be", "utf-16be"},
		{"UTF-16BE", "utf-16be"},
	}
	for _, tt := range tests {
		got := NormalizeCharset(tt.in)
		if got != tt.want {
			t.Errorf("NormalizeCharset(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNormalizeCharset_UnknownPassthrough(t *testing.T) {
	got := NormalizeCharset("euc-jp")
	if got != "euc-jp" {
		t.Fatalf("expected euc-jp passthrough, got %q", got)
	}
}

func TestNormalizeCharset_WhitespaceHandling(t *testing.T) {
	got := NormalizeCharset("  utf-8  ")
	if got != "" {
		t.Fatalf("expected empty for utf-8, got %q", got)
	}
}

func TestDetectBOM_UTF8BOM(t *testing.T) {
	data := append([]byte{0xEF, 0xBB, 0xBF}, []byte("content")...)
	cs, bl := DetectBOM(data)
	if cs != "" || bl != 3 {
		t.Fatalf("expected (\"\", 3) for UTF-8 BOM, got (%q, %d)", cs, bl)
	}
}

func TestDetectBOM_UTF16LEBOM(t *testing.T) {
	data := []byte{0xFF, 0xFE, 0x41, 0x00}
	cs, bl := DetectBOM(data)
	if cs != "utf-16le" || bl != 2 {
		t.Fatalf("expected (utf-16le, 2), got (%q, %d)", cs, bl)
	}
}

func TestDetectBOM_UTF16BEBOM(t *testing.T) {
	data := []byte{0xFE, 0xFF, 0x00, 0x41}
	cs, bl := DetectBOM(data)
	if cs != "utf-16be" || bl != 2 {
		t.Fatalf("expected (utf-16be, 2), got (%q, %d)", cs, bl)
	}
}

func TestDetectBOM_NoBOM(t *testing.T) {
	data := []byte("plain text")
	cs, bl := DetectBOM(data)
	if cs != "" || bl != 0 {
		t.Fatalf("expected (\"\", 0), got (%q, %d)", cs, bl)
	}
}

func TestDetectBOM_ShortData(t *testing.T) {
	data := []byte{0xEF}
	cs, bl := DetectBOM(data)
	if cs != "" || bl != 0 {
		t.Fatalf("expected no BOM for short data, got (%q, %d)", cs, bl)
	}
}

func TestToUTF8_EmptyCharset_ValidUTF8(t *testing.T) {
	data := []byte("hello world")
	got, err := ToUTF8(data, "")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("expected passthrough, got %q", string(got))
	}
}

func TestToUTF8_ExplicitUTF8(t *testing.T) {
	data := []byte("café")
	got, err := ToUTF8(data, "utf-8")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "café" {
		t.Fatalf("expected café, got %q", string(got))
	}
}

func TestToUTF8_UTF8Alias(t *testing.T) {
	data := []byte("test")
	got, err := ToUTF8(data, "utf8")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "test" {
		t.Fatalf("expected test, got %q", string(got))
	}
}

func TestToUTF8_UTF16BE(t *testing.T) {
	// "Hi" in UTF-16BE: H=0x00,0x48 i=0x00,0x69
	data := []byte{0x00, 0x48, 0x00, 0x69}
	got, err := ToUTF8(data, "utf-16be")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "Hi" {
		t.Fatalf("expected Hi, got %q", string(got))
	}
}

func TestToUTF8_UTF16Generic(t *testing.T) {
	// "AB" in UTF-16LE (no BOM): A=0x41,0x00 B=0x42,0x00
	data := []byte{0x41, 0x00, 0x42, 0x00}
	got, err := ToUTF8(data, "utf-16")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "AB" {
		t.Fatalf("expected AB, got %q", string(got))
	}
}

func TestToUTF8_UnsupportedCharset(t *testing.T) {
	_, err := ToUTF8([]byte("data"), "totally-fake-charset-xyz")
	if err == nil {
		t.Fatal("expected error for unsupported charset")
	}
	if !strings.Contains(err.Error(), "unsupported charset") {
		t.Fatalf("expected 'unsupported charset' in error, got: %v", err)
	}
}

func TestToUTF8_BOMWithDeclaredCharset(t *testing.T) {
	// UTF-8 BOM + declared charset=utf-8: should strip BOM
	data := append([]byte{0xEF, 0xBB, 0xBF}, []byte("text")...)
	got, err := ToUTF8(data, "utf-8")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "text" {
		t.Fatalf("expected text, got %q", string(got))
	}
}

func TestToUTF8_UTF16LEWithBOM(t *testing.T) {
	// UTF-16LE BOM + "A" in UTF-16LE
	data := []byte{0xFF, 0xFE, 0x41, 0x00}
	got, err := ToUTF8(data, "utf-16le")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "A" {
		t.Fatalf("expected A, got %q", string(got))
	}
}

func TestToUTF8_UTF16BEWithBOMAutoDetect(t *testing.T) {
	// No charset declared, UTF-16 BE BOM + "A"
	data := []byte{0xFE, 0xFF, 0x00, 0x41}
	got, err := ToUTF8(data, "")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "A" {
		t.Fatalf("expected A, got %q", string(got))
	}
}

func TestToUTF8_UTF16LEWithBOMAutoDetect(t *testing.T) {
	// No charset declared, UTF-16 LE BOM + "B"
	data := []byte{0xFF, 0xFE, 0x42, 0x00}
	got, err := ToUTF8(data, "")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "B" {
		t.Fatalf("expected B, got %q", string(got))
	}
}

func TestToUTF8_InvalidUTF8FallsBackToWindows1252(t *testing.T) {
	// 0x93 is left double quotation mark in Windows-1252 but invalid UTF-8
	data := []byte{0x93}
	got, err := ToUTF8(data, "")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if !utf8.Valid(got) {
		t.Fatal("expected valid UTF-8 output")
	}
	if string(got) != "\u201c" {
		t.Fatalf("expected left double quote, got %q", string(got))
	}
}

func TestToUTF8_EmptyInput(t *testing.T) {
	got, err := ToUTF8([]byte{}, "")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty output, got %q", string(got))
	}
}

func TestToUTF8_Latin1Alias(t *testing.T) {
	latin1 := []byte{0x63, 0x61, 0x66, 0xe9}
	got, err := ToUTF8(latin1, "latin1")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	if string(got) != "café" {
		t.Fatalf("expected café, got %q", string(got))
	}
}

func TestToUTF8_CP1252Alias(t *testing.T) {
	win1252 := []byte{0x93, 'h', 'i', 0x94}
	got, err := ToUTF8(win1252, "cp1252")
	if err != nil {
		t.Fatalf("ToUTF8 error: %v", err)
	}
	want := "\u201chi\u201d"
	if string(got) != want {
		t.Fatalf("expected %q, got %q", want, string(got))
	}
}

func TestTranscodeUTF16_LE(t *testing.T) {
	data := []byte{0x48, 0x00, 0x69, 0x00}
	got, err := transcodeUTF16(data, "utf-16le")
	if err != nil {
		t.Fatalf("transcodeUTF16 error: %v", err)
	}
	if string(got) != "Hi" {
		t.Fatalf("expected Hi, got %q", string(got))
	}
}

func TestTranscodeUTF16_BE(t *testing.T) {
	data := []byte{0x00, 0x48, 0x00, 0x69}
	got, err := transcodeUTF16(data, "utf-16be")
	if err != nil {
		t.Fatalf("transcodeUTF16 error: %v", err)
	}
	if string(got) != "Hi" {
		t.Fatalf("expected Hi, got %q", string(got))
	}
}

func TestTranscodeUTF16_Generic(t *testing.T) {
	// "utf-16" uses BOM detection; without BOM defaults to LE
	data := []byte{0x41, 0x00}
	got, err := transcodeUTF16(data, "utf-16")
	if err != nil {
		t.Fatalf("transcodeUTF16 error: %v", err)
	}
	if string(got) != "A" {
		t.Fatalf("expected A, got %q", string(got))
	}
}
