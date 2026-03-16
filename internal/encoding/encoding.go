package encoding

import (
	"bytes"
	"fmt"
	"io"
	"mime"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

var (
	utf8BOM    = []byte{0xEF, 0xBB, 0xBF}
	utf16LEBOM = []byte{0xFF, 0xFE}
	utf16BEBOM = []byte{0xFE, 0xFF}
)

// ExtractCharset parses the charset parameter from a Content-Type header value.
// Returns empty string if no charset is declared.
func ExtractCharset(contentType string) string {
	if contentType == "" {
		return ""
	}
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ""
	}
	cs := params["charset"]
	if cs == "" {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(cs))
}

// NormalizeCharset maps common charset aliases to canonical lowercase names.
func NormalizeCharset(charset string) string {
	cs := strings.ToLower(strings.TrimSpace(charset))
	switch cs {
	case "", "utf-8", "utf8":
		return ""
	case "latin1", "latin-1", "iso_8859-1", "iso8859-1", "iso-8859-1":
		return "iso-8859-1"
	case "windows-1252", "cp1252", "win1252":
		return "windows-1252"
	case "utf-16", "utf16":
		return "utf-16"
	case "utf-16le", "utf16le":
		return "utf-16le"
	case "utf-16be", "utf16be":
		return "utf-16be"
	default:
		return cs
	}
}

// DetectBOM inspects the leading bytes for a Unicode BOM and returns the
// detected charset and number of BOM bytes to skip. Returns ("", 0) if
// no BOM is found.
func DetectBOM(data []byte) (charset string, bomLen int) {
	if bytes.HasPrefix(data, utf8BOM) {
		return "", len(utf8BOM) // UTF-8 BOM: just strip it
	}
	if bytes.HasPrefix(data, utf16BEBOM) {
		return "utf-16be", len(utf16BEBOM)
	}
	if bytes.HasPrefix(data, utf16LEBOM) {
		return "utf-16le", len(utf16LEBOM)
	}
	return "", 0
}

// ToUTF8 transcodes data from the given charset to UTF-8.
// If charset is empty or "utf-8", the data is returned with the BOM stripped
// (if present) but otherwise unchanged. Invalid UTF-8 sequences in already-
// UTF-8 data are replaced with U+FFFD to ensure valid output.
func ToUTF8(data []byte, charset string) ([]byte, error) {
	charset = NormalizeCharset(charset)

	// Detect BOM if no charset declared
	if charset == "" {
		bomCharset, bomLen := DetectBOM(data)
		if bomCharset != "" {
			charset = bomCharset
			data = data[bomLen:]
		} else if bomLen > 0 {
			data = data[bomLen:] // UTF-8 BOM: strip it
		}
	} else {
		// Strip BOM even if charset is declared (sender may include both)
		_, bomLen := DetectBOM(data)
		if bomLen > 0 {
			data = data[bomLen:]
		}
	}

	if charset == "" {
		if utf8.Valid(data) {
			return data, nil
		}
		// Best-effort: assume Windows-1252 for invalid UTF-8 (most common in
		// healthcare systems sending "Latin" data without declaring charset)
		charset = "windows-1252"
	}

	// Handle UTF-16 variants
	if charset == "utf-16" || charset == "utf-16le" || charset == "utf-16be" {
		return transcodeUTF16(data, charset)
	}

	enc, err := htmlindex.Get(charset)
	if err != nil {
		return nil, fmt.Errorf("unsupported charset %q: %w", charset, err)
	}
	decoder := enc.NewDecoder()
	out, err := io.ReadAll(transform.NewReader(bytes.NewReader(data), decoder))
	if err != nil {
		return nil, fmt.Errorf("transcode from %q: %w", charset, err)
	}
	return out, nil
}

func transcodeUTF16(data []byte, variant string) ([]byte, error) {
	var enc encoding.Encoding
	switch variant {
	case "utf-16be":
		enc = unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM)
	case "utf-16le":
		enc = unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM)
	default:
		enc = unicode.UTF16(unicode.LittleEndian, unicode.UseBOM)
	}
	decoder := enc.NewDecoder()
	out, err := io.ReadAll(transform.NewReader(bytes.NewReader(data), decoder))
	if err != nil {
		return nil, fmt.Errorf("transcode from %q: %w", variant, err)
	}
	return out, nil
}
