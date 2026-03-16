package runtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

func TestExecutePostprocessor_NoConfig(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config:    &config.ChannelConfig{},
		logger:    discardLogger(),
	}

	err := p.ExecutePostprocessor(context.Background(), message.New("ch1", []byte("data")), "output", nil)
	if err != nil {
		t.Errorf("expected nil for no postprocessor, got %v", err)
	}
}

func TestExecutePostprocessor_NilPipeline(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config:    &config.ChannelConfig{Pipeline: nil},
		logger:    discardLogger(),
	}

	err := p.ExecutePostprocessor(context.Background(), message.New("ch1", []byte("data")), "output", nil)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestExecutePostprocessor_EmptyPostprocessor(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config: &config.ChannelConfig{
			Pipeline: &config.PipelineConfig{Postprocessor: ""},
		},
		logger: discardLogger(),
	}

	err := p.ExecutePostprocessor(context.Background(), message.New("ch1", []byte("data")), "output", nil)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestExecuteResponseTransformer_NoConfig(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config:    &config.ChannelConfig{},
		logger:    discardLogger(),
	}

	dest := config.ChannelDestination{Name: "d1"}
	resp := &message.Response{StatusCode: 200, Body: []byte("ok")}
	err := p.ExecuteResponseTransformer(context.Background(), message.New("ch1", []byte("data")), dest, resp)
	if err != nil {
		t.Errorf("expected nil for no response transformer, got %v", err)
	}
}

func TestExecuteResponseTransformer_NilResponseTransformer(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config:    &config.ChannelConfig{},
		logger:    discardLogger(),
	}

	dest := config.ChannelDestination{
		Name:                "d1",
		ResponseTransformer: nil,
	}
	resp := &message.Response{StatusCode: 200}
	err := p.ExecuteResponseTransformer(context.Background(), message.New("ch1", []byte("data")), dest, resp)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestExecuteResponseTransformer_EmptyEntrypoint(t *testing.T) {
	p := &Pipeline{
		channelID: "ch-1",
		config:    &config.ChannelConfig{},
		logger:    discardLogger(),
	}

	dest := config.ChannelDestination{
		Name:                "d1",
		ResponseTransformer: &config.ScriptRef{Entrypoint: ""},
	}
	resp := &message.Response{StatusCode: 200}
	err := p.ExecuteResponseTransformer(context.Background(), message.New("ch1", []byte("data")), dest, resp)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestResolveDestTransformer(t *testing.T) {
	tests := []struct {
		name string
		dest config.ChannelDestination
		want string
	}{
		{
			name: "with entrypoint",
			dest: config.ChannelDestination{
				Transformer: &config.ScriptRef{Entrypoint: "transform.ts"},
			},
			want: "transform.ts",
		},
		{
			name: "nil transformer",
			dest: config.ChannelDestination{},
			want: "",
		},
		{
			name: "empty entrypoint",
			dest: config.ChannelDestination{
				Transformer: &config.ScriptRef{Entrypoint: ""},
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveDestTransformer(tt.dest)
			if got != tt.want {
				t.Errorf("resolveDestTransformer() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveDestResponseTransformer(t *testing.T) {
	tests := []struct {
		name string
		dest config.ChannelDestination
		want string
	}{
		{
			name: "with entrypoint",
			dest: config.ChannelDestination{
				ResponseTransformer: &config.ScriptRef{Entrypoint: "resp.ts"},
			},
			want: "resp.ts",
		},
		{
			name: "nil response transformer",
			dest: config.ChannelDestination{},
			want: "",
		},
		{
			name: "empty entrypoint",
			dest: config.ChannelDestination{
				ResponseTransformer: &config.ScriptRef{Entrypoint: ""},
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveDestResponseTransformer(tt.dest)
			if got != tt.want {
				t.Errorf("resolveDestResponseTransformer() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestToStringMap(t *testing.T) {
	tests := []struct {
		name string
		v    any
		want map[string]string
	}{
		{
			name: "nil input",
			v:    nil,
			want: map[string]string{},
		},
		{
			name: "non-map input",
			v:    "not a map",
			want: map[string]string{},
		},
		{
			name: "string values",
			v:    map[string]any{"a": "hello", "b": "world"},
			want: map[string]string{"a": "hello", "b": "world"},
		},
		{
			name: "non-string values formatted with Sprintf",
			v:    map[string]any{"num": 42, "flag": true, "pi": 3.14},
			want: map[string]string{"num": "42", "flag": "true", "pi": "3.14"},
		},
		{
			name: "mixed values",
			v:    map[string]any{"name": "test", "count": 5},
			want: map[string]string{"name": "test", "count": "5"},
		},
		{
			name: "empty map",
			v:    map[string]any{},
			want: map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toStringMap(tt.v)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toStringMap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToStringSlice(t *testing.T) {
	tests := []struct {
		name string
		v    any
		want []string
	}{
		{
			name: "string slice",
			v:    []any{"a", "b", "c"},
			want: []string{"a", "b", "c"},
		},
		{
			name: "non-slice input",
			v:    "not a slice",
			want: nil,
		},
		{
			name: "nil input",
			v:    nil,
			want: nil,
		},
		{
			name: "mixed types filters non-strings",
			v:    []any{"a", 42, "b", true},
			want: []string{"a", "b"},
		},
		{
			name: "empty slice",
			v:    []any{},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toStringSlice(tt.v)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toStringSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNonNilMap(t *testing.T) {
	t.Run("nil map returns empty", func(t *testing.T) {
		got := nonNilMap(nil)
		if got == nil {
			t.Fatal("nonNilMap(nil) returned nil, want empty map")
		}
		if len(got) != 0 {
			t.Errorf("nonNilMap(nil) returned map with len %d, want 0", len(got))
		}
	})
	t.Run("non-nil map returned as-is", func(t *testing.T) {
		m := map[string]string{"key": "val"}
		got := nonNilMap(m)
		if !reflect.DeepEqual(got, m) {
			t.Errorf("nonNilMap() = %v, want %v", got, m)
		}
	})
}

func TestNonNilStrMap(t *testing.T) {
	t.Run("nil map returns empty", func(t *testing.T) {
		got := nonNilStrMap(nil)
		if got == nil {
			t.Fatal("nonNilStrMap(nil) returned nil, want empty map")
		}
		if len(got) != 0 {
			t.Errorf("nonNilStrMap(nil) returned map with len %d, want 0", len(got))
		}
	})
	t.Run("non-nil map returned as-is", func(t *testing.T) {
		m := map[string]string{"x": "y"}
		got := nonNilStrMap(m)
		if !reflect.DeepEqual(got, m) {
			t.Errorf("nonNilStrMap() = %v, want %v", got, m)
		}
	})
}

func TestCloneMessageShell(t *testing.T) {
	now := time.Now()
	orig := &message.Message{
		ID:            "msg-1",
		CorrelationID: "corr-1",
		ChannelID:     "ch-1",
		Raw:           []byte("original raw"),
		Transport:     "http",
		ContentType:   message.ContentTypeJSON,
		SourceCharset: "utf-8",
		HTTP: &message.HTTPMeta{
			Headers: map[string]string{"X-Test": "1"},
			Method:  "POST",
		},
		File:  &message.FileMeta{Filename: "test.txt", Directory: "/tmp"},
		FTP:   &message.FTPMeta{Filename: "ftp.txt", Directory: "/ftp"},
		Kafka: &message.KafkaMeta{Topic: "t1", Key: "k1", Partition: 2, Offset: 100},
		TCP:   &message.TCPMeta{RemoteAddr: "127.0.0.1:5000"},
		SMTP:  &message.SMTPMeta{From: "a@b.com", To: []string{"c@d.com"}, Subject: "Hi"},
		DICOM: &message.DICOMMeta{CallingAE: "AE1", CalledAE: "AE2"},
		Database: &message.DatabaseMeta{
			Query:  "SELECT 1",
			Params: map[string]any{"p": "v"},
		},
		Metadata:  map[string]any{"key": "val"},
		Timestamp: now,
	}

	clone := cloneMessageShell(orig)

	if clone.ID != orig.ID {
		t.Errorf("ID = %q, want %q", clone.ID, orig.ID)
	}
	if clone.CorrelationID != orig.CorrelationID {
		t.Errorf("CorrelationID = %q, want %q", clone.CorrelationID, orig.CorrelationID)
	}
	if clone.ChannelID != orig.ChannelID {
		t.Errorf("ChannelID = %q, want %q", clone.ChannelID, orig.ChannelID)
	}
	if clone.Transport != orig.Transport {
		t.Errorf("Transport = %q, want %q", clone.Transport, orig.Transport)
	}
	if clone.ContentType != orig.ContentType {
		t.Errorf("ContentType = %q, want %q", clone.ContentType, orig.ContentType)
	}
	if clone.SourceCharset != orig.SourceCharset {
		t.Errorf("SourceCharset = %q, want %q", clone.SourceCharset, orig.SourceCharset)
	}
	if clone.HTTP != orig.HTTP {
		t.Error("HTTP pointer not shared with original")
	}
	if clone.File != orig.File {
		t.Error("File pointer not shared with original")
	}
	if clone.Kafka != orig.Kafka {
		t.Error("Kafka pointer not shared with original")
	}
	if clone.TCP != orig.TCP {
		t.Error("TCP pointer not shared with original")
	}
	if clone.SMTP != orig.SMTP {
		t.Error("SMTP pointer not shared with original")
	}
	if clone.DICOM != orig.DICOM {
		t.Error("DICOM pointer not shared with original")
	}
	if clone.Database != orig.Database {
		t.Error("Database pointer not shared with original")
	}
	if !clone.Timestamp.Equal(orig.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", clone.Timestamp, orig.Timestamp)
	}

	// Raw must NOT be copied
	if clone.Raw != nil {
		t.Errorf("Raw should be nil in clone, got %v", clone.Raw)
	}
}

func TestParseHTTPMeta(t *testing.T) {
	t.Run("full data", func(t *testing.T) {
		data := map[string]any{
			"headers":     map[string]any{"Content-Type": "application/json"},
			"queryParams": map[string]any{"page": "1"},
			"pathParams":  map[string]any{"id": "42"},
			"method":      "POST",
			"statusCode":  float64(200),
		}
		meta := parseHTTPMeta(data)
		if meta.Headers["Content-Type"] != "application/json" {
			t.Errorf("Headers = %v", meta.Headers)
		}
		if meta.QueryParams["page"] != "1" {
			t.Errorf("QueryParams = %v", meta.QueryParams)
		}
		if meta.PathParams["id"] != "42" {
			t.Errorf("PathParams = %v", meta.PathParams)
		}
		if meta.Method != "POST" {
			t.Errorf("Method = %q, want POST", meta.Method)
		}
		if meta.StatusCode != 200 {
			t.Errorf("StatusCode = %d, want 200", meta.StatusCode)
		}
	})
	t.Run("empty data", func(t *testing.T) {
		meta := parseHTTPMeta(map[string]any{})
		if meta.Method != "" {
			t.Errorf("Method = %q, want empty", meta.Method)
		}
		if meta.StatusCode != 0 {
			t.Errorf("StatusCode = %d, want 0", meta.StatusCode)
		}
	})
}

func TestParseFileMeta(t *testing.T) {
	t.Run("full data", func(t *testing.T) {
		meta := parseFileMeta(map[string]any{
			"filename":  "report.csv",
			"directory": "/data/incoming",
		})
		if meta.Filename != "report.csv" {
			t.Errorf("Filename = %q", meta.Filename)
		}
		if meta.Directory != "/data/incoming" {
			t.Errorf("Directory = %q", meta.Directory)
		}
	})
	t.Run("empty data", func(t *testing.T) {
		meta := parseFileMeta(map[string]any{})
		if meta.Filename != "" || meta.Directory != "" {
			t.Errorf("expected empty fields, got %+v", meta)
		}
	})
}

func TestParseFTPMeta(t *testing.T) {
	meta := parseFTPMeta(map[string]any{
		"filename":  "upload.dat",
		"directory": "/remote/dir",
	})
	if meta.Filename != "upload.dat" {
		t.Errorf("Filename = %q", meta.Filename)
	}
	if meta.Directory != "/remote/dir" {
		t.Errorf("Directory = %q", meta.Directory)
	}
}

func TestParseKafkaMeta(t *testing.T) {
	t.Run("full data", func(t *testing.T) {
		meta := parseKafkaMeta(map[string]any{
			"headers":   map[string]any{"h1": "v1"},
			"topic":     "events",
			"key":       "k1",
			"partition": float64(3),
			"offset":    float64(999),
		})
		if meta.Headers["h1"] != "v1" {
			t.Errorf("Headers = %v", meta.Headers)
		}
		if meta.Topic != "events" {
			t.Errorf("Topic = %q", meta.Topic)
		}
		if meta.Key != "k1" {
			t.Errorf("Key = %q", meta.Key)
		}
		if meta.Partition != 3 {
			t.Errorf("Partition = %d", meta.Partition)
		}
		if meta.Offset != 999 {
			t.Errorf("Offset = %d", meta.Offset)
		}
	})
	t.Run("empty data", func(t *testing.T) {
		meta := parseKafkaMeta(map[string]any{})
		if meta.Topic != "" {
			t.Errorf("Topic = %q, want empty", meta.Topic)
		}
	})
}

func TestParseTCPMeta(t *testing.T) {
	meta := parseTCPMeta(map[string]any{"remoteAddr": "10.0.0.1:8080"})
	if meta.RemoteAddr != "10.0.0.1:8080" {
		t.Errorf("RemoteAddr = %q", meta.RemoteAddr)
	}
	meta2 := parseTCPMeta(map[string]any{})
	if meta2.RemoteAddr != "" {
		t.Errorf("RemoteAddr = %q, want empty", meta2.RemoteAddr)
	}
}

func TestParseSMTPMeta(t *testing.T) {
	meta := parseSMTPMeta(map[string]any{
		"from":    "sender@test.com",
		"to":      []any{"a@b.com", "c@d.com"},
		"subject": "Test",
		"cc":      []any{"cc@test.com"},
		"bcc":     []any{"bcc@test.com"},
	})
	if meta.From != "sender@test.com" {
		t.Errorf("From = %q", meta.From)
	}
	if !reflect.DeepEqual(meta.To, []string{"a@b.com", "c@d.com"}) {
		t.Errorf("To = %v", meta.To)
	}
	if meta.Subject != "Test" {
		t.Errorf("Subject = %q", meta.Subject)
	}
	if !reflect.DeepEqual(meta.CC, []string{"cc@test.com"}) {
		t.Errorf("CC = %v", meta.CC)
	}
	if !reflect.DeepEqual(meta.BCC, []string{"bcc@test.com"}) {
		t.Errorf("BCC = %v", meta.BCC)
	}
}

func TestParseDICOMMeta(t *testing.T) {
	meta := parseDICOMMeta(map[string]any{
		"callingAE": "SCU",
		"calledAE":  "SCP",
	})
	if meta.CallingAE != "SCU" {
		t.Errorf("CallingAE = %q", meta.CallingAE)
	}
	if meta.CalledAE != "SCP" {
		t.Errorf("CalledAE = %q", meta.CalledAE)
	}
}

func TestParseDatabaseMeta(t *testing.T) {
	params := map[string]any{"id": float64(1), "name": "test"}
	meta := parseDatabaseMeta(map[string]any{
		"query":  "SELECT * FROM t WHERE id = :id",
		"params": params,
	})
	if meta.Query != "SELECT * FROM t WHERE id = :id" {
		t.Errorf("Query = %q", meta.Query)
	}
	if !reflect.DeepEqual(meta.Params, params) {
		t.Errorf("Params = %v", meta.Params)
	}

	// Empty data
	meta2 := parseDatabaseMeta(map[string]any{})
	if meta2.Query != "" {
		t.Errorf("Query should be empty, got %q", meta2.Query)
	}
	if meta2.Params != nil {
		t.Errorf("Params should be nil, got %v", meta2.Params)
	}
}

func TestIsValidUTF8(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{"valid ascii", []byte("hello world"), true},
		{"valid utf8 multibyte", []byte("Ünïcödé"), true},
		{"empty", []byte{}, true},
		{"invalid byte", []byte{0xff, 0xfe}, false},
		{"truncated sequence", []byte{0xc3}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isValidUTF8(tt.data); got != tt.want {
				t.Errorf("isValidUTF8() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnsureValidUTF8(t *testing.T) {
	t.Run("valid stays same", func(t *testing.T) {
		data := []byte("hello")
		got := ensureValidUTF8(data)
		if string(got) != "hello" {
			t.Errorf("ensureValidUTF8() = %q, want %q", got, "hello")
		}
	})
	t.Run("invalid gets replacement chars", func(t *testing.T) {
		data := []byte{0x48, 0x69, 0xff, 0x21} // Hi<invalid>!
		got := ensureValidUTF8(data)
		if !isValidUTF8(got) {
			t.Error("result should be valid UTF-8")
		}
		expected := "Hi\uFFFD!"
		if string(got) != expected {
			t.Errorf("ensureValidUTF8() = %q, want %q", string(got), expected)
		}
	})
	t.Run("all invalid bytes", func(t *testing.T) {
		data := []byte{0xff, 0xfe}
		got := ensureValidUTF8(data)
		if !isValidUTF8(got) {
			t.Error("result should be valid UTF-8")
		}
		expected := "\uFFFD\uFFFD"
		if string(got) != expected {
			t.Errorf("ensureValidUTF8() = %q, want %q", string(got), expected)
		}
	})
}

// Helper to create a minimal Pipeline for method tests.
func newTestPipeline(cfg *config.ChannelConfig) *Pipeline {
	if cfg == nil {
		cfg = &config.ChannelConfig{}
	}
	return &Pipeline{
		channelDir: "/project/channels/my-channel",
		projectDir: "/project",
		channelID:  "test-channel",
		config:     cfg,
		logger:     slog.Default(),
		plugins:    NewPluginRegistry(),
	}
}

func TestPipeline_toBytes(t *testing.T) {
	p := newTestPipeline(nil)

	t.Run("[]byte passthrough", func(t *testing.T) {
		in := []byte("raw bytes")
		got := p.toBytes(in)
		if string(got) != "raw bytes" {
			t.Errorf("toBytes([]byte) = %q", got)
		}
	})
	t.Run("string conversion", func(t *testing.T) {
		got := p.toBytes("hello")
		if string(got) != "hello" {
			t.Errorf("toBytes(string) = %q", got)
		}
	})
	t.Run("json marshal for other types", func(t *testing.T) {
		got := p.toBytes(map[string]string{"a": "b"})
		var m map[string]string
		if err := json.Unmarshal(got, &m); err != nil {
			t.Fatalf("toBytes(map) produced invalid JSON: %v", err)
		}
		if m["a"] != "b" {
			t.Errorf("unexpected JSON: %s", got)
		}
	})
	t.Run("unmarshalable falls back to Sprintf", func(t *testing.T) {
		ch := make(chan int)
		got := p.toBytes(ch)
		if len(got) == 0 {
			t.Error("toBytes(chan) returned empty")
		}
	})
}

func TestPipeline_resolveScriptPath(t *testing.T) {
	p := &Pipeline{
		channelDir: "/project/channels/my-channel",
		projectDir: "/project",
	}

	t.Run("ts file resolves to dist", func(t *testing.T) {
		got := p.resolveScriptPath("transform.ts")
		want := filepath.Join("/project", "dist", "channels/my-channel", "transform.js")
		if got != want {
			t.Errorf("resolveScriptPath(ts) = %q, want %q", got, want)
		}
	})
	t.Run("js file resolves to channelDir", func(t *testing.T) {
		got := p.resolveScriptPath("helper.js")
		want := filepath.Join("/project/channels/my-channel", "helper.js")
		if got != want {
			t.Errorf("resolveScriptPath(js) = %q, want %q", got, want)
		}
	})
	t.Run("other extension resolves to channelDir", func(t *testing.T) {
		got := p.resolveScriptPath("data.json")
		want := filepath.Join("/project/channels/my-channel", "data.json")
		if got != want {
			t.Errorf("resolveScriptPath(json) = %q, want %q", got, want)
		}
	})
}

func TestPipeline_resolveValidator(t *testing.T) {
	t.Run("from pipeline config", func(t *testing.T) {
		p := newTestPipeline(&config.ChannelConfig{
			Pipeline: &config.PipelineConfig{Validator: "validator.ts"},
		})
		if got := p.resolveValidator(); got != "validator.ts" {
			t.Errorf("resolveValidator() = %q, want %q", got, "validator.ts")
		}
	})
	t.Run("from Validator ScriptRef", func(t *testing.T) {
		p := newTestPipeline(&config.ChannelConfig{
			Validator: &config.ScriptRef{Entrypoint: "validate.js"},
		})
		if got := p.resolveValidator(); got != "validate.js" {
			t.Errorf("resolveValidator() = %q, want %q", got, "validate.js")
		}
	})
	t.Run("pipeline config takes precedence", func(t *testing.T) {
		p := newTestPipeline(&config.ChannelConfig{
			Pipeline:  &config.PipelineConfig{Validator: "pipeline-val.ts"},
			Validator: &config.ScriptRef{Entrypoint: "ref-val.ts"},
		})
		if got := p.resolveValidator(); got != "pipeline-val.ts" {
			t.Errorf("resolveValidator() = %q, want %q", got, "pipeline-val.ts")
		}
	})
	t.Run("empty when nothing configured", func(t *testing.T) {
		p := newTestPipeline(&config.ChannelConfig{})
		if got := p.resolveValidator(); got != "" {
			t.Errorf("resolveValidator() = %q, want empty", got)
		}
	})
}

func TestPipeline_resolveTransformer(t *testing.T) {
	t.Run("from pipeline config", func(t *testing.T) {
		p := newTestPipeline(&config.ChannelConfig{
			Pipeline: &config.PipelineConfig{Transformer: "transform.ts"},
		})
		if got := p.resolveTransformer(); got != "transform.ts" {
			t.Errorf("resolveTransformer() = %q, want %q", got, "transform.ts")
		}
	})
	t.Run("from Transformer ScriptRef", func(t *testing.T) {
		p := newTestPipeline(&config.ChannelConfig{
			Transformer: &config.ScriptRef{Entrypoint: "tx.js"},
		})
		if got := p.resolveTransformer(); got != "tx.js" {
			t.Errorf("resolveTransformer() = %q, want %q", got, "tx.js")
		}
	})
	t.Run("pipeline config takes precedence", func(t *testing.T) {
		p := newTestPipeline(&config.ChannelConfig{
			Pipeline:    &config.PipelineConfig{Transformer: "p-tx.ts"},
			Transformer: &config.ScriptRef{Entrypoint: "r-tx.ts"},
		})
		if got := p.resolveTransformer(); got != "p-tx.ts" {
			t.Errorf("resolveTransformer() = %q, want %q", got, "p-tx.ts")
		}
	})
	t.Run("empty when nothing configured", func(t *testing.T) {
		p := newTestPipeline(&config.ChannelConfig{})
		if got := p.resolveTransformer(); got != "" {
			t.Errorf("resolveTransformer() = %q, want empty", got)
		}
	})
}

func TestPipeline_buildIntuMessage(t *testing.T) {
	now := time.Now()
	t.Run("http transport", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{
			ID: "1", Transport: "http", ContentType: message.ContentTypeJSON,
			HTTP: &message.HTTPMeta{
				Headers:     map[string]string{"X-H": "v"},
				QueryParams: map[string]string{"q": "1"},
				PathParams:  map[string]string{"id": "42"},
				Method:      "GET",
				StatusCode:  200,
			},
			Timestamp: now,
		}
		im := p.buildIntuMessage(msg, "body text")
		if im["body"] != "body text" {
			t.Errorf("body = %v", im["body"])
		}
		if im["transport"] != "http" {
			t.Errorf("transport = %v", im["transport"])
		}
		http, ok := im["http"].(map[string]any)
		if !ok {
			t.Fatal("missing http field")
		}
		headers := http["headers"].(map[string]string)
		if headers["X-H"] != "v" {
			t.Errorf("headers = %v", headers)
		}
		if http["method"] != "GET" {
			t.Errorf("method = %v", http["method"])
		}
		if http["statusCode"] != 200 {
			t.Errorf("statusCode = %v", http["statusCode"])
		}
	})

	t.Run("file transport", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{
			Transport:   "file",
			ContentType: message.ContentTypeRaw,
			File:        &message.FileMeta{Filename: "f.txt", Directory: "/d"},
			Timestamp:   now,
		}
		im := p.buildIntuMessage(msg, "data")
		fdata := im["file"].(map[string]any)
		if fdata["filename"] != "f.txt" || fdata["directory"] != "/d" {
			t.Errorf("file = %v", fdata)
		}
	})

	t.Run("ftp transport", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{
			Transport:   "ftp",
			ContentType: message.ContentTypeRaw,
			FTP:         &message.FTPMeta{Filename: "up.dat", Directory: "/remote"},
			Timestamp:   now,
		}
		im := p.buildIntuMessage(msg, "data")
		fdata := im["ftp"].(map[string]any)
		if fdata["filename"] != "up.dat" {
			t.Errorf("ftp = %v", fdata)
		}
	})

	t.Run("kafka transport", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{
			Transport:   "kafka",
			ContentType: message.ContentTypeRaw,
			Kafka: &message.KafkaMeta{
				Headers: map[string]string{"kh": "kv"}, Topic: "t", Key: "k", Partition: 1, Offset: 50,
			},
			Timestamp: now,
		}
		im := p.buildIntuMessage(msg, "data")
		kdata := im["kafka"].(map[string]any)
		if kdata["topic"] != "t" {
			t.Errorf("kafka = %v", kdata)
		}
	})

	t.Run("tcp transport", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{
			Transport:   "tcp",
			ContentType: message.ContentTypeRaw,
			TCP:         &message.TCPMeta{RemoteAddr: "1.2.3.4:80"},
			Timestamp:   now,
		}
		im := p.buildIntuMessage(msg, "data")
		tdata := im["tcp"].(map[string]any)
		if tdata["remoteAddr"] != "1.2.3.4:80" {
			t.Errorf("tcp = %v", tdata)
		}
	})

	t.Run("smtp transport", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{
			Transport:   "smtp",
			ContentType: message.ContentTypeRaw,
			SMTP: &message.SMTPMeta{
				From: "a@b.com", To: []string{"x@y.com"}, Subject: "S",
				CC: []string{"cc@c.com"}, BCC: []string{"bcc@b.com"},
			},
			Timestamp: now,
		}
		im := p.buildIntuMessage(msg, "data")
		sdata := im["smtp"].(map[string]any)
		if sdata["from"] != "a@b.com" {
			t.Errorf("smtp from = %v", sdata["from"])
		}
	})

	t.Run("dicom transport", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{
			Transport:   "dicom",
			ContentType: message.ContentTypeRaw,
			DICOM:       &message.DICOMMeta{CallingAE: "A", CalledAE: "B"},
			Timestamp:   now,
		}
		im := p.buildIntuMessage(msg, "data")
		ddata := im["dicom"].(map[string]any)
		if ddata["callingAE"] != "A" || ddata["calledAE"] != "B" {
			t.Errorf("dicom = %v", ddata)
		}
	})

	t.Run("database transport", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{
			Transport:   "database",
			ContentType: message.ContentTypeRaw,
			Database:    &message.DatabaseMeta{Query: "SELECT 1", Params: map[string]any{"a": 1}},
			Timestamp:   now,
		}
		im := p.buildIntuMessage(msg, "data")
		dbdata := im["database"].(map[string]any)
		if dbdata["query"] != "SELECT 1" {
			t.Errorf("database = %v", dbdata)
		}
	})

	t.Run("sourceCharset set", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{
			Transport:     "http",
			ContentType:   message.ContentTypeRaw,
			SourceCharset: "iso-8859-1",
			Timestamp:     now,
		}
		im := p.buildIntuMessage(msg, "data")
		if im["sourceCharset"] != "iso-8859-1" {
			t.Errorf("sourceCharset = %v", im["sourceCharset"])
		}
	})

	t.Run("sourceCharset omitted when empty", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{Transport: "http", ContentType: message.ContentTypeRaw, Timestamp: now}
		im := p.buildIntuMessage(msg, "data")
		if _, exists := im["sourceCharset"]; exists {
			t.Error("sourceCharset should not be in map when empty")
		}
	})

	t.Run("metadata set", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{
			Transport:   "http",
			ContentType: message.ContentTypeRaw,
			Metadata:    map[string]any{"env": "test"},
			Timestamp:   now,
		}
		im := p.buildIntuMessage(msg, "data")
		md := im["metadata"].(map[string]any)
		if md["env"] != "test" {
			t.Errorf("metadata = %v", md)
		}
	})

	t.Run("metadata omitted when empty", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{Transport: "http", ContentType: message.ContentTypeRaw, Timestamp: now}
		im := p.buildIntuMessage(msg, "data")
		if _, exists := im["metadata"]; exists {
			t.Error("metadata should not be in map when empty")
		}
	})

	t.Run("nil HTTP headers become empty map", func(t *testing.T) {
		p := newTestPipeline(nil)
		msg := &message.Message{
			Transport:   "http",
			ContentType: message.ContentTypeRaw,
			HTTP:        &message.HTTPMeta{Method: "GET"},
			Timestamp:   now,
		}
		im := p.buildIntuMessage(msg, "body")
		http := im["http"].(map[string]any)
		headers := http["headers"].(map[string]string)
		if headers == nil {
			t.Error("headers should be non-nil empty map")
		}
	})
}

func TestPipeline_buildPipelineCtx(t *testing.T) {
	now := time.Now()
	msg := &message.Message{
		ID:            "m1",
		CorrelationID: "c1",
		Timestamp:     now,
	}

	t.Run("without maps", func(t *testing.T) {
		p := newTestPipeline(nil)
		ctx := p.buildPipelineCtx(msg, "transformer")
		if ctx["channelId"] != "test-channel" {
			t.Errorf("channelId = %v", ctx["channelId"])
		}
		if ctx["correlationId"] != "c1" {
			t.Errorf("correlationId = %v", ctx["correlationId"])
		}
		if ctx["messageId"] != "m1" {
			t.Errorf("messageId = %v", ctx["messageId"])
		}
		if ctx["stage"] != "transformer" {
			t.Errorf("stage = %v", ctx["stage"])
		}
		ts := ctx["timestamp"].(string)
		if ts == "" {
			t.Error("timestamp should be set")
		}
		if _, exists := ctx["globalMap"]; exists {
			t.Error("globalMap should not be set without maps")
		}
		if _, exists := ctx["connectorMap"]; exists {
			t.Error("connectorMap should not be set without connectorMap")
		}
	})

	t.Run("with maps and connectorMap", func(t *testing.T) {
		p := newTestPipeline(nil)
		maps := NewMapVariables()
		maps.GlobalMap().Put("gk", "gv")
		maps.ChannelMap("test-channel").Put("ck", "cv")
		maps.ResponseMap("test-channel").Put("rk", "rv")
		cm := NewConnectorMap()
		cm.Put("cmk", "cmv")
		p.SetMapContext(maps, cm)

		ctx := p.buildPipelineCtx(msg, "validator")
		gm := ctx["globalMap"].(map[string]any)
		if gm["gk"] != "gv" {
			t.Errorf("globalMap = %v", gm)
		}
		chm := ctx["channelMap"].(map[string]any)
		if chm["ck"] != "cv" {
			t.Errorf("channelMap = %v", chm)
		}
		rm := ctx["responseMap"].(map[string]any)
		if rm["rk"] != "rv" {
			t.Errorf("responseMap = %v", rm)
		}
		cmSnap := ctx["connectorMap"].(map[string]any)
		if cmSnap["cmk"] != "cmv" {
			t.Errorf("connectorMap = %v", cmSnap)
		}
	})
}

func TestPipeline_buildTransformCtx(t *testing.T) {
	now := time.Now()
	msg := &message.Message{ID: "m1", CorrelationID: "c1", Timestamp: now}

	t.Run("with DataTypes", func(t *testing.T) {
		p := newTestPipeline(&config.ChannelConfig{
			DataTypes: &config.DataTypesConfig{Inbound: "hl7v2", Outbound: "fhir_r4"},
		})
		ctx := p.buildTransformCtx(msg, "transformer")
		if ctx["inboundDataType"] != "hl7v2" {
			t.Errorf("inboundDataType = %v", ctx["inboundDataType"])
		}
		if ctx["outboundDataType"] != "fhir_r4" {
			t.Errorf("outboundDataType = %v", ctx["outboundDataType"])
		}
	})

	t.Run("without DataTypes", func(t *testing.T) {
		p := newTestPipeline(&config.ChannelConfig{})
		ctx := p.buildTransformCtx(msg, "transformer")
		if _, exists := ctx["inboundDataType"]; exists {
			t.Error("inboundDataType should not be set")
		}
	})
}

func TestPipeline_buildDestCtx(t *testing.T) {
	now := time.Now()
	msg := &message.Message{ID: "m1", CorrelationID: "c1", Timestamp: now}

	t.Run("with sourceIntuMsg", func(t *testing.T) {
		p := newTestPipeline(nil)
		src := map[string]any{"body": "hello"}
		ctx := p.buildDestCtx(msg, "dest-1", src, "destination_transformer")
		if ctx["destinationName"] != "dest-1" {
			t.Errorf("destinationName = %v", ctx["destinationName"])
		}
		if ctx["sourceMessage"] == nil {
			t.Error("sourceMessage should be set")
		}
	})

	t.Run("without sourceIntuMsg", func(t *testing.T) {
		p := newTestPipeline(nil)
		ctx := p.buildDestCtx(msg, "dest-2", nil, "destination_filter")
		if _, exists := ctx["sourceMessage"]; exists {
			t.Error("sourceMessage should not be set for nil input")
		}
		if ctx["destinationName"] != "dest-2" {
			t.Errorf("destinationName = %v", ctx["destinationName"])
		}
	})
}

func TestPipeline_buildDestIntuMessage(t *testing.T) {
	t.Run("http destination", func(t *testing.T) {
		p := newTestPipeline(&config.ChannelConfig{
			DataTypes: &config.DataTypesConfig{Outbound: "json"},
		})
		dest := config.ChannelDestination{
			Type: "http",
			HTTP: &config.HTTPDestConfig{
				URL:     "http://example.com",
				Method:  "POST",
				Headers: map[string]string{"Authorization": "Bearer tok"},
			},
		}
		im := p.buildDestIntuMessage("transformed body", "http-dest", dest)
		if im["body"] != "transformed body" {
			t.Errorf("body = %v", im["body"])
		}
		if im["transport"] != "http" {
			t.Errorf("transport = %v", im["transport"])
		}
		if im["contentType"] != "json" {
			t.Errorf("contentType = %v", im["contentType"])
		}
		http := im["http"].(map[string]any)
		if http["method"] != "POST" {
			t.Errorf("http method = %v", http["method"])
		}
	})

	t.Run("file destination", func(t *testing.T) {
		p := newTestPipeline(nil)
		dest := config.ChannelDestination{
			Type: "file",
			File: &config.FileDestConfig{
				Directory:       "/out",
				FilenamePattern: "msg_${date}.txt",
			},
		}
		im := p.buildDestIntuMessage("data", "file-dest", dest)
		if im["transport"] != "file" {
			t.Errorf("transport = %v", im["transport"])
		}
		fdata := im["file"].(map[string]any)
		if fdata["filename"] != "msg_${date}.txt" {
			t.Errorf("file filename = %v", fdata["filename"])
		}
		if fdata["directory"] != "/out" {
			t.Errorf("file directory = %v", fdata["directory"])
		}
	})

	t.Run("kafka destination", func(t *testing.T) {
		p := newTestPipeline(nil)
		dest := config.ChannelDestination{
			Type: "kafka",
			Kafka: &config.KafkaDestConfig{
				Topic: "out-topic",
			},
		}
		im := p.buildDestIntuMessage("data", "kafka-dest", dest)
		kdata := im["kafka"].(map[string]any)
		if kdata["topic"] != "out-topic" {
			t.Errorf("kafka topic = %v", kdata["topic"])
		}
	})

	t.Run("tcp destination", func(t *testing.T) {
		p := newTestPipeline(nil)
		dest := config.ChannelDestination{
			Type: "tcp",
			TCP:  &config.TCPDestConfig{Host: "10.0.0.1", Port: 6661},
		}
		im := p.buildDestIntuMessage("data", "tcp-dest", dest)
		tdata := im["tcp"].(map[string]any)
		if tdata["remoteAddr"] != "10.0.0.1:6661" {
			t.Errorf("tcp remoteAddr = %v", tdata["remoteAddr"])
		}
	})

	t.Run("smtp destination", func(t *testing.T) {
		p := newTestPipeline(nil)
		dest := config.ChannelDestination{
			Type: "smtp",
			SMTP: &config.SMTPDestConfig{
				Host: "mail.test.com", From: "a@b.com", To: []string{"c@d.com"}, Subject: "S",
			},
		}
		im := p.buildDestIntuMessage("data", "smtp-dest", dest)
		sdata := im["smtp"].(map[string]any)
		if sdata["from"] != "a@b.com" {
			t.Errorf("smtp from = %v", sdata["from"])
		}
	})

	t.Run("dicom destination", func(t *testing.T) {
		p := newTestPipeline(nil)
		dest := config.ChannelDestination{
			Type:  "dicom",
			DICOM: &config.DICOMDestConfig{AETitle: "MY_SCU", CalledAETitle: "REMOTE_SCP"},
		}
		im := p.buildDestIntuMessage("data", "dicom-dest", dest)
		ddata := im["dicom"].(map[string]any)
		if ddata["callingAE"] != "MY_SCU" {
			t.Errorf("dicom callingAE = %v", ddata["callingAE"])
		}
		if ddata["calledAE"] != "REMOTE_SCP" {
			t.Errorf("dicom calledAE = %v", ddata["calledAE"])
		}
	})

	t.Run("database destination", func(t *testing.T) {
		p := newTestPipeline(nil)
		dest := config.ChannelDestination{
			Type:     "database",
			Database: &config.DBDestConfig{Statement: "INSERT INTO t VALUES(:v)"},
		}
		im := p.buildDestIntuMessage("data", "db-dest", dest)
		dbdata := im["database"].(map[string]any)
		if dbdata["query"] != "INSERT INTO t VALUES(:v)" {
			t.Errorf("database query = %v", dbdata["query"])
		}
	})

	t.Run("fhir destination", func(t *testing.T) {
		p := newTestPipeline(nil)
		dest := config.ChannelDestination{
			Type: "fhir",
			FHIR: &config.FHIRDestConfig{BaseURL: "http://fhir.local"},
		}
		im := p.buildDestIntuMessage("data", "fhir-dest", dest)
		if im["transport"] != "fhir" {
			t.Errorf("transport = %v", im["transport"])
		}
		http := im["http"].(map[string]any)
		if http["method"] != "POST" {
			t.Errorf("fhir http method = %v", http["method"])
		}
	})

	t.Run("unknown type no protocol block", func(t *testing.T) {
		p := newTestPipeline(nil)
		dest := config.ChannelDestination{Type: "custom"}
		im := p.buildDestIntuMessage("data", "custom-dest", dest)
		if im["transport"] != "custom" {
			t.Errorf("transport = %v", im["transport"])
		}
		for _, key := range []string{"http", "file", "kafka", "tcp", "smtp", "dicom", "database"} {
			if _, exists := im[key]; exists {
				t.Errorf("unexpected key %q in im", key)
			}
		}
	})
}

func TestPipeline_resolveDestType(t *testing.T) {
	t.Run("explicit type", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{Type: "http"})
		if got != "http" {
			t.Errorf("resolveDestType() = %q, want %q", got, "http")
		}
	})

	t.Run("from resolvedDests", func(t *testing.T) {
		p := newTestPipeline(nil)
		p.SetResolvedDestinations(map[string]config.Destination{
			"d1": {Type: "kafka"},
		})
		got := p.resolveDestType("d1", config.ChannelDestination{})
		if got != "kafka" {
			t.Errorf("resolveDestType() = %q, want %q", got, "kafka")
		}
	})

	t.Run("inferred from HTTP config", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{
			HTTP: &config.HTTPDestConfig{URL: "http://x"},
		})
		if got != "http" {
			t.Errorf("resolveDestType() = %q", got)
		}
	})

	t.Run("inferred from File config", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{
			File: &config.FileDestConfig{Directory: "/tmp"},
		})
		if got != "file" {
			t.Errorf("resolveDestType() = %q", got)
		}
	})

	t.Run("inferred from Kafka config", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{
			Kafka: &config.KafkaDestConfig{Topic: "t"},
		})
		if got != "kafka" {
			t.Errorf("resolveDestType() = %q", got)
		}
	})

	t.Run("inferred from TCP config", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{
			TCP: &config.TCPDestConfig{Host: "h", Port: 1},
		})
		if got != "tcp" {
			t.Errorf("resolveDestType() = %q", got)
		}
	})

	t.Run("inferred from SMTP config", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{
			SMTP: &config.SMTPDestConfig{Host: "h"},
		})
		if got != "smtp" {
			t.Errorf("resolveDestType() = %q", got)
		}
	})

	t.Run("inferred from Database config", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{
			Database: &config.DBDestConfig{Statement: "s"},
		})
		if got != "database" {
			t.Errorf("resolveDestType() = %q", got)
		}
	})

	t.Run("inferred from DICOM config", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{
			DICOM: &config.DICOMDestConfig{Host: "h"},
		})
		if got != "dicom" {
			t.Errorf("resolveDestType() = %q", got)
		}
	})

	t.Run("inferred from Channel config", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{
			ChannelDest: &config.ChannelDestRef{TargetChannelID: "ch2"},
		})
		if got != "channel" {
			t.Errorf("resolveDestType() = %q", got)
		}
	})

	t.Run("inferred from FHIR config", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{
			FHIR: &config.FHIRDestConfig{BaseURL: "http://fhir"},
		})
		if got != "fhir" {
			t.Errorf("resolveDestType() = %q", got)
		}
	})

	t.Run("inferred from JMS config", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{
			JMS: &config.JMSDestConfig{URL: "tcp://broker"},
		})
		if got != "jms" {
			t.Errorf("resolveDestType() = %q", got)
		}
	})

	t.Run("inferred from Direct config", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{
			Direct: &config.DirectDestConfig{To: "addr@direct.example"},
		})
		if got != "direct" {
			t.Errorf("resolveDestType() = %q", got)
		}
	})

	t.Run("empty when nothing set", func(t *testing.T) {
		p := newTestPipeline(nil)
		got := p.resolveDestType("d1", config.ChannelDestination{})
		if got != "" {
			t.Errorf("resolveDestType() = %q, want empty", got)
		}
	})

	t.Run("explicit type takes precedence over resolvedDests", func(t *testing.T) {
		p := newTestPipeline(nil)
		p.SetResolvedDestinations(map[string]config.Destination{
			"d1": {Type: "kafka"},
		})
		got := p.resolveDestType("d1", config.ChannelDestination{Type: "http"})
		if got != "http" {
			t.Errorf("resolveDestType() = %q, want http", got)
		}
	})
}

func TestPipeline_getResolvedDest(t *testing.T) {
	t.Run("from resolvedDests map", func(t *testing.T) {
		p := newTestPipeline(nil)
		expected := config.Destination{Type: "kafka", Kafka: &config.KafkaDestConfig{Topic: "t1"}}
		p.SetResolvedDestinations(map[string]config.Destination{"d1": expected})
		got := p.getResolvedDest("d1", config.ChannelDestination{})
		if got.Type != "kafka" {
			t.Errorf("getResolvedDest() type = %q", got.Type)
		}
	})

	t.Run("fallback to ToDestination", func(t *testing.T) {
		p := newTestPipeline(nil)
		dest := config.ChannelDestination{
			Type: "file",
			File: &config.FileDestConfig{Directory: "/out", FilenamePattern: "f.txt"},
		}
		got := p.getResolvedDest("d2", dest)
		if got.Type != "file" {
			t.Errorf("getResolvedDest() type = %q", got.Type)
		}
		if got.File == nil || got.File.Directory != "/out" {
			t.Errorf("getResolvedDest() file = %+v", got.File)
		}
	})

	t.Run("nil resolvedDests falls back", func(t *testing.T) {
		p := newTestPipeline(nil)
		dest := config.ChannelDestination{Type: "tcp", TCP: &config.TCPDestConfig{Host: "h", Port: 1}}
		got := p.getResolvedDest("d3", dest)
		if got.Type != "tcp" {
			t.Errorf("getResolvedDest() type = %q", got.Type)
		}
	})
}

func TestPipeline_parseIntuResult(t *testing.T) {
	now := time.Now()
	orig := &message.Message{
		ID:            "orig-id",
		CorrelationID: "corr-1",
		ChannelID:     "ch-1",
		Transport:     "http",
		ContentType:   message.ContentTypeRaw,
		Timestamp:     now,
	}

	t.Run("non-map result", func(t *testing.T) {
		p := newTestPipeline(nil)
		r := p.parseIntuResult("just a string", orig)
		if r.Output != "just a string" {
			t.Errorf("Output = %v", r.Output)
		}
		if r.Msg.ID != orig.ID {
			t.Errorf("Msg.ID = %q", r.Msg.ID)
		}
	})

	t.Run("map without body", func(t *testing.T) {
		p := newTestPipeline(nil)
		r := p.parseIntuResult(map[string]any{"notBody": "val"}, orig)
		m := r.Output.(map[string]any)
		if m["notBody"] != "val" {
			t.Errorf("Output = %v", r.Output)
		}
		if r.Msg.ID != orig.ID {
			t.Errorf("Msg.ID = %q", r.Msg.ID)
		}
	})

	t.Run("map with body and all fields", func(t *testing.T) {
		p := newTestPipeline(nil)
		result := map[string]any{
			"body":          "transformed body",
			"contentType":   "json",
			"transport":     "kafka",
			"sourceCharset": "utf-16",
			"http": map[string]any{
				"headers":    map[string]any{"X-Out": "1"},
				"method":     "PUT",
				"statusCode": float64(201),
			},
			"file":     map[string]any{"filename": "out.csv", "directory": "/out"},
			"ftp":      map[string]any{"filename": "ftp.csv", "directory": "/ftp"},
			"kafka":    map[string]any{"topic": "t2", "key": "k2", "partition": float64(5), "offset": float64(200)},
			"tcp":      map[string]any{"remoteAddr": "5.6.7.8:9999"},
			"smtp":     map[string]any{"from": "x@y.com", "to": []any{"z@w.com"}, "subject": "Re"},
			"dicom":    map[string]any{"callingAE": "SCU2", "calledAE": "SCP2"},
			"database": map[string]any{"query": "UPDATE t SET x=1", "params": map[string]any{"id": float64(7)}},
			"metadata": map[string]any{"custom": "value"},
		}
		r := p.parseIntuResult(result, orig)
		if r.Output != "transformed body" {
			t.Errorf("Output = %v", r.Output)
		}
		if r.Msg.ContentType != message.ContentType("json") {
			t.Errorf("ContentType = %v", r.Msg.ContentType)
		}
		if r.Msg.Transport != "kafka" {
			t.Errorf("Transport = %v", r.Msg.Transport)
		}
		if r.Msg.SourceCharset != "utf-16" {
			t.Errorf("SourceCharset = %v", r.Msg.SourceCharset)
		}
		if r.Msg.HTTP == nil || r.Msg.HTTP.Method != "PUT" {
			t.Errorf("HTTP = %+v", r.Msg.HTTP)
		}
		if r.Msg.HTTP.StatusCode != 201 {
			t.Errorf("HTTP.StatusCode = %d", r.Msg.HTTP.StatusCode)
		}
		if r.Msg.File == nil || r.Msg.File.Filename != "out.csv" {
			t.Errorf("File = %+v", r.Msg.File)
		}
		if r.Msg.FTP == nil || r.Msg.FTP.Filename != "ftp.csv" {
			t.Errorf("FTP = %+v", r.Msg.FTP)
		}
		if r.Msg.Kafka == nil || r.Msg.Kafka.Topic != "t2" {
			t.Errorf("Kafka = %+v", r.Msg.Kafka)
		}
		if r.Msg.TCP == nil || r.Msg.TCP.RemoteAddr != "5.6.7.8:9999" {
			t.Errorf("TCP = %+v", r.Msg.TCP)
		}
		if r.Msg.SMTP == nil || r.Msg.SMTP.From != "x@y.com" {
			t.Errorf("SMTP = %+v", r.Msg.SMTP)
		}
		if r.Msg.DICOM == nil || r.Msg.DICOM.CallingAE != "SCU2" {
			t.Errorf("DICOM = %+v", r.Msg.DICOM)
		}
		if r.Msg.Database == nil || r.Msg.Database.Query != "UPDATE t SET x=1" {
			t.Errorf("Database = %+v", r.Msg.Database)
		}
		if r.Msg.Metadata["custom"] != "value" {
			t.Errorf("Metadata = %v", r.Msg.Metadata)
		}
	})

	t.Run("metadata merged into existing", func(t *testing.T) {
		p := newTestPipeline(nil)
		origWithMeta := &message.Message{
			ID:          "m-meta",
			Transport:   "http",
			ContentType: message.ContentTypeRaw,
			Metadata:    map[string]any{"existing": "yes"},
			Timestamp:   now,
		}
		result := map[string]any{
			"body":     "b",
			"metadata": map[string]any{"new": "added"},
		}
		r := p.parseIntuResult(result, origWithMeta)
		if r.Msg.Metadata["existing"] != "yes" {
			t.Errorf("existing metadata lost: %v", r.Msg.Metadata)
		}
		if r.Msg.Metadata["new"] != "added" {
			t.Errorf("new metadata not set: %v", r.Msg.Metadata)
		}
	})

	t.Run("nil result", func(t *testing.T) {
		p := newTestPipeline(nil)
		r := p.parseIntuResult(nil, orig)
		if r.Output != nil {
			t.Errorf("Output = %v, want nil", r.Output)
		}
		if r.Msg.ID != orig.ID {
			t.Errorf("Msg.ID = %q", r.Msg.ID)
		}
	})
}

func TestNewPipeline(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		cfg := &config.ChannelConfig{
			ID:      "ch-1",
			Enabled: true,
		}
		p := NewPipeline("/project/channels/ch-1", "/project", "ch-1", cfg, nil, slog.Default())
		if p.channelID != "ch-1" {
			t.Errorf("channelID = %q", p.channelID)
		}
		if p.channelDir != "/project/channels/ch-1" {
			t.Errorf("channelDir = %q", p.channelDir)
		}
		if p.projectDir != "/project" {
			t.Errorf("projectDir = %q", p.projectDir)
		}
		if p.config != cfg {
			t.Error("config not set correctly")
		}
		if p.parser == nil {
			t.Error("parser should not be nil")
		}
		if p.splitter != nil {
			t.Error("splitter should be nil without batch config")
		}
		if p.plugins == nil {
			t.Error("plugins registry should not be nil")
		}
	})

	t.Run("unsupported inbound type falls back to raw", func(t *testing.T) {
		cfg := &config.ChannelConfig{
			DataTypes: &config.DataTypesConfig{Inbound: "unsupported_format"},
		}
		p := NewPipeline("/tmp/ch", "/tmp", "ch-bad", cfg, nil, slog.Default())
		if p.parser == nil {
			t.Fatal("parser should not be nil after fallback")
		}
		parsed, err := p.parser.Parse([]byte("hello"))
		if err != nil {
			t.Fatalf("raw parser should not error: %v", err)
		}
		if parsed != "hello" {
			t.Errorf("raw parser returned %v", parsed)
		}
	})

	t.Run("batch splitter configured", func(t *testing.T) {
		cfg := &config.ChannelConfig{
			Batch: &config.BatchConfig{
				Enabled: true,
				SplitOn: "newline",
			},
		}
		p := NewPipeline("/tmp/ch", "/tmp", "ch-batch", cfg, nil, slog.Default())
		if p.splitter == nil {
			t.Error("splitter should be set when batch is enabled with valid splitOn")
		}
	})

	t.Run("batch disabled ignores splitter", func(t *testing.T) {
		cfg := &config.ChannelConfig{
			Batch: &config.BatchConfig{
				Enabled: false,
				SplitOn: "newline",
			},
		}
		p := NewPipeline("/tmp/ch", "/tmp", "ch-no-batch", cfg, nil, slog.Default())
		if p.splitter != nil {
			t.Error("splitter should be nil when batch is disabled")
		}
	})

	t.Run("invalid batch splitter disabled", func(t *testing.T) {
		cfg := &config.ChannelConfig{
			Batch: &config.BatchConfig{
				Enabled: true,
				SplitOn: "invalid_splitter_type",
			},
		}
		p := NewPipeline("/tmp/ch", "/tmp", "ch-bad-batch", cfg, nil, slog.Default())
		if p.splitter != nil {
			t.Error("splitter should be nil for invalid split type")
		}
	})

	t.Run("known data type parsed correctly", func(t *testing.T) {
		cfg := &config.ChannelConfig{
			DataTypes: &config.DataTypesConfig{Inbound: "json"},
		}
		p := NewPipeline("/tmp/ch", "/tmp", "ch-json", cfg, nil, slog.Default())
		parsed, err := p.parser.Parse([]byte(`{"a":1}`))
		if err != nil {
			t.Fatalf("json parser error: %v", err)
		}
		m, ok := parsed.(map[string]any)
		if !ok {
			t.Fatalf("parsed type = %T, want map[string]any", parsed)
		}
		if m["a"] != float64(1) {
			t.Errorf("parsed value = %v", m["a"])
		}
	})
}

func TestPipeline_SetMessageStore(t *testing.T) {
	p := newTestPipeline(nil)
	if p.store != nil {
		t.Error("store should start nil")
	}
	p.SetMessageStore(nil)
	if p.store != nil {
		t.Error("store should be nil after setting nil")
	}
}

func TestPipeline_SetResolvedDestinations(t *testing.T) {
	p := newTestPipeline(nil)
	dests := map[string]config.Destination{
		"d1": {Type: "http"},
	}
	p.SetResolvedDestinations(dests)
	if p.resolvedDests == nil {
		t.Error("resolvedDests should be set")
	}
	if p.resolvedDests["d1"].Type != "http" {
		t.Errorf("resolvedDests[d1].Type = %q", p.resolvedDests["d1"].Type)
	}
}

func TestPipeline_buildDestIntuMessage_sftp(t *testing.T) {
	p := newTestPipeline(nil)
	p.SetResolvedDestinations(map[string]config.Destination{
		"sftp-dest": {
			Type: "sftp",
			SFTP: &config.SFTPDestMapConfig{
				Host:            "sftp.example.com",
				Directory:       "/uploads",
				FilenamePattern: "data_*.csv",
			},
		},
	})
	dest := config.ChannelDestination{Name: "sftp-dest"}
	im := p.buildDestIntuMessage("data", "sftp-dest", dest)
	if im["transport"] != "sftp" {
		t.Errorf("transport = %v", im["transport"])
	}
	fdata := im["file"].(map[string]any)
	if fdata["filename"] != "data_*.csv" {
		t.Errorf("sftp filename = %v", fdata["filename"])
	}
	if fdata["directory"] != "/uploads" {
		t.Errorf("sftp directory = %v", fdata["directory"])
	}
}

func TestPipeline_buildDestIntuMessage_noOutboundDataType(t *testing.T) {
	p := newTestPipeline(&config.ChannelConfig{})
	dest := config.ChannelDestination{Type: "http", HTTP: &config.HTTPDestConfig{URL: "http://x", Method: "POST"}}
	im := p.buildDestIntuMessage("data", "d", dest)
	if im["contentType"] != "" {
		t.Errorf("contentType = %q, want empty", im["contentType"])
	}
}

func TestPipeline_resolveScriptPath_nestedChannel(t *testing.T) {
	p := &Pipeline{
		channelDir: "/project/channels/group/sub-channel",
		projectDir: "/project",
	}
	got := p.resolveScriptPath("handler.ts")
	want := filepath.Join("/project", "dist", "channels/group/sub-channel", "handler.js")
	if got != want {
		t.Errorf("resolveScriptPath(nested) = %q, want %q", got, want)
	}
}
