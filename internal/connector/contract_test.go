package connector

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- Source contract tests ---
// Every SourceConnector must: Start without error, report a Type, and Stop cleanly.

func TestSourceContract_HTTP(t *testing.T) {
	src := NewHTTPSource(&config.HTTPListener{Port: 0}, discardLogger())
	assertSourceContract(t, src, "http")
}

func TestSourceContract_TCP(t *testing.T) {
	src := NewTCPSource(&config.TCPListener{Port: 0, Mode: "raw", TimeoutMs: 1000}, discardLogger())
	assertSourceContract(t, src, "tcp")
}

func TestSourceContract_File(t *testing.T) {
	dir := t.TempDir()
	src := NewFileSource(&config.FileListener{
		Directory:    dir,
		FilePattern:  "*.dat",
		PollInterval: "1s",
	}, discardLogger())
	assertSourceContract(t, src, "file/")
}

func TestSourceContract_Channel(t *testing.T) {
	src := NewChannelSource(&config.ChannelListener{SourceChannelID: "test-ch-contract"}, discardLogger())
	assertSourceContract(t, src, "channel")
}

func TestSourceContract_SFTP(t *testing.T) {
	src := NewSFTPSource(&config.SFTPListener{
		Host:         "127.0.0.1",
		Port:         0,
		Directory:    "/nonexistent",
		PollInterval: "10s",
	}, discardLogger())
	if src.Type() != "sftp" {
		t.Fatalf("expected type sftp, got %s", src.Type())
	}
}

func TestSourceContract_Kafka(t *testing.T) {
	src := NewKafkaSource(&config.KafkaListener{
		Brokers: []string{"localhost:19092"},
		Topic:   "test-topic",
		GroupID: "test-group",
	}, discardLogger())
	if src.Type() != "kafka" {
		t.Fatalf("expected type kafka, got %s", src.Type())
	}
}

func TestSourceContract_Database(t *testing.T) {
	src := NewDatabaseSource(&config.DBListener{
		Driver:       "postgres",
		DSN:          "postgres://localhost/test",
		PollInterval: "10s",
		Query:        "SELECT 1",
	}, discardLogger())
	if src.Type() != "database" {
		t.Fatalf("expected type database, got %s", src.Type())
	}
}

func TestSourceContract_Email(t *testing.T) {
	src := NewEmailSource(&config.EmailListener{
		Protocol:     "imap",
		Host:         "localhost",
		Port:         143,
		PollInterval: "10s",
	}, discardLogger())
	if src.Type() != "email/imap" {
		t.Fatalf("expected type email/imap, got %s", src.Type())
	}
}

func TestSourceContract_DICOM(t *testing.T) {
	src := NewDICOMSource(&config.DICOMListener{Port: 0, AETitle: "TEST"}, discardLogger())
	assertSourceContract(t, src, "dicom")
}

func TestSourceContract_SOAP(t *testing.T) {
	src := NewSOAPSource(&config.SOAPListener{Port: 0, ServiceName: "TestService"}, discardLogger())
	assertSourceContract(t, src, "soap")
}

func TestSourceContract_FHIR(t *testing.T) {
	src := NewFHIRSource(&config.FHIRListener{Port: 0, Version: "R4"}, discardLogger())
	assertSourceContract(t, src, "fhir")
}

func TestSourceContract_IHE(t *testing.T) {
	src := NewIHESource(&config.IHEListener{Profile: "XDS.b", Port: 0}, discardLogger())
	assertSourceContract(t, src, "ihe/xds.b")
}

func TestSourceContract_Stub(t *testing.T) {
	src := NewStubSource("custom", discardLogger())
	assertSourceContract(t, src, "custom")
}

func assertSourceContract(t *testing.T, src SourceConnector, expectedType string) {
	t.Helper()

	if src.Type() != expectedType {
		t.Fatalf("Type(): expected %q, got %q", expectedType, src.Type())
	}

	ctx, cancel := context.WithCancel(context.Background())
	handler := func(ctx context.Context, msg *message.Message) error {
		return nil
	}

	err := src.Start(ctx, handler)
	if err != nil {
		t.Fatalf("Start() should not error: %v", err)
	}

	cancel()
	time.Sleep(50 * time.Millisecond)

	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() should not error: %v", err)
	}
}

// --- Destination contract tests ---
// Every DestinationConnector must: report a Type and Stop cleanly.

func TestDestContract_HTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	dest := NewHTTPDest("test", &config.HTTPDestConfig{URL: srv.URL}, discardLogger())
	assertDestContract(t, dest, "http")
	assertDestCanSend(t, dest)
}

func TestDestContract_File(t *testing.T) {
	dir := t.TempDir()
	dest := NewFileDest("test", &config.FileDestMapConfig{
		Directory:       dir,
		FilenamePattern: "msg_{{messageId}}.json",
	}, discardLogger())
	assertDestContract(t, dest, "file")
	assertDestCanSend(t, dest)

	entries, _ := os.ReadDir(dir)
	if len(entries) < 1 {
		t.Fatal("file dest should have created at least 1 file")
	}
}

func TestDestContract_Channel(t *testing.T) {
	dest := NewChannelDest("test", "target-ch", discardLogger())
	assertDestContract(t, dest, "channel")
}

func TestDestContract_TCP(t *testing.T) {
	dest := NewTCPDest("test", &config.TCPDestMapConfig{
		Host: "127.0.0.1", Port: 0, Mode: "raw",
	}, discardLogger())
	if dest.Type() != "tcp" {
		t.Fatalf("expected type tcp, got %s", dest.Type())
	}
}

func TestDestContract_Kafka(t *testing.T) {
	dest := NewKafkaDest("test", &config.KafkaDestConfig{
		Brokers: []string{"localhost:19092"}, Topic: "test",
	}, discardLogger())
	if dest.Type() != "kafka" {
		t.Fatalf("expected type kafka, got %s", dest.Type())
	}
}

func TestDestContract_Database(t *testing.T) {
	dest := NewDatabaseDest("test", &config.DBDestMapConfig{
		Driver: "postgres", DSN: "postgres://localhost/test",
	}, discardLogger())
	if dest.Type() != "database" {
		t.Fatalf("expected type database, got %s", dest.Type())
	}
}

func TestDestContract_SMTP(t *testing.T) {
	dest := NewSMTPDest("test", &config.SMTPDestMapConfig{
		Host: "localhost", Port: 1025,
	}, discardLogger())
	if dest.Type() != "smtp" {
		t.Fatalf("expected type smtp, got %s", dest.Type())
	}
}

func TestDestContract_DICOM(t *testing.T) {
	dest := NewDICOMDest("test", &config.DICOMDestMapConfig{
		Host: "localhost", Port: 104, AETitle: "TEST",
	}, discardLogger())
	if dest.Type() != "dicom" {
		t.Fatalf("expected type dicom, got %s", dest.Type())
	}
}

func TestDestContract_JMS(t *testing.T) {
	dest := NewJMSDest("test", &config.JMSDestMapConfig{
		Provider: "activemq", URL: "http://localhost:8161", Queue: "test",
	}, discardLogger())
	if dest.Type() != "jms" {
		t.Fatalf("expected type jms, got %s", dest.Type())
	}
}

func TestDestContract_SFTP(t *testing.T) {
	dest := NewSFTPDest("test", &config.SFTPDestMapConfig{
		Host: "localhost", Port: 0, Directory: "/tmp",
	}, discardLogger())
	if dest.Type() != "sftp" {
		t.Fatalf("expected type sftp, got %s", dest.Type())
	}
}

func TestDestContract_FHIR(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/fhir+json")
		w.WriteHeader(201)
		w.Write([]byte(`{"resourceType":"OperationOutcome"}`))
	}))
	defer srv.Close()

	dest := NewFHIRDest("test", &config.FHIRDestMapConfig{
		BaseURL: srv.URL, Version: "R4",
		Operations: []string{"Patient:create"},
	}, discardLogger())
	assertDestContract(t, dest, "fhir")
}

func TestDestContract_Direct(t *testing.T) {
	dest := NewDirectDest("test", &config.DirectDestMapConfig{
		To: "test@example.com", From: "intu@example.com",
	}, discardLogger())
	if dest.Type() != "direct" {
		t.Fatalf("expected type direct, got %s", dest.Type())
	}
}

func TestDestContract_Log(t *testing.T) {
	dest := NewLogDest("test", discardLogger())
	assertDestContract(t, dest, "log")
	assertDestCanSend(t, dest)
}

func assertDestContract(t *testing.T, dest DestinationConnector, expectedType string) {
	t.Helper()

	if dest.Type() != expectedType {
		t.Fatalf("Type(): expected %q, got %q", expectedType, dest.Type())
	}

	if err := dest.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() should not error: %v", err)
	}
}

func assertDestCanSend(t *testing.T, dest DestinationConnector) {
	t.Helper()
	msg := message.New("contract-test", []byte(`{"test":true}`))
	resp, err := dest.Send(context.Background(), msg)
	if err != nil {
		t.Fatalf("Send() should not error: %v", err)
	}
	_ = resp
}

// --- Factory contract: every source and destination type creates without error ---

func TestFactory_AllSourceTypes_ReturnCorrectType(t *testing.T) {
	f := NewFactory(discardLogger())

	dir := t.TempDir()
	cases := []struct {
		name string
		cfg  config.ListenerConfig
		want string
	}{
		{"http", config.ListenerConfig{Type: "http", HTTP: &config.HTTPListener{Port: 0}}, "http"},
		{"tcp", config.ListenerConfig{Type: "tcp", TCP: &config.TCPListener{Port: 0}}, "tcp"},
		{"file", config.ListenerConfig{Type: "file", File: &config.FileListener{Directory: dir, PollInterval: "1s"}}, "file/"},
		{"channel", config.ListenerConfig{Type: "channel", Channel: &config.ChannelListener{SourceChannelID: "x"}}, "channel"},
		{"sftp", config.ListenerConfig{Type: "sftp", SFTP: &config.SFTPListener{Host: "localhost"}}, "sftp"},
		{"database", config.ListenerConfig{Type: "database", Database: &config.DBListener{Driver: "postgres", DSN: "x"}}, "database"},
		{"kafka", config.ListenerConfig{Type: "kafka", Kafka: &config.KafkaListener{Brokers: []string{"a"}, Topic: "t"}}, "kafka"},
		{"email", config.ListenerConfig{Type: "email", Email: &config.EmailListener{Host: "localhost"}}, "email/imap"},
		{"dicom", config.ListenerConfig{Type: "dicom", DICOM: &config.DICOMListener{Port: 0}}, "dicom"},
		{"soap", config.ListenerConfig{Type: "soap", SOAP: &config.SOAPListener{Port: 0}}, "soap"},
		{"fhir", config.ListenerConfig{Type: "fhir", FHIR: &config.FHIRListener{Port: 0}}, "fhir"},
		{"ihe", config.ListenerConfig{Type: "ihe", IHE: &config.IHEListener{Profile: "XDS.b", Port: 0}}, "ihe/xds.b"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src, err := f.CreateSource(tc.cfg)
			if err != nil {
				t.Fatalf("CreateSource(%s): %v", tc.name, err)
			}
			if src.Type() != tc.want {
				t.Fatalf("expected type %q, got %q", tc.want, src.Type())
			}
		})
	}
}

func TestFactory_AllDestTypes_ReturnCorrectType(t *testing.T) {
	f := NewFactory(discardLogger())

	dir := filepath.Join(t.TempDir(), "output")
	os.MkdirAll(dir, 0o755)

	cases := []struct {
		name string
		cfg  config.Destination
		want string
	}{
		{"http", config.Destination{Type: "http", HTTP: &config.HTTPDestConfig{URL: "http://localhost"}}, "http"},
		{"file", config.Destination{Type: "file", File: &config.FileDestMapConfig{Directory: dir}}, "file"},
		{"channel", config.Destination{Type: "channel", Channel: &config.ChannelDestMapConfig{TargetChannelID: "x"}}, "channel"},
		{"tcp", config.Destination{Type: "tcp", TCP: &config.TCPDestMapConfig{Host: "localhost", Port: 0}}, "tcp"},
		{"kafka", config.Destination{Type: "kafka", Kafka: &config.KafkaDestConfig{Brokers: []string{"a"}, Topic: "t"}}, "kafka"},
		{"database", config.Destination{Type: "database", Database: &config.DBDestMapConfig{Driver: "postgres", DSN: "x"}}, "database"},
		{"smtp", config.Destination{Type: "smtp", SMTP: &config.SMTPDestMapConfig{Host: "localhost"}}, "smtp"},
		{"dicom", config.Destination{Type: "dicom", DICOM: &config.DICOMDestMapConfig{Host: "localhost"}}, "dicom"},
		{"jms", config.Destination{Type: "jms", JMS: &config.JMSDestMapConfig{Provider: "activemq", URL: "http://localhost", Queue: "q"}}, "jms"},
		{"sftp", config.Destination{Type: "sftp", SFTP: &config.SFTPDestMapConfig{Host: "localhost"}}, "sftp"},
		{"fhir", config.Destination{Type: "fhir", FHIR: &config.FHIRDestMapConfig{BaseURL: "http://localhost"}}, "fhir"},
		{"direct", config.Destination{Type: "direct", Direct: &config.DirectDestMapConfig{To: "a@b.com"}}, "direct"},
		{"log", config.Destination{Type: "log"}, "log"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dest, err := f.CreateDestination(tc.name, tc.cfg)
			if err != nil {
				t.Fatalf("CreateDestination(%s): %v", tc.name, err)
			}
			if dest.Type() != tc.want {
				t.Fatalf("expected type %q, got %q", tc.want, dest.Type())
			}
		})
	}
}
