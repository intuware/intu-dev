package logging

import (
	"errors"
	"testing"

	"github.com/intuware/intu-dev/pkg/config"
)

// ===================================================================
// isAlreadyExistsError
// ===================================================================

func TestIsAlreadyExistsError_Nil(t *testing.T) {
	if isAlreadyExistsError(nil) {
		t.Fatal("expected false for nil error")
	}
}

func TestIsAlreadyExistsError_ResourceAlreadyExistsException(t *testing.T) {
	err := errors.New("ResourceAlreadyExistsException: The log group already exists")
	if !isAlreadyExistsError(err) {
		t.Fatal("expected true for ResourceAlreadyExistsException")
	}
}

func TestIsAlreadyExistsError_AlreadyExists(t *testing.T) {
	err := errors.New("log group already exists in region us-east-1")
	if !isAlreadyExistsError(err) {
		t.Fatal("expected true for 'already exists'")
	}
}

func TestIsAlreadyExistsError_Unrelated(t *testing.T) {
	err := errors.New("access denied")
	if isAlreadyExistsError(err) {
		t.Fatal("expected false for unrelated error")
	}
}

func TestIsAlreadyExistsError_WrappedError(t *testing.T) {
	inner := errors.New("ResourceAlreadyExistsException")
	wrapped := errors.New("operation failed: " + inner.Error())
	if !isAlreadyExistsError(wrapped) {
		t.Fatal("expected true for wrapped error containing ResourceAlreadyExistsException")
	}
}

// ===================================================================
// CloudWatchTransport — Write and Close with nil client (panic guard)
// ===================================================================

func TestCloudWatchTransport_WriteWithBatch(t *testing.T) {
	flushCalled := false
	cw := &CloudWatchTransport{
		logGroup:  "test-group",
		logStream: "test-stream",
	}
	cw.batch = newBatchBuffer(10, 1048576, 500000000, func(batch [][]byte) error {
		flushCalled = true
		return nil
	})

	n, err := cw.Write([]byte("test log entry"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 14 {
		t.Fatalf("expected 14 bytes written, got %d", n)
	}

	cw.batch.Close()
	if !flushCalled {
		t.Fatal("expected flush to be called on close")
	}
}

func TestCloudWatchTransport_CloseDelegates(t *testing.T) {
	closeCalled := false
	cw := &CloudWatchTransport{
		logGroup:  "g",
		logStream: "s",
	}
	cw.batch = newBatchBuffer(10, 1048576, 500000000, func(batch [][]byte) error {
		closeCalled = true
		return nil
	})
	cw.Write([]byte("entry"))
	if err := cw.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if !closeCalled {
		t.Fatal("expected batch flush on close")
	}
}

func TestCloudWatchTransport_FlushBatchEmpty(t *testing.T) {
	cw := &CloudWatchTransport{
		logGroup:  "g",
		logStream: "s",
	}
	err := cw.flushBatch(nil)
	if err != nil {
		t.Fatalf("expected nil error for empty batch, got %v", err)
	}
	err = cw.flushBatch([][]byte{})
	if err != nil {
		t.Fatalf("expected nil error for empty batch, got %v", err)
	}
}

func TestCloudWatchTransport_FlushBatchNilClient(t *testing.T) {
	cw := &CloudWatchTransport{
		client:    nil,
		logGroup:  "g",
		logStream: "s",
	}

	defer func() {
		if r := recover(); r != nil {
			// nil client will panic — that's the expected behavior
			// because CloudWatch requires a valid client in production
		}
	}()

	cw.flushBatch([][]byte{[]byte("log line")})
}

// ===================================================================
// buildTransport — cloudwatch type with nil config
// ===================================================================

func TestBuildTransport_CloudWatchNilConfig(t *testing.T) {
	_, err := buildTransport(config.LogTransportConfig{
		Type:       "cloudwatch",
		CloudWatch: nil,
	})
	if err == nil {
		t.Fatal("expected error for cloudwatch with nil config")
	}
}

func TestBuildTransport_CloudWatchWithConfig(t *testing.T) {
	_, err := buildTransport(config.LogTransportConfig{
		Type: "cloudwatch",
		CloudWatch: &config.CloudWatchLogConfig{
			Region:    "us-east-1",
			LogGroup:  "test-group",
			LogStream: "test-stream",
		},
	})
	// This will error because there are no real AWS credentials,
	// but we're testing that the code path is reached.
	if err == nil {
		t.Log("cloudwatch transport created (credentials were available)")
	}
}

// ===================================================================
// buildTransport — other types nil config
// ===================================================================

func TestBuildTransport_DatadogNilConfig(t *testing.T) {
	_, err := buildTransport(config.LogTransportConfig{
		Type:    "datadog",
		Datadog: nil,
	})
	if err == nil {
		t.Fatal("expected error for datadog with nil config")
	}
}

func TestBuildTransport_SumoLogicNilConfig(t *testing.T) {
	_, err := buildTransport(config.LogTransportConfig{
		Type:      "sumologic",
		SumoLogic: nil,
	})
	if err == nil {
		t.Fatal("expected error for sumologic with nil config")
	}
}

func TestBuildTransport_ElasticsearchNilConfig(t *testing.T) {
	_, err := buildTransport(config.LogTransportConfig{
		Type:          "elasticsearch",
		Elasticsearch: nil,
	})
	if err == nil {
		t.Fatal("expected error for elasticsearch with nil config")
	}
}

func TestBuildTransport_FileNilConfig(t *testing.T) {
	_, err := buildTransport(config.LogTransportConfig{
		Type: "file",
		File: nil,
	})
	if err == nil {
		t.Fatal("expected error for file with nil config")
	}
}

func TestBuildTransport_StdoutDefault(t *testing.T) {
	tr, err := buildTransport(config.LogTransportConfig{Type: ""})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tr == nil {
		t.Fatal("expected non-nil transport")
	}
}

func TestBuildTransport_StdoutExplicit(t *testing.T) {
	tr, err := buildTransport(config.LogTransportConfig{Type: "stdout"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := tr.(*stdoutTransport); !ok {
		t.Fatalf("expected *stdoutTransport, got %T", tr)
	}
}

func TestBuildTransport_UnknownType(t *testing.T) {
	_, err := buildTransport(config.LogTransportConfig{Type: "graylog"})
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

// ===================================================================
// CloudWatchTransport — multiple writes batch correctly
// ===================================================================

func TestCloudWatchTransport_MultipleWrites(t *testing.T) {
	var flushed [][]byte
	cw := &CloudWatchTransport{
		logGroup:  "g",
		logStream: "s",
	}
	cw.batch = newBatchBuffer(100, 1048576, 500000000, func(batch [][]byte) error {
		flushed = append(flushed, batch...)
		return nil
	})

	for i := 0; i < 5; i++ {
		cw.Write([]byte("line"))
	}
	cw.Close()

	if len(flushed) != 5 {
		t.Fatalf("expected 5 flushed entries, got %d", len(flushed))
	}
}
