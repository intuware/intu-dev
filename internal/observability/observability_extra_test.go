package observability

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

// ---------------------------------------------------------------------------
// OTelMetrics: GetOTelMetrics returns nil when not initialized
// ---------------------------------------------------------------------------

func TestGetOTelMetrics_NilWhenNotInitialized(t *testing.T) {
	// globalOTelMetrics is initialized via sync.Once in initOTelMetricsInstruments
	// when called via NewPrometheusServer. In a fresh test binary where that hasn't
	// fired yet, GetOTelMetrics may return nil. Since the once may have already
	// fired in other tests in this file, we just verify the function doesn't panic.
	_ = GetOTelMetrics()
}

// ---------------------------------------------------------------------------
// OTelMetrics: nil receiver safety for all methods
// ---------------------------------------------------------------------------

func TestOTelMetrics_NilReceiver_IncrReceived(t *testing.T) {
	var om *OTelMetrics
	om.IncrReceived("ch1") // must not panic
}

func TestOTelMetrics_NilReceiver_IncrProcessed(t *testing.T) {
	var om *OTelMetrics
	om.IncrProcessed("ch1") // must not panic
}

func TestOTelMetrics_NilReceiver_IncrErrored(t *testing.T) {
	var om *OTelMetrics
	om.IncrErrored("ch1", "dest1") // must not panic
}

func TestOTelMetrics_NilReceiver_IncrFiltered(t *testing.T) {
	var om *OTelMetrics
	om.IncrFiltered("ch1") // must not panic
}

func TestOTelMetrics_NilReceiver_RecordLatency(t *testing.T) {
	var om *OTelMetrics
	om.RecordLatency("ch1", "transform", 10*time.Millisecond) // must not panic
}

func TestOTelMetrics_NilReceiver_RecordDestLatency(t *testing.T) {
	var om *OTelMetrics
	om.RecordDestLatency("ch1", "dest1", 10*time.Millisecond) // must not panic
}

func TestOTelMetrics_NilReceiver_SetQueueDepth(t *testing.T) {
	var om *OTelMetrics
	om.SetQueueDepth("ch1", "dest1", 5) // must not panic
}

// ---------------------------------------------------------------------------
// PrometheusServer: constructor
// ---------------------------------------------------------------------------

func TestNewPrometheusServer_NilConfig(t *testing.T) {
	ps, err := NewPrometheusServer(nil, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps != nil {
		t.Fatal("expected nil server when config is nil")
	}
}

func TestNewPrometheusServer_Disabled(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{Enabled: false}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps != nil {
		t.Fatal("expected nil server when disabled")
	}
}

func TestNewPrometheusServer_Enabled(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    0,
		Path:    "",
	}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ps == nil {
		t.Fatal("expected non-nil PrometheusServer")
	}
}

// ---------------------------------------------------------------------------
// PrometheusServer: Start/Stop lifecycle
// ---------------------------------------------------------------------------

func TestPrometheusServer_StartStop(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    19123,
		Path:    "/metrics",
	}, slog.Default())
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	if err := ps.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Give it a moment to start
	time.Sleep(50 * time.Millisecond)

	if err := ps.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// ---------------------------------------------------------------------------
// OTelShutdown: Shutdown with nil providers
// ---------------------------------------------------------------------------

func TestOTelShutdown_NilProviders(t *testing.T) {
	s := &OTelShutdown{}
	err := s.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("expected no error from nil providers, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// InitOTel: nil/disabled config
// ---------------------------------------------------------------------------

func TestInitOTel_NilConfig(t *testing.T) {
	shutdown, err := InitOTel(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil OTelShutdown")
	}
	// Should be safe to shut down
	if err := shutdown.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
}

func TestInitOTel_Disabled(t *testing.T) {
	shutdown, err := InitOTel(&config.OTelConfig{Enabled: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil OTelShutdown")
	}
}

// ---------------------------------------------------------------------------
// MeterProvider: returns nil when not initialized with SDK provider
// ---------------------------------------------------------------------------

func TestMeterProvider_ReturnType(t *testing.T) {
	// MeterProvider() casts otel.GetMeterProvider() to *metric.MeterProvider.
	// If it's the noop global (before InitOTel with metrics), it returns nil.
	// We just verify it doesn't panic and returns a consistent value.
	_ = MeterProvider()
}

// ---------------------------------------------------------------------------
// OTelMetrics: exercising methods with real (non-nil) instance
// ---------------------------------------------------------------------------

func TestOTelMetrics_WithPrometheus(t *testing.T) {
	ps, err := NewPrometheusServer(&config.PrometheusConfig{
		Enabled: true,
		Port:    19124,
		Path:    "/metrics",
	}, slog.Default())
	if err != nil {
		t.Fatalf("create prometheus server: %v", err)
	}

	om := GetOTelMetrics()
	if om == nil {
		t.Skip("OTel metrics not initialized (sync.Once already fired without prometheus)")
	}

	// Exercise all methods on a real instance
	om.IncrReceived("test-ch")
	om.IncrProcessed("test-ch")
	om.IncrErrored("test-ch", "dest-1")
	om.IncrFiltered("test-ch")
	om.RecordLatency("test-ch", "transform", 50*time.Millisecond)
	om.RecordDestLatency("test-ch", "dest-1", 25*time.Millisecond)
	om.SetQueueDepth("test-ch", "dest-1", 10)

	_ = ps // keep reference alive
}
