package cluster

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/intuware/intu-dev/pkg/config"
)

func TestNewHealthCheckerNilConfigDefaults(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	if hc == nil {
		t.Fatal("expected non-nil health checker")
	}
	if hc.cfg.Port != 8081 {
		t.Fatalf("expected default port 8081, got %d", hc.cfg.Port)
	}
	if hc.cfg.Path != "/health" {
		t.Fatalf("expected default path /health, got %s", hc.cfg.Path)
	}
	if hc.cfg.ReadinessPath != "/ready" {
		t.Fatalf("expected default readiness path /ready, got %s", hc.cfg.ReadinessPath)
	}
	if hc.cfg.LivenessPath != "/live" {
		t.Fatalf("expected default liveness path /live, got %s", hc.cfg.LivenessPath)
	}
	if hc.status != StatusHealthy {
		t.Fatalf("expected initial status healthy, got %v", hc.status)
	}
}

func TestNewHealthCheckerCustomConfig(t *testing.T) {
	cfg := &config.HealthConfig{
		Port:          9090,
		Path:          "/healthz",
		ReadinessPath: "/readyz",
		LivenessPath:  "/livez",
	}
	hc := NewHealthChecker(cfg, slog.Default())
	if hc.cfg.Port != 9090 {
		t.Fatalf("expected port 9090, got %d", hc.cfg.Port)
	}
	if hc.cfg.Path != "/healthz" {
		t.Fatalf("expected path /healthz, got %s", hc.cfg.Path)
	}
}

func TestUpdateChannelsHealthy(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.UpdateChannels(5, 1, 0)
	if hc.status != StatusHealthy {
		t.Fatalf("expected healthy status with 0 errored, got %v", hc.status)
	}
	if hc.channels.Running != 5 {
		t.Fatalf("expected 5 running, got %d", hc.channels.Running)
	}
	if hc.channels.Stopped != 1 {
		t.Fatalf("expected 1 stopped, got %d", hc.channels.Stopped)
	}
	if hc.channels.Errored != 0 {
		t.Fatalf("expected 0 errored, got %d", hc.channels.Errored)
	}
}

func TestUpdateChannelsDegraded(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.UpdateChannels(3, 0, 2)
	if hc.status != StatusDegraded {
		t.Fatalf("expected degraded status when errored > 0, got %v", hc.status)
	}
	if hc.channels.Errored != 2 {
		t.Fatalf("expected 2 errored, got %d", hc.channels.Errored)
	}
}

func TestUpdateChannelsRecovery(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.UpdateChannels(3, 0, 2)
	if hc.status != StatusDegraded {
		t.Fatal("expected degraded")
	}
	hc.UpdateChannels(5, 0, 0)
	if hc.status != StatusHealthy {
		t.Fatalf("expected healthy after recovery, got %v", hc.status)
	}
}

func TestSetStatus(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.SetStatus(StatusUnhealthy)
	if hc.status != StatusUnhealthy {
		t.Fatalf("expected unhealthy, got %v", hc.status)
	}
	hc.SetStatus(StatusDegraded)
	if hc.status != StatusDegraded {
		t.Fatalf("expected degraded, got %v", hc.status)
	}
	hc.SetStatus(StatusHealthy)
	if hc.status != StatusHealthy {
		t.Fatalf("expected healthy, got %v", hc.status)
	}
}

func TestHandleHealthHealthy(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.UpdateChannels(3, 0, 0)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	hc.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != StatusHealthy {
		t.Fatalf("expected healthy status in response, got %v", resp.Status)
	}
	if resp.Channels.Running != 3 {
		t.Fatalf("expected 3 running channels, got %d", resp.Channels.Running)
	}
	if resp.Uptime == "" {
		t.Fatal("expected non-empty uptime")
	}
}

func TestHandleHealthUnhealthy(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.SetStatus(StatusUnhealthy)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	hc.handleHealth(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}

	var resp HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != StatusUnhealthy {
		t.Fatalf("expected unhealthy, got %v", resp.Status)
	}
}

func TestHandleHealthDegradedReturns200(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.SetStatus(StatusDegraded)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	hc.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for degraded status, got %d", w.Code)
	}
}

func TestHandleHealthContentType(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	hc.handleHealth(w, req)

	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %s", ct)
	}
}

func TestHandleReadinessHealthy(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	hc.handleReadiness(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	expected := `{"ready":true}`
	if w.Body.String() != expected {
		t.Fatalf("expected %q, got %q", expected, w.Body.String())
	}
}

func TestHandleReadinessUnhealthy(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.SetStatus(StatusUnhealthy)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	hc.handleReadiness(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
	expected := `{"ready":false}`
	if w.Body.String() != expected {
		t.Fatalf("expected %q, got %q", expected, w.Body.String())
	}
}

func TestHandleReadinessDegradedReturns200(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.SetStatus(StatusDegraded)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	hc.handleReadiness(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for degraded (not unhealthy), got %d", w.Code)
	}
}

func TestHandleLiveness(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/live", nil)
	w := httptest.NewRecorder()

	hc.handleLiveness(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	expected := `{"alive":true}`
	if w.Body.String() != expected {
		t.Fatalf("expected %q, got %q", expected, w.Body.String())
	}
}

func TestHandleLivenessAlwaysAlive(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.SetStatus(StatusUnhealthy)

	req := httptest.NewRequest(http.MethodGet, "/live", nil)
	w := httptest.NewRecorder()

	hc.handleLiveness(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 even when unhealthy, got %d", w.Code)
	}
	expected := `{"alive":true}`
	if w.Body.String() != expected {
		t.Fatalf("expected %q, got %q", expected, w.Body.String())
	}
}
