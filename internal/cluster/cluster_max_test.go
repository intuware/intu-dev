package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

// ---------------------------------------------------------------------------
// Coordinator: heartbeat, IsLeader with cluster enabled
// ---------------------------------------------------------------------------

func TestCoordinator_Heartbeat_Updates_Peers(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:           true,
		InstanceID:        "node-a",
		HeartbeatInterval: "50ms",
	}, slog.Default())

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	peers := c.GetPeers()
	found := false
	for _, p := range peers {
		if p.InstanceID == "node-a" && p.Status == "active" {
			found = true
			if time.Since(p.LastSeen) > 200*time.Millisecond {
				t.Error("peer LastSeen is too old")
			}
		}
	}
	if !found {
		t.Fatal("node-a not found in peers after heartbeat")
	}

	c.Stop()
}

func TestCoordinator_Heartbeat_RemovesStale(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:    true,
		InstanceID: "node-x",
	}, slog.Default())

	c.mu.Lock()
	c.peers["stale-node"] = PeerInfo{
		InstanceID: "stale-node",
		LastSeen:   time.Now().Add(-60 * time.Second),
		Status:     "active",
	}
	c.mu.Unlock()

	c.heartbeat()

	c.mu.RLock()
	_, staleExists := c.peers["stale-node"]
	c.mu.RUnlock()

	if staleExists {
		t.Error("stale peer should be removed")
	}
}

func TestCoordinator_IsLeader_ClusterEnabled_SingleNode(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:    true,
		InstanceID: "node-1",
	}, slog.Default())

	c.heartbeat()

	if !c.IsLeader() {
		t.Error("single node should be leader")
	}
}

func TestCoordinator_IsLeader_ClusterEnabled_MultipleNodes(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:    true,
		InstanceID: "node-b",
	}, slog.Default())

	c.mu.Lock()
	c.peers["node-a"] = PeerInfo{InstanceID: "node-a", LastSeen: time.Now(), Status: "active"}
	c.peers["node-b"] = PeerInfo{InstanceID: "node-b", LastSeen: time.Now(), Status: "active"}
	c.peers["node-c"] = PeerInfo{InstanceID: "node-c", LastSeen: time.Now(), Status: "active"}
	c.mu.Unlock()

	if c.IsLeader() {
		t.Error("node-b should not be leader when node-a exists (alphabetically smaller)")
	}
}

func TestCoordinator_IsLeader_ClusterEnabled_IsSmallest(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:    true,
		InstanceID: "node-a",
	}, slog.Default())

	c.mu.Lock()
	c.peers["node-a"] = PeerInfo{InstanceID: "node-a", LastSeen: time.Now(), Status: "active"}
	c.peers["node-b"] = PeerInfo{InstanceID: "node-b", LastSeen: time.Now(), Status: "active"}
	c.mu.Unlock()

	if !c.IsLeader() {
		t.Error("node-a should be leader (alphabetically first)")
	}
}

func TestCoordinator_IsLeader_ClusterDisabled(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled: false,
	}, slog.Default())

	if !c.IsLeader() {
		t.Error("disabled cluster should always be leader")
	}
}

func TestCoordinator_IsLeader_NilConfig(t *testing.T) {
	c := NewCoordinator(nil, slog.Default())
	if !c.IsLeader() {
		t.Error("nil config should always be leader")
	}
}

// ---------------------------------------------------------------------------
// Coordinator: ShouldAcquireChannel with tags
// ---------------------------------------------------------------------------

func TestCoordinator_ShouldAcquireChannel_AlwaysTrue(t *testing.T) {
	c := NewCoordinator(nil, slog.Default())

	if !c.ShouldAcquireChannel("ch-1", nil) {
		t.Error("memory coordinator should always allow acquisition")
	}
	if !c.ShouldAcquireChannel("ch-1", []string{"tag1", "tag2"}) {
		t.Error("memory coordinator should always allow acquisition even with tags")
	}
}

func TestCoordinator_AcquireMultipleChannels(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:    true,
		InstanceID: "test",
	}, slog.Default())

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		acquired, err := c.AcquireChannel(ctx, fmt.Sprintf("ch-%d", i))
		if err != nil {
			t.Fatalf("acquire ch-%d: %v", i, err)
		}
		if !acquired {
			t.Fatalf("expected to acquire ch-%d", i)
		}
	}

	owned := c.OwnedChannels()
	if len(owned) != 5 {
		t.Errorf("expected 5 owned channels, got %d", len(owned))
	}
}

func TestCoordinator_ReleaseUnownedChannel(t *testing.T) {
	c := NewCoordinator(nil, slog.Default())
	err := c.ReleaseChannel(context.Background(), "nonexistent")
	if err != nil {
		t.Errorf("releasing unowned channel should not error: %v", err)
	}
}

func TestCoordinator_InstanceID_Custom(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		InstanceID: "my-instance",
	}, slog.Default())
	if c.InstanceID() != "my-instance" {
		t.Errorf("expected 'my-instance', got %q", c.InstanceID())
	}
}

func TestCoordinator_StartStop_ClusterEnabled(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:           true,
		InstanceID:        "test-1",
		HeartbeatInterval: "50ms",
	}, slog.Default())

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	c.Stop()
}

func TestCoordinator_StartStop_ClusterDisabled(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled: false,
	}, slog.Default())

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	c.Stop()
}

// ---------------------------------------------------------------------------
// Deduplicator: many keys, cleanup
// ---------------------------------------------------------------------------

func TestDeduplicator_ManyKeys(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		if d.IsDuplicate(key) {
			t.Fatalf("first check for %s should not be duplicate", key)
		}
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		if !d.IsDuplicate(key) {
			t.Fatalf("second check for %s should be duplicate", key)
		}
	}
}

func TestDeduplicator_CleanupRemovesExpired(t *testing.T) {
	d := NewDeduplicator(100 * time.Millisecond)

	d.IsDuplicate("expire-me")
	if !d.IsDuplicate("expire-me") {
		t.Fatal("should be duplicate immediately")
	}

	time.Sleep(250 * time.Millisecond)

	if d.IsDuplicate("expire-me") {
		t.Fatal("key should have been cleaned up and not be duplicate")
	}
}

func TestDeduplicator_ConcurrentAccess(t *testing.T) {
	d := NewDeduplicator(2 * time.Second)

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			for j := 0; j < 100; j++ {
				d.IsDuplicate(fmt.Sprintf("concurrent-%d-%d", n, j))
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for concurrent access")
		}
	}
}

// ---------------------------------------------------------------------------
// HealthChecker: custom paths, custom port, HTTP handler responses
// ---------------------------------------------------------------------------

func TestHealthChecker_DefaultConfig(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	if hc.cfg.Port != 8081 {
		t.Errorf("expected default port 8081, got %d", hc.cfg.Port)
	}
	if hc.cfg.Path != "/health" {
		t.Errorf("expected default path '/health', got %q", hc.cfg.Path)
	}
}

func TestHealthChecker_CustomConfig(t *testing.T) {
	hc := NewHealthChecker(&config.HealthConfig{
		Port:          9090,
		Path:          "/status",
		ReadinessPath: "/readiness",
		LivenessPath:  "/liveness",
	}, slog.Default())

	if hc.cfg.Port != 9090 {
		t.Errorf("expected port 9090, got %d", hc.cfg.Port)
	}
	if hc.cfg.Path != "/status" {
		t.Errorf("expected path '/status', got %q", hc.cfg.Path)
	}
}

func TestHealthChecker_HandleHealth_Healthy(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.UpdateChannels(3, 0, 0)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	hc.handleHealth(w, req)

	if w.Code != 200 {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp HealthResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Status != StatusHealthy {
		t.Errorf("expected healthy, got %v", resp.Status)
	}
	if resp.Channels.Running != 3 {
		t.Errorf("expected 3 running, got %d", resp.Channels.Running)
	}
}

func TestHealthChecker_HandleHealth_Degraded(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.UpdateChannels(2, 0, 1)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	hc.handleHealth(w, req)

	var resp HealthResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Status != StatusDegraded {
		t.Errorf("expected degraded, got %v", resp.Status)
	}
}

func TestHealthChecker_HandleHealth_Unhealthy(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.SetStatus(StatusUnhealthy)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	hc.handleHealth(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestHealthChecker_HandleReadiness_Healthy(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())

	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()
	hc.handleReadiness(w, req)

	if w.Code != 200 {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != `{"ready":true}` {
		t.Errorf("unexpected body: %s", w.Body.String())
	}
}

func TestHealthChecker_HandleReadiness_Unhealthy(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.SetStatus(StatusUnhealthy)

	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()
	hc.handleReadiness(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestHealthChecker_HandleLiveness(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())

	req := httptest.NewRequest("GET", "/live", nil)
	w := httptest.NewRecorder()
	hc.handleLiveness(w, req)

	if w.Code != 200 {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != `{"alive":true}` {
		t.Errorf("unexpected body: %s", w.Body.String())
	}
}

func TestHealthChecker_SetStatus(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())

	hc.SetStatus(StatusDegraded)
	if hc.status != StatusDegraded {
		t.Errorf("expected degraded, got %v", hc.status)
	}

	hc.SetStatus(StatusUnhealthy)
	if hc.status != StatusUnhealthy {
		t.Errorf("expected unhealthy, got %v", hc.status)
	}

	hc.SetStatus(StatusHealthy)
	if hc.status != StatusHealthy {
		t.Errorf("expected healthy, got %v", hc.status)
	}
}

func TestHealthChecker_Uptime(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	time.Sleep(50 * time.Millisecond)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	hc.handleHealth(w, req)

	var resp HealthResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Uptime == "" {
		t.Error("expected non-empty uptime")
	}
}

func TestHealthChecker_ContentType(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	hc.handleHealth(w, req)

	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected 'application/json', got %q", ct)
	}
}
