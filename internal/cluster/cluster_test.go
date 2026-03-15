package cluster

import (
	"context"
	"encoding/json"
	"fmt"

	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

func TestMemoryCoordinatorImplementsInterface(t *testing.T) {
	c := NewCoordinator(nil, slog.Default())
	var _ ChannelCoordinator = c
}

func TestMemoryCoordinatorStandalone(t *testing.T) {
	c := NewCoordinator(nil, slog.Default())

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer c.Stop()

	if !c.IsLeader() {
		t.Fatal("standalone instance should be leader")
	}

	if c.InstanceID() != "standalone" {
		t.Fatalf("expected instanceID 'standalone', got %q", c.InstanceID())
	}
}

func TestMemoryCoordinatorAcquireRelease(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:    true,
		InstanceID: "test-1",
	}, slog.Default())

	ctx := context.Background()

	acquired, err := c.AcquireChannel(ctx, "ch-1")
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	if !acquired {
		t.Fatal("expected to acquire channel")
	}

	owned := c.OwnedChannels()
	if len(owned) == 0 {
		t.Fatal("expected at least one owned channel")
	}

	found := false
	for _, ch := range owned {
		if ch == "ch-1" {
			found = true
		}
	}
	if !found {
		t.Fatal("ch-1 not found in owned channels")
	}

	if err := c.ReleaseChannel(ctx, "ch-1"); err != nil {
		t.Fatalf("release failed: %v", err)
	}

	owned = c.OwnedChannels()
	for _, ch := range owned {
		if ch == "ch-1" {
			t.Fatal("ch-1 should not be in owned channels after release")
		}
	}
}

func TestMemoryCoordinatorRenewLease(t *testing.T) {
	c := NewCoordinator(nil, slog.Default())
	if err := c.RenewChannelLease(context.Background(), "ch-1"); err != nil {
		t.Fatalf("renew should be a no-op for memory coordinator: %v", err)
	}
}

func TestMemoryCoordinatorShouldAcquire(t *testing.T) {
	c := NewCoordinator(nil, slog.Default())
	if !c.ShouldAcquireChannel("ch-1", nil) {
		t.Fatal("memory coordinator should always allow channel acquisition")
	}
}

func TestMemoryCoordinatorHeartbeat(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:           true,
		InstanceID:        "test-1",
		HeartbeatInterval: "100ms",
	}, slog.Default())

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	time.Sleep(250 * time.Millisecond)

	peers := c.GetPeers()
	if len(peers) == 0 {
		t.Fatal("expected at least one peer from heartbeat")
	}

	found := false
	for _, p := range peers {
		if p.InstanceID == "test-1" {
			found = true
			if p.Status != "active" {
				t.Fatalf("expected status 'active', got %q", p.Status)
			}
		}
	}
	if !found {
		t.Fatal("test-1 not found in peers")
	}

	c.Stop()
}

func TestDeduplicatorInMemory(t *testing.T) {
	d := NewDeduplicator(1 * time.Second)

	if d.IsDuplicate("key-1") {
		t.Fatal("first call should not be duplicate")
	}

	if !d.IsDuplicate("key-1") {
		t.Fatal("second call should be duplicate")
	}

	if d.IsDuplicate("key-2") {
		t.Fatal("different key should not be duplicate")
	}
}

func TestDeduplicatorExpiry(t *testing.T) {
	d := NewDeduplicator(200 * time.Millisecond)

	if d.IsDuplicate("key-1") {
		t.Fatal("first call should not be duplicate")
	}

	time.Sleep(300 * time.Millisecond)

	if d.IsDuplicate("key-1") {
		t.Fatal("key should have expired, not duplicate")
	}
}

func TestDeduplicatorImplementsInterface(t *testing.T) {
	d := NewDeduplicator(time.Second)
	var _ MessageDeduplicator = d
}

func TestHealthCheckerCreation(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	if hc == nil {
		t.Fatal("expected non-nil health checker")
	}
}

func TestHealthCheckerUpdateChannels(t *testing.T) {
	hc := NewHealthChecker(nil, slog.Default())
	hc.UpdateChannels(5, 1, 0)

	if hc.status != StatusHealthy {
		t.Fatalf("expected healthy status, got %v", hc.status)
	}

	hc.UpdateChannels(5, 1, 2)
	if hc.status != StatusDegraded {
		t.Fatalf("expected degraded status when errored > 0, got %v", hc.status)
	}
}

func TestRedisCoordinatorShouldAcquireAuto(t *testing.T) {
	rc := &RedisCoordinator{
		cfg: &config.ClusterConfig{
			ChannelAssignment: &config.ChannelAssignConfig{
				Strategy: "auto",
			},
		},
	}
	if !rc.ShouldAcquireChannel("ch-1", nil) {
		t.Fatal("auto strategy should always allow acquisition")
	}
}

func TestRedisCoordinatorShouldAcquireTagBased(t *testing.T) {
	rc := &RedisCoordinator{
		instanceID: "inst-1",
		cfg: &config.ClusterConfig{
			ChannelAssignment: &config.ChannelAssignConfig{
				Strategy: "tag-based",
				TagAffinity: map[string][]string{
					"inst-1": {"adt", "lab"},
				},
			},
		},
	}

	if !rc.ShouldAcquireChannel("ch-1", []string{"adt"}) {
		t.Fatal("should acquire channel with matching tag")
	}

	if rc.ShouldAcquireChannel("ch-2", []string{"radiology"}) {
		t.Fatal("should not acquire channel without matching tag")
	}

	if rc.ShouldAcquireChannel("ch-3", nil) {
		t.Fatal("should not acquire channel with no tags when affinity is set")
	}
}
func TestMemoryCoordinator_IsLeader_NilConfig(t *testing.T) {
	c := NewCoordinator(nil, slog.Default())
	if !c.IsLeader() {
		t.Fatal("single node with nil config should be leader")
	}
}

func TestMemoryCoordinator_IsLeader_DisabledCluster(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{Enabled: false}, slog.Default())
	if !c.IsLeader() {
		t.Fatal("disabled cluster mode should always be leader")
	}
}

func TestMemoryCoordinator_IsLeader_Enabled(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:    true,
		InstanceID: "a-node",
	}, slog.Default())

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer c.Stop()

	time.Sleep(50 * time.Millisecond)

	if !c.IsLeader() {
		t.Fatal("single node in cluster should be leader")
	}
}

func TestMemoryCoordinator_HeartbeatLifecycle(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:           true,
		InstanceID:        "hb-node",
		HeartbeatInterval: "50ms",
	}, slog.Default())

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	peers := c.GetPeers()
	if len(peers) == 0 {
		t.Fatal("expected at least one peer after heartbeat")
	}

	found := false
	for _, p := range peers {
		if p.InstanceID == "hb-node" {
			found = true
			if p.Status != "active" {
				t.Fatalf("expected active status, got %q", p.Status)
			}
		}
	}
	if !found {
		t.Fatal("hb-node not found in peers")
	}

	c.Stop()

}

func TestMemoryCoordinator_StopWithoutStart(t *testing.T) {
	c := NewCoordinator(nil, slog.Default())
	c.Stop()
}

func TestMemoryCoordinator_ConcurrentChannelOps(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:    true,
		InstanceID: "conc-node",
	}, slog.Default())

	ctx := context.Background()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			chID := "ch-" + string(rune('A'+id%26))
			acquired, err := c.AcquireChannel(ctx, chID)
			if err != nil {
				t.Errorf("acquire %s: %v", chID, err)
				return
			}
			if !acquired {
				t.Errorf("expected to acquire %s", chID)
			}
		}(i)
	}
	wg.Wait()

	owned := c.OwnedChannels()
	if len(owned) == 0 {
		t.Fatal("expected some owned channels")
	}

	for _, ch := range owned {
		wg.Add(1)
		go func(chID string) {
			defer wg.Done()
			if err := c.ReleaseChannel(ctx, chID); err != nil {
				t.Errorf("release %s: %v", chID, err)
			}
		}(ch)
	}
	wg.Wait()

	if len(c.OwnedChannels()) != 0 {
		t.Fatal("expected no owned channels after release")
	}
}

func TestMemoryCoordinator_InstanceID_Custom(t *testing.T) {
	c := NewCoordinator(&config.ClusterConfig{
		Enabled:    true,
		InstanceID: "my-instance",
	}, slog.Default())
	if c.InstanceID() != "my-instance" {
		t.Fatalf("expected 'my-instance', got %q", c.InstanceID())
	}
}

func TestMemoryCoordinator_InstanceID_Default(t *testing.T) {
	c := NewCoordinator(nil, slog.Default())
	if c.InstanceID() != "standalone" {
		t.Fatalf("expected 'standalone', got %q", c.InstanceID())
	}
}

func TestDeduplicator_ConcurrentIsDuplicate(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)

	var wg sync.WaitGroup
	results := make([]bool, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = d.IsDuplicate("concurrent-key")
		}(i)
	}
	wg.Wait()

	nonDups := 0
	for _, isDup := range results {
		if !isDup {
			nonDups++
		}
	}

	if nonDups != 1 {
		t.Fatalf("expected exactly 1 non-duplicate, got %d", nonDups)
	}
}

func TestDeduplicator_ConcurrentDifferentKeys(t *testing.T) {
	d := NewDeduplicator(5 * time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := string(rune('A' + idx%26))
			d.IsDuplicate(key)
		}(i)
	}
	wg.Wait()
}

func TestDeduplicator_WindowExpiry(t *testing.T) {
	d := NewDeduplicator(100 * time.Millisecond)

	if d.IsDuplicate("exp-key") {
		t.Fatal("first call should not be duplicate")
	}
	if !d.IsDuplicate("exp-key") {
		t.Fatal("immediate second call should be duplicate")
	}

	time.Sleep(200 * time.Millisecond)

	if d.IsDuplicate("exp-key") {
		t.Fatal("after window expiry, key should not be duplicate")
	}
}

func TestNewRedisClient_NilConfig(t *testing.T) {
	_, err := NewRedisClient(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	if err.Error() != "redis config is nil" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewRedisClient_EmptyAddress(t *testing.T) {
	_, err := NewRedisClient(&config.RedisConfig{Address: ""})
	if err == nil {
		t.Fatal("expected error for empty address")
	}
	if err.Error() != "redis address is required" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewRedisClient_UnreachableAddress(t *testing.T) {
	_, err := NewRedisClient(&config.RedisConfig{Address: "localhost:1"})
	if err == nil {
		t.Fatal("expected error for unreachable redis")
	}
}

func TestNewRedisCoordinator_NilConfig(t *testing.T) {
	rc := NewRedisCoordinator(nil, nil, slog.Default())
	if rc == nil {
		t.Fatal("expected non-nil RedisCoordinator")
	}
	if rc.instanceID != "standalone" {
		t.Fatalf("expected 'standalone', got %q", rc.instanceID)
	}
}

func TestNewRedisCoordinator_CustomInstanceID(t *testing.T) {
	rc := NewRedisCoordinator(nil, &config.ClusterConfig{
		InstanceID: "redis-node-1",
	}, slog.Default())
	if rc.instanceID != "redis-node-1" {
		t.Fatalf("expected 'redis-node-1', got %q", rc.instanceID)
	}
}

func TestNewRedisCoordinator_OwnedChannelsEmpty(t *testing.T) {
	rc := NewRedisCoordinator(nil, nil, slog.Default())
	owned := rc.OwnedChannels()
	if len(owned) != 0 {
		t.Fatalf("expected 0 owned channels, got %d", len(owned))
	}
}

func TestRedisCoordinator_ShouldAcquire_NilCfg(t *testing.T) {
	rc := &RedisCoordinator{}
	if !rc.ShouldAcquireChannel("ch-1", nil) {
		t.Fatal("nil cfg should allow acquisition")
	}
}

func TestRedisCoordinator_ShouldAcquire_NilAssignment(t *testing.T) {
	rc := &RedisCoordinator{
		cfg: &config.ClusterConfig{ChannelAssignment: nil},
	}
	if !rc.ShouldAcquireChannel("ch-1", nil) {
		t.Fatal("nil channel assignment should allow acquisition")
	}
}

func TestRedisCoordinator_ShouldAcquire_EmptyStrategy(t *testing.T) {
	rc := &RedisCoordinator{
		cfg: &config.ClusterConfig{
			ChannelAssignment: &config.ChannelAssignConfig{Strategy: ""},
		},
	}
	if !rc.ShouldAcquireChannel("ch-1", nil) {
		t.Fatal("empty strategy should allow acquisition")
	}
}

func TestRedisCoordinator_ShouldAcquire_TagBased_NoAffinityForInstance(t *testing.T) {
	rc := &RedisCoordinator{
		instanceID: "inst-x",
		cfg: &config.ClusterConfig{
			ChannelAssignment: &config.ChannelAssignConfig{
				Strategy: "tag-based",
				TagAffinity: map[string][]string{
					"inst-1": {"adt"},
				},
			},
		},
	}

	if !rc.ShouldAcquireChannel("ch-1", []string{"adt"}) {
		t.Fatal("instance with no affinity entry should be allowed")
	}
}

func TestNewRedisDeduplicator(t *testing.T) {
	rd := NewRedisDeduplicator(nil, 5*time.Second)
	if rd == nil {
		t.Fatal("expected non-nil RedisDeduplicator")
	}
	if rd.window != 5*time.Second {
		t.Fatalf("expected window 5s, got %v", rd.window)
	}
}
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
