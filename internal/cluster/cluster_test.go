package cluster

import (
	"context"
	"log/slog"
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
