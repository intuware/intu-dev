package cluster

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

// ---------------------------------------------------------------------------
// Memory coordinator: IsLeader (single node always leader)
// ---------------------------------------------------------------------------

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
	// With only self as peer, should be leader once heartbeat runs
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer c.Stop()

	time.Sleep(50 * time.Millisecond)
	// After heartbeat, the node registers itself so it should be leader
	if !c.IsLeader() {
		t.Fatal("single node in cluster should be leader")
	}
}

// ---------------------------------------------------------------------------
// Memory coordinator: heartbeat lifecycle
// ---------------------------------------------------------------------------

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

	// After stop, no panic should occur
}

func TestMemoryCoordinator_StopWithoutStart(t *testing.T) {
	c := NewCoordinator(nil, slog.Default())
	c.Stop() // should not panic
}

// ---------------------------------------------------------------------------
// Memory coordinator: concurrent channel operations
// ---------------------------------------------------------------------------

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

	// Release all concurrently
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

// ---------------------------------------------------------------------------
// Memory dedup: concurrent IsDuplicate calls
// ---------------------------------------------------------------------------

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
	// Exactly one goroutine should see it as non-duplicate (the first one)
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

// ---------------------------------------------------------------------------
// Memory dedup: window expiry behavior
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// RedisClient: constructor error handling
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// RedisCoordinator: constructor
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// RedisCoordinator: ShouldAcquireChannel edge cases
// ---------------------------------------------------------------------------

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
	// No affinity entry for inst-x, so it falls through to allow
	if !rc.ShouldAcquireChannel("ch-1", []string{"adt"}) {
		t.Fatal("instance with no affinity entry should be allowed")
	}
}

// ---------------------------------------------------------------------------
// RedisDeduplicator: constructor
// ---------------------------------------------------------------------------

func TestNewRedisDeduplicator(t *testing.T) {
	rd := NewRedisDeduplicator(nil, 5*time.Second)
	if rd == nil {
		t.Fatal("expected non-nil RedisDeduplicator")
	}
	if rd.window != 5*time.Second {
		t.Fatalf("expected window 5s, got %v", rd.window)
	}
}
