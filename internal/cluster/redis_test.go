package cluster

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/intuware/intu-dev/pkg/config"
)

func setupMiniredis(t *testing.T) (*miniredis.Miniredis, *RedisClient) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client, err := NewRedisClient(&config.RedisConfig{
		Address: mr.Addr(),
	})
	if err != nil {
		mr.Close()
		t.Fatalf("failed to create redis client: %v", err)
	}

	return mr, client
}

// --- RedisClient tests ---

func TestRedisClientMiniredisNilConfig(t *testing.T) {
	_, err := NewRedisClient(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
}

func TestRedisClientMiniredisEmptyAddress(t *testing.T) {
	_, err := NewRedisClient(&config.RedisConfig{Address: ""})
	if err == nil {
		t.Fatal("expected error for empty address")
	}
}

func TestRedisClientMiniredisSuccess(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	if client.Client() == nil {
		t.Fatal("expected non-nil redis client")
	}
}

func TestRedisClientMiniredisCustomPrefix(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client, err := NewRedisClient(&config.RedisConfig{
		Address:   mr.Addr(),
		KeyPrefix: "myapp",
	})
	if err != nil {
		t.Fatalf("failed to create redis client: %v", err)
	}
	defer client.Close()

	if client.keyPrefix != "myapp" {
		t.Fatalf("expected prefix 'myapp', got %q", client.keyPrefix)
	}
}

func TestRedisClientMiniredisDefaultPrefix(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	if client.keyPrefix != "intu" {
		t.Fatalf("expected default prefix 'intu', got %q", client.keyPrefix)
	}
}

func TestRedisClientMiniredisPingFailure(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	addr := mr.Addr()
	mr.Close()

	_, err = NewRedisClient(&config.RedisConfig{Address: addr})
	if err == nil {
		t.Fatal("expected error when ping fails")
	}
}

func TestRedisClientMiniredisKey(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	key := client.Key("instances", "node-1")
	if key != "intu:instances:node-1" {
		t.Fatalf("expected 'intu:instances:node-1', got %q", key)
	}

	key = client.Key("a", "b", "c")
	if key != "intu:a:b:c" {
		t.Fatalf("expected 'intu:a:b:c', got %q", key)
	}

	key = client.Key()
	if key != "intu" {
		t.Fatalf("expected 'intu', got %q", key)
	}
}

func TestRedisClientMiniredisClose(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()

	if err := client.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

// --- RedisCoordinator tests ---

func TestRedisCoordinatorMiniredisNew(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "test-node",
	}, slog.Default())

	if coord.InstanceID() != "test-node" {
		t.Fatalf("expected instanceID 'test-node', got %q", coord.InstanceID())
	}
}

func TestRedisCoordinatorMiniredisDefaultInstanceID(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, nil, slog.Default())
	if coord.InstanceID() != "standalone" {
		t.Fatalf("expected 'standalone', got %q", coord.InstanceID())
	}
}

func TestRedisCoordinatorMiniredisStartStop(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID:        "test-node",
		HeartbeatInterval: "50ms",
	}, slog.Default())

	if err := coord.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	coord.Stop()
}

func TestRedisCoordinatorMiniredisHeartbeat(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID:        "test-node",
		HeartbeatInterval: "50ms",
	}, slog.Default())

	coord.heartbeat(context.Background())

	key := client.Key("instances", "test-node")
	val, err := mr.Get(key)
	if err != nil {
		t.Fatalf("expected heartbeat key to exist: %v", err)
	}
	if val == "" {
		t.Fatal("expected non-empty heartbeat value")
	}
}

func TestRedisCoordinatorMiniredisGetPeers(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID:        "node-a",
		HeartbeatInterval: "50ms",
	}, slog.Default())

	coord.heartbeat(context.Background())

	peers := coord.GetPeers()
	if len(peers) == 0 {
		t.Fatal("expected at least one peer")
	}

	found := false
	for _, p := range peers {
		if p.InstanceID == "node-a" {
			found = true
			if p.Status != "active" {
				t.Fatalf("expected status 'active', got %q", p.Status)
			}
		}
	}
	if !found {
		t.Fatal("node-a not found in peers")
	}
}

func TestRedisCoordinatorMiniredisIsLeader(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "aaa-node",
	}, slog.Default())

	coord.heartbeat(context.Background())

	if !coord.IsLeader() {
		t.Fatal("expected single node to be leader")
	}
}

func TestRedisCoordinatorMiniredisIsLeaderMultipleNodes(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coordA := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "aaa-node",
	}, slog.Default())
	coordB := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "zzz-node",
	}, slog.Default())

	coordA.heartbeat(context.Background())
	coordB.heartbeat(context.Background())

	if !coordA.IsLeader() {
		t.Fatal("aaa-node should be leader (lexicographically first)")
	}
	if coordB.IsLeader() {
		t.Fatal("zzz-node should not be leader")
	}
}

func TestRedisCoordinatorMiniredisIsLeaderNoPeers(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-1",
	}, slog.Default())

	if !coord.IsLeader() {
		t.Fatal("expected leader when no peers exist")
	}
}

func TestRedisCoordinatorMiniredisAcquireChannel(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-1",
	}, slog.Default())

	ctx := context.Background()
	acquired, err := coord.AcquireChannel(ctx, "ch-1")
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}
	if !acquired {
		t.Fatal("expected to acquire channel")
	}

	owned := coord.OwnedChannels()
	found := false
	for _, ch := range owned {
		if ch == "ch-1" {
			found = true
		}
	}
	if !found {
		t.Fatal("ch-1 not found in owned channels")
	}
}

func TestRedisCoordinatorMiniredisAcquireChannelAlreadyOwned(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coordA := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-a",
	}, slog.Default())
	coordB := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-b",
	}, slog.Default())

	ctx := context.Background()

	acquired, err := coordA.AcquireChannel(ctx, "ch-1")
	if err != nil || !acquired {
		t.Fatal("node-a should acquire ch-1")
	}

	acquired, err = coordB.AcquireChannel(ctx, "ch-1")
	if err != nil {
		t.Fatalf("acquire should not error: %v", err)
	}
	if acquired {
		t.Fatal("node-b should not acquire ch-1 already owned by node-a")
	}
}

func TestRedisCoordinatorMiniredisRenewChannelLease(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-1",
	}, slog.Default())

	ctx := context.Background()
	coord.AcquireChannel(ctx, "ch-1")

	if err := coord.RenewChannelLease(ctx, "ch-1"); err != nil {
		t.Fatalf("renew failed: %v", err)
	}
}

func TestRedisCoordinatorMiniredisRenewChannelLeaseNotOwner(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coordA := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-a",
	}, slog.Default())
	coordB := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-b",
	}, slog.Default())

	ctx := context.Background()
	coordA.AcquireChannel(ctx, "ch-1")

	err := coordB.RenewChannelLease(ctx, "ch-1")
	if err == nil {
		t.Fatal("expected error when renewing lease not owned")
	}
}

func TestRedisCoordinatorMiniredisReleaseChannel(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-1",
	}, slog.Default())

	ctx := context.Background()
	coord.AcquireChannel(ctx, "ch-1")

	if err := coord.ReleaseChannel(ctx, "ch-1"); err != nil {
		t.Fatalf("release failed: %v", err)
	}

	owned := coord.OwnedChannels()
	for _, ch := range owned {
		if ch == "ch-1" {
			t.Fatal("ch-1 should not be owned after release")
		}
	}
}

func TestRedisCoordinatorMiniredisReleaseChannelNotOwned(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-1",
	}, slog.Default())

	err := coord.ReleaseChannel(context.Background(), "ch-nonexistent")
	if err != nil {
		t.Fatalf("releasing non-owned channel should not error: %v", err)
	}
}

func TestRedisCoordinatorMiniredisReleaseChannelOwnedByOther(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coordA := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-a",
	}, slog.Default())
	coordB := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-b",
	}, slog.Default())

	ctx := context.Background()
	coordA.AcquireChannel(ctx, "ch-1")

	err := coordB.ReleaseChannel(ctx, "ch-1")
	if err != nil {
		t.Fatalf("release by non-owner should not error: %v", err)
	}
}

func TestRedisCoordinatorMiniredisOwnedChannels(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID: "node-1",
	}, slog.Default())

	ctx := context.Background()
	coord.AcquireChannel(ctx, "ch-1")
	coord.AcquireChannel(ctx, "ch-2")

	owned := coord.OwnedChannels()
	if len(owned) != 2 {
		t.Fatalf("expected 2 owned channels, got %d", len(owned))
	}
}

func TestRedisCoordinatorMiniredisShouldAcquireChannelNilCfg(t *testing.T) {
	coord := &RedisCoordinator{cfg: nil}
	if !coord.ShouldAcquireChannel("ch-1", nil) {
		t.Fatal("nil config should allow acquisition")
	}
}

func TestRedisCoordinatorMiniredisShouldAcquireChannelNoAssignment(t *testing.T) {
	coord := &RedisCoordinator{
		cfg: &config.ClusterConfig{ChannelAssignment: nil},
	}
	if !coord.ShouldAcquireChannel("ch-1", nil) {
		t.Fatal("nil assignment should allow acquisition")
	}
}

func TestRedisCoordinatorMiniredisShouldAcquireChannelEmptyStrategy(t *testing.T) {
	coord := &RedisCoordinator{
		cfg: &config.ClusterConfig{
			ChannelAssignment: &config.ChannelAssignConfig{Strategy: ""},
		},
	}
	if !coord.ShouldAcquireChannel("ch-1", nil) {
		t.Fatal("empty strategy should allow acquisition")
	}
}

func TestRedisCoordinatorMiniredisShouldAcquireChannelTagBasedMatch(t *testing.T) {
	coord := &RedisCoordinator{
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

	if !coord.ShouldAcquireChannel("ch-1", []string{"adt"}) {
		t.Fatal("should acquire channel with matching tag")
	}
	if !coord.ShouldAcquireChannel("ch-2", []string{"lab"}) {
		t.Fatal("should acquire channel with matching tag")
	}
}

func TestRedisCoordinatorMiniredisShouldAcquireChannelTagBasedNoMatch(t *testing.T) {
	coord := &RedisCoordinator{
		instanceID: "inst-1",
		cfg: &config.ClusterConfig{
			ChannelAssignment: &config.ChannelAssignConfig{
				Strategy: "tag-based",
				TagAffinity: map[string][]string{
					"inst-1": {"adt"},
				},
			},
		},
	}

	if coord.ShouldAcquireChannel("ch-1", []string{"radiology"}) {
		t.Fatal("should not acquire channel without matching tag")
	}
	if coord.ShouldAcquireChannel("ch-2", nil) {
		t.Fatal("should not acquire channel with no tags when affinity is set")
	}
}

func TestRedisCoordinatorMiniredisShouldAcquireChannelTagBasedNoAffinity(t *testing.T) {
	coord := &RedisCoordinator{
		instanceID: "inst-2",
		cfg: &config.ClusterConfig{
			ChannelAssignment: &config.ChannelAssignConfig{
				Strategy: "tag-based",
				TagAffinity: map[string][]string{
					"inst-1": {"adt"},
				},
			},
		},
	}

	if !coord.ShouldAcquireChannel("ch-1", []string{"adt"}) {
		t.Fatal("should acquire when instance has no affinity entry")
	}
}

// --- RedisDeduplicator tests ---

func TestRedisDeduplicatorMiniredisNew(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	dedup := NewRedisDeduplicator(client, 5*time.Second)
	if dedup == nil {
		t.Fatal("expected non-nil deduplicator")
	}
}

func TestRedisDeduplicatorMiniredisIsDuplicate(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	dedup := NewRedisDeduplicator(client, 5*time.Second)

	if dedup.IsDuplicate("key-1") {
		t.Fatal("first call should not be duplicate")
	}

	if !dedup.IsDuplicate("key-1") {
		t.Fatal("second call should be duplicate")
	}

	if dedup.IsDuplicate("key-2") {
		t.Fatal("different key should not be duplicate")
	}
}

func TestRedisDeduplicatorMiniredisWindowExpiry(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	dedup := NewRedisDeduplicator(client, 1*time.Second)

	if dedup.IsDuplicate("key-1") {
		t.Fatal("first call should not be duplicate")
	}

	mr.FastForward(2 * time.Second)

	if dedup.IsDuplicate("key-1") {
		t.Fatal("key should have expired, not duplicate")
	}
}

func TestRedisDeduplicatorMiniredisIsDuplicateCtx(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	dedup := NewRedisDeduplicator(client, 5*time.Second)
	ctx := context.Background()

	isDup, err := dedup.IsDuplicateCtx(ctx, "ctx-key-1")
	if err != nil {
		t.Fatalf("IsDuplicateCtx failed: %v", err)
	}
	if isDup {
		t.Fatal("first call should not be duplicate")
	}

	isDup, err = dedup.IsDuplicateCtx(ctx, "ctx-key-1")
	if err != nil {
		t.Fatalf("IsDuplicateCtx failed: %v", err)
	}
	if !isDup {
		t.Fatal("second call should be duplicate")
	}
}

func TestRedisDeduplicatorMiniredisIsDuplicateCtxExpiry(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	dedup := NewRedisDeduplicator(client, 1*time.Second)
	ctx := context.Background()

	isDup, err := dedup.IsDuplicateCtx(ctx, "ctx-exp-1")
	if err != nil {
		t.Fatalf("IsDuplicateCtx failed: %v", err)
	}
	if isDup {
		t.Fatal("first call should not be duplicate")
	}

	mr.FastForward(2 * time.Second)

	isDup, err = dedup.IsDuplicateCtx(ctx, "ctx-exp-1")
	if err != nil {
		t.Fatalf("IsDuplicateCtx failed: %v", err)
	}
	if isDup {
		t.Fatal("key should have expired")
	}
}

func TestRedisCoordinatorMiniredisStartWithHeartbeat(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID:        "hb-node",
		HeartbeatInterval: "50ms",
	}, slog.Default())

	if err := coord.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	time.Sleep(120 * time.Millisecond)

	peers := coord.GetPeers()
	found := false
	for _, p := range peers {
		if p.InstanceID == "hb-node" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected hb-node in peers after heartbeat")
	}

	coord.Stop()
}

func TestRedisCoordinatorMiniredisStopReleasesChannels(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	coord := NewRedisCoordinator(client, &config.ClusterConfig{
		InstanceID:        "stop-node",
		HeartbeatInterval: "50ms",
	}, slog.Default())

	if err := coord.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	ctx := context.Background()
	coord.AcquireChannel(ctx, "ch-1")
	coord.AcquireChannel(ctx, "ch-2")

	coord.Stop()

	owned := coord.OwnedChannels()
	if len(owned) != 0 {
		t.Fatalf("expected 0 owned channels after stop, got %d", len(owned))
	}
}

func TestRedisClientMiniredisWithPoolSettings(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client, err := NewRedisClient(&config.RedisConfig{
		Address:      mr.Addr(),
		PoolSize:     50,
		MinIdleConns: 10,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	if client.Client() == nil {
		t.Fatal("expected non-nil client")
	}
}
