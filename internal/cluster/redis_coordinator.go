package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

type RedisCoordinator struct {
	client     *RedisClient
	cfg        *config.ClusterConfig
	logger     *slog.Logger
	instanceID string

	mu            sync.RWMutex
	ownedChannels map[string]bool
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	leaseDuration time.Duration
	renewInterval time.Duration
}

type redisInstanceInfo struct {
	InstanceID string   `json:"instance_id"`
	LastSeen   int64    `json:"last_seen"`
	Channels   []string `json:"channels"`
	Status     string   `json:"status"`
}

func NewRedisCoordinator(client *RedisClient, cfg *config.ClusterConfig, logger *slog.Logger) *RedisCoordinator {
	instanceID := "standalone"
	if cfg != nil && cfg.InstanceID != "" {
		instanceID = cfg.InstanceID
	}

	return &RedisCoordinator{
		client:        client,
		cfg:           cfg,
		logger:        logger,
		instanceID:    instanceID,
		ownedChannels: make(map[string]bool),
		leaseDuration: 30 * time.Second,
		renewInterval: 10 * time.Second,
	}
}

func (rc *RedisCoordinator) Start(ctx context.Context) error {
	ctx, rc.cancel = context.WithCancel(ctx)

	interval := 5 * time.Second
	if rc.cfg != nil && rc.cfg.HeartbeatInterval != "" {
		d, err := time.ParseDuration(rc.cfg.HeartbeatInterval)
		if err == nil {
			interval = d
		}
	}

	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rc.heartbeat(ctx)
			}
		}
	}()

	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		ticker := time.NewTicker(rc.renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rc.renewAllLeases(ctx)
			}
		}
	}()

	rc.heartbeat(ctx)

	rc.logger.Info("redis coordinator started",
		"instanceID", rc.instanceID,
	)
	return nil
}

func (rc *RedisCoordinator) Stop() {
	ctx := context.Background()
	rc.releaseAllChannels(ctx)
	rc.removeInstance(ctx)

	if rc.cancel != nil {
		rc.cancel()
	}
	rc.wg.Wait()
}

func (rc *RedisCoordinator) heartbeat(ctx context.Context) {
	rc.mu.RLock()
	channels := make([]string, 0, len(rc.ownedChannels))
	for ch := range rc.ownedChannels {
		channels = append(channels, ch)
	}
	rc.mu.RUnlock()

	info := redisInstanceInfo{
		InstanceID: rc.instanceID,
		LastSeen:   time.Now().UnixMilli(),
		Channels:   channels,
		Status:     "active",
	}

	data, err := json.Marshal(info)
	if err != nil {
		rc.logger.Error("failed to marshal instance info", "error", err)
		return
	}

	key := rc.client.Key("instances", rc.instanceID)
	err = rc.client.Client().Set(ctx, key, data, rc.leaseDuration).Err()
	if err != nil {
		rc.logger.Error("heartbeat failed", "error", err)
	}
}

func (rc *RedisCoordinator) removeInstance(ctx context.Context) {
	key := rc.client.Key("instances", rc.instanceID)
	rc.client.Client().Del(ctx, key)
}

func (rc *RedisCoordinator) GetPeers() []PeerInfo {
	ctx := context.Background()
	pattern := rc.client.Key("instances", "*")

	keys, err := rc.client.Client().Keys(ctx, pattern).Result()
	if err != nil {
		rc.logger.Error("failed to get peers", "error", err)
		return nil
	}

	var peers []PeerInfo
	for _, key := range keys {
		data, err := rc.client.Client().Get(ctx, key).Bytes()
		if err != nil {
			continue
		}

		var info redisInstanceInfo
		if err := json.Unmarshal(data, &info); err != nil {
			continue
		}

		peers = append(peers, PeerInfo{
			InstanceID: info.InstanceID,
			LastSeen:   time.UnixMilli(info.LastSeen),
			Channels:   info.Channels,
			Status:     info.Status,
		})
	}

	return peers
}

func (rc *RedisCoordinator) InstanceID() string {
	return rc.instanceID
}

func (rc *RedisCoordinator) IsLeader() bool {
	peers := rc.GetPeers()
	if len(peers) == 0 {
		return true
	}

	ids := make([]string, len(peers))
	for i, p := range peers {
		ids[i] = p.InstanceID
	}
	sort.Strings(ids)

	return ids[0] == rc.instanceID
}

func (rc *RedisCoordinator) AcquireChannel(ctx context.Context, channelID string) (bool, error) {
	key := rc.client.Key("channel", channelID, "owner")

	ok, err := rc.client.Client().SetNX(ctx, key, rc.instanceID, rc.leaseDuration).Result()
	if err != nil {
		return false, fmt.Errorf("acquire channel %s: %w", channelID, err)
	}

	if ok {
		rc.mu.Lock()
		rc.ownedChannels[channelID] = true
		rc.mu.Unlock()
		rc.logger.Info("acquired channel", "channel", channelID, "instance", rc.instanceID)
	}

	return ok, nil
}

func (rc *RedisCoordinator) RenewChannelLease(ctx context.Context, channelID string) error {
	key := rc.client.Key("channel", channelID, "owner")

	owner, err := rc.client.Client().Get(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("get channel owner: %w", err)
	}
	if owner != rc.instanceID {
		rc.mu.Lock()
		delete(rc.ownedChannels, channelID)
		rc.mu.Unlock()
		return fmt.Errorf("channel %s owned by %s, not %s", channelID, owner, rc.instanceID)
	}

	return rc.client.Client().Expire(ctx, key, rc.leaseDuration).Err()
}

func (rc *RedisCoordinator) ReleaseChannel(ctx context.Context, channelID string) error {
	key := rc.client.Key("channel", channelID, "owner")

	owner, err := rc.client.Client().Get(ctx, key).Result()
	if err != nil {
		rc.mu.Lock()
		delete(rc.ownedChannels, channelID)
		rc.mu.Unlock()
		return nil
	}

	if owner != rc.instanceID {
		rc.mu.Lock()
		delete(rc.ownedChannels, channelID)
		rc.mu.Unlock()
		return nil
	}

	err = rc.client.Client().Del(ctx, key).Err()
	rc.mu.Lock()
	delete(rc.ownedChannels, channelID)
	rc.mu.Unlock()

	if err != nil {
		return fmt.Errorf("release channel %s: %w", channelID, err)
	}

	rc.logger.Info("released channel", "channel", channelID, "instance", rc.instanceID)
	return nil
}

func (rc *RedisCoordinator) OwnedChannels() []string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	result := make([]string, 0, len(rc.ownedChannels))
	for ch := range rc.ownedChannels {
		result = append(result, ch)
	}
	return result
}

func (rc *RedisCoordinator) renewAllLeases(ctx context.Context) {
	rc.mu.RLock()
	channels := make([]string, 0, len(rc.ownedChannels))
	for ch := range rc.ownedChannels {
		channels = append(channels, ch)
	}
	rc.mu.RUnlock()

	for _, ch := range channels {
		if err := rc.RenewChannelLease(ctx, ch); err != nil {
			rc.logger.Warn("lease renewal failed", "channel", ch, "error", err)
		}
	}
}

func (rc *RedisCoordinator) releaseAllChannels(ctx context.Context) {
	rc.mu.RLock()
	channels := make([]string, 0, len(rc.ownedChannels))
	for ch := range rc.ownedChannels {
		channels = append(channels, ch)
	}
	rc.mu.RUnlock()

	for _, ch := range channels {
		if err := rc.ReleaseChannel(ctx, ch); err != nil {
			rc.logger.Warn("failed to release channel on shutdown", "channel", ch, "error", err)
		}
	}
}

func (rc *RedisCoordinator) ShouldAcquireChannel(channelID string, tags []string) bool {
	if rc.cfg == nil || rc.cfg.ChannelAssignment == nil {
		return true
	}

	strategy := rc.cfg.ChannelAssignment.Strategy
	if strategy == "" || strategy == "auto" {
		return true
	}

	if strategy == "tag-based" && rc.cfg.ChannelAssignment.TagAffinity != nil {
		allowedTags, ok := rc.cfg.ChannelAssignment.TagAffinity[rc.instanceID]
		if !ok {
			return true
		}
		tagSet := make(map[string]bool, len(allowedTags))
		for _, t := range allowedTags {
			tagSet[t] = true
		}
		for _, t := range tags {
			if tagSet[t] {
				return true
			}
		}
		return false
	}

	return true
}
