package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/intuware/intu/pkg/config"
)

type Coordinator struct {
	cfg        *config.ClusterConfig
	logger     *slog.Logger
	instanceID string
	mu         sync.RWMutex
	peers      map[string]PeerInfo
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

type PeerInfo struct {
	InstanceID string    `json:"instance_id"`
	LastSeen   time.Time `json:"last_seen"`
	Channels   []string  `json:"channels"`
	Status     string    `json:"status"`
}

func NewCoordinator(cfg *config.ClusterConfig, logger *slog.Logger) *Coordinator {
	instanceID := "standalone"
	if cfg != nil && cfg.InstanceID != "" {
		instanceID = cfg.InstanceID
	}
	return &Coordinator{
		cfg:        cfg,
		logger:     logger,
		instanceID: instanceID,
		peers:      make(map[string]PeerInfo),
	}
}

func (c *Coordinator) Start(ctx context.Context) error {
	if c.cfg == nil || !c.cfg.Enabled {
		c.logger.Info("cluster mode disabled, running standalone")
		return nil
	}

	ctx, c.cancel = context.WithCancel(ctx)

	interval := 5 * time.Second
	if c.cfg.HeartbeatInterval != "" {
		d, err := time.ParseDuration(c.cfg.HeartbeatInterval)
		if err == nil {
			interval = d
		}
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.heartbeat()
			}
		}
	}()

	c.logger.Info("cluster coordinator started",
		"instanceID", c.instanceID,
		"mode", c.cfg.Mode,
	)
	return nil
}

func (c *Coordinator) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

func (c *Coordinator) heartbeat() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.peers[c.instanceID] = PeerInfo{
		InstanceID: c.instanceID,
		LastSeen:   time.Now(),
		Status:     "active",
	}

	staleThreshold := 30 * time.Second
	for id, peer := range c.peers {
		if time.Since(peer.LastSeen) > staleThreshold && id != c.instanceID {
			c.logger.Warn("peer stale, removing", "peer", id)
			delete(c.peers, id)
		}
	}
}

func (c *Coordinator) GetPeers() []PeerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var result []PeerInfo
	for _, p := range c.peers {
		result = append(result, p)
	}
	return result
}

func (c *Coordinator) InstanceID() string {
	return c.instanceID
}

func (c *Coordinator) IsLeader() bool {
	if c.cfg == nil || !c.cfg.Enabled {
		return true
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for id := range c.peers {
		if id < c.instanceID {
			return false
		}
	}
	return true
}
