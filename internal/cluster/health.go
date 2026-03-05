package cluster

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/intuware/intu/pkg/config"
)

type HealthChecker struct {
	cfg       *config.HealthConfig
	logger    *slog.Logger
	mu        sync.RWMutex
	status    HealthStatus
	channels  ChannelHealth
	startTime time.Time
}

type HealthStatus string

const (
	StatusHealthy   HealthStatus = "healthy"
	StatusDegraded  HealthStatus = "degraded"
	StatusUnhealthy HealthStatus = "unhealthy"
)

type ChannelHealth struct {
	Running int `json:"running"`
	Stopped int `json:"stopped"`
	Errored int `json:"errored"`
}

type HealthResponse struct {
	Status   HealthStatus  `json:"status"`
	Channels ChannelHealth `json:"channels"`
	Uptime   string        `json:"uptime"`
}

func NewHealthChecker(cfg *config.HealthConfig, logger *slog.Logger) *HealthChecker {
	if cfg == nil {
		cfg = &config.HealthConfig{
			Port:          8081,
			Path:          "/health",
			ReadinessPath: "/ready",
			LivenessPath:  "/live",
		}
	}
	return &HealthChecker{
		cfg:       cfg,
		logger:    logger,
		status:    StatusHealthy,
		startTime: time.Now(),
	}
}

func (hc *HealthChecker) Start() error {
	mux := http.NewServeMux()

	path := hc.cfg.Path
	if path == "" {
		path = "/health"
	}
	readinessPath := hc.cfg.ReadinessPath
	if readinessPath == "" {
		readinessPath = "/ready"
	}
	livenessPath := hc.cfg.LivenessPath
	if livenessPath == "" {
		livenessPath = "/live"
	}

	mux.HandleFunc(path, hc.handleHealth)
	mux.HandleFunc(readinessPath, hc.handleReadiness)
	mux.HandleFunc(livenessPath, hc.handleLiveness)

	addr := fmt.Sprintf(":%d", hc.cfg.Port)
	hc.logger.Info("health check server starting", "addr", addr)

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			hc.logger.Error("health check server error", "error", err)
		}
	}()

	return nil
}

func (hc *HealthChecker) UpdateChannels(running, stopped, errored int) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.channels = ChannelHealth{
		Running: running,
		Stopped: stopped,
		Errored: errored,
	}
	if errored > 0 {
		hc.status = StatusDegraded
	} else {
		hc.status = StatusHealthy
	}
}

func (hc *HealthChecker) SetStatus(status HealthStatus) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.status = status
}

func (hc *HealthChecker) handleHealth(w http.ResponseWriter, r *http.Request) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	resp := HealthResponse{
		Status:   hc.status,
		Channels: hc.channels,
		Uptime:   time.Since(hc.startTime).Truncate(time.Second).String(),
	}

	w.Header().Set("Content-Type", "application/json")
	if hc.status == StatusUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(resp)
}

func (hc *HealthChecker) handleReadiness(w http.ResponseWriter, r *http.Request) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	if hc.status == StatusUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"ready":false}`)
		return
	}
	fmt.Fprint(w, `{"ready":true}`)
}

func (hc *HealthChecker) handleLiveness(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, `{"alive":true}`)
}
