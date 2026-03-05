package auth

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/intuware/intu/pkg/config"
)

type AuditLogger struct {
	cfg     *config.AuditConfig
	logger  *slog.Logger
	mu      sync.Mutex
	entries []AuditEntry
}

type AuditEntry struct {
	Timestamp time.Time      `json:"timestamp"`
	Event     string         `json:"event"`
	User      string         `json:"user"`
	Details   map[string]any `json:"details,omitempty"`
	SourceIP  string         `json:"source_ip,omitempty"`
}

func NewAuditLogger(cfg *config.AuditConfig, logger *slog.Logger) *AuditLogger {
	return &AuditLogger{
		cfg:    cfg,
		logger: logger,
	}
}

func (al *AuditLogger) Log(event, user string, details map[string]any) {
	if al.cfg == nil || !al.cfg.Enabled {
		return
	}

	if !al.isTrackedEvent(event) {
		return
	}

	entry := AuditEntry{
		Timestamp: time.Now(),
		Event:     event,
		User:      user,
		Details:   details,
	}

	al.mu.Lock()
	al.entries = append(al.entries, entry)
	al.mu.Unlock()

	data, _ := json.Marshal(entry)
	al.logger.Info("audit", "entry", string(data))
}

func (al *AuditLogger) isTrackedEvent(event string) bool {
	if al.cfg == nil || len(al.cfg.Events) == 0 {
		return true
	}
	for _, e := range al.cfg.Events {
		if e == event {
			return true
		}
	}
	return false
}

func (al *AuditLogger) GetEntries(limit int) []AuditEntry {
	al.mu.Lock()
	defer al.mu.Unlock()

	if limit <= 0 || limit > len(al.entries) {
		result := make([]AuditEntry, len(al.entries))
		copy(result, al.entries)
		return result
	}

	start := len(al.entries) - limit
	result := make([]AuditEntry, limit)
	copy(result, al.entries[start:])
	return result
}
