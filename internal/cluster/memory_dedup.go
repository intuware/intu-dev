package cluster

import (
	"sync"
	"time"
)

type Deduplicator struct {
	mu     sync.RWMutex
	seen   map[string]time.Time
	window time.Duration
}

func NewDeduplicator(window time.Duration) *Deduplicator {
	d := &Deduplicator{
		seen:   make(map[string]time.Time),
		window: window,
	}
	go d.cleanup()
	return d
}

func (d *Deduplicator) IsDuplicate(key string) bool {
	d.mu.RLock()
	if ts, ok := d.seen[key]; ok && time.Since(ts) < d.window {
		d.mu.RUnlock()
		return true
	}
	d.mu.RUnlock()

	d.mu.Lock()
	defer d.mu.Unlock()
	if ts, ok := d.seen[key]; ok && time.Since(ts) < d.window {
		return true
	}
	d.seen[key] = time.Now()
	return false
}

func (d *Deduplicator) cleanup() {
	ticker := time.NewTicker(d.window / 2)
	defer ticker.Stop()
	for range ticker.C {
		d.mu.Lock()
		now := time.Now()
		for key, ts := range d.seen {
			if now.Sub(ts) > d.window {
				delete(d.seen, key)
			}
		}
		d.mu.Unlock()
	}
}
