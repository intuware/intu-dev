package observability

import (
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	mu       sync.RWMutex
	counters map[string]*atomic.Int64
	gauges   map[string]*atomic.Int64
	timings  map[string]*TimingStat
}

type TimingStat struct {
	mu    sync.Mutex
	count int64
	total time.Duration
	min   time.Duration
	max   time.Duration
}

func (ts *TimingStat) Record(d time.Duration) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.count++
	ts.total += d
	if ts.count == 1 || d < ts.min {
		ts.min = d
	}
	if d > ts.max {
		ts.max = d
	}
}

func (ts *TimingStat) Stats() (count int64, avg, min, max time.Duration) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.count == 0 {
		return 0, 0, 0, 0
	}
	return ts.count, ts.total / time.Duration(ts.count), ts.min, ts.max
}

var globalMetrics = NewMetrics()

func NewMetrics() *Metrics {
	return &Metrics{
		counters: make(map[string]*atomic.Int64),
		gauges:   make(map[string]*atomic.Int64),
		timings:  make(map[string]*TimingStat),
	}
}

func Global() *Metrics {
	return globalMetrics
}

func (m *Metrics) Counter(name string) *atomic.Int64 {
	m.mu.RLock()
	if c, ok := m.counters[name]; ok {
		m.mu.RUnlock()
		return c
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.counters[name]; ok {
		return c
	}
	c := &atomic.Int64{}
	m.counters[name] = c
	return c
}

func (m *Metrics) Gauge(name string) *atomic.Int64 {
	m.mu.RLock()
	if g, ok := m.gauges[name]; ok {
		m.mu.RUnlock()
		return g
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if g, ok := m.gauges[name]; ok {
		return g
	}
	g := &atomic.Int64{}
	m.gauges[name] = g
	return g
}

func (m *Metrics) Timing(name string) *TimingStat {
	m.mu.RLock()
	if t, ok := m.timings[name]; ok {
		m.mu.RUnlock()
		return t
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.timings[name]; ok {
		return t
	}
	t := &TimingStat{}
	m.timings[name] = t
	return t
}

func (m *Metrics) IncrReceived(channel string) {
	m.Counter("messages_received_total." + channel).Add(1)
}

func (m *Metrics) IncrProcessed(channel string) {
	m.Counter("messages_processed_total." + channel).Add(1)
}

func (m *Metrics) IncrErrored(channel, destination string) {
	m.Counter("messages_errored_total." + channel + "." + destination).Add(1)
}

func (m *Metrics) IncrFiltered(channel string) {
	m.Counter("messages_filtered_total." + channel).Add(1)
}

func (m *Metrics) RecordLatency(channel, stage string, d time.Duration) {
	m.Timing("processing_duration." + channel + "." + stage).Record(d)
}

func (m *Metrics) RecordDestLatency(channel, destination string, d time.Duration) {
	m.Timing("destination_latency." + channel + "." + destination).Record(d)
}

func (m *Metrics) Snapshot() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snap := make(map[string]any)
	counters := make(map[string]int64)
	for k, v := range m.counters {
		counters[k] = v.Load()
	}
	snap["counters"] = counters

	gauges := make(map[string]int64)
	for k, v := range m.gauges {
		gauges[k] = v.Load()
	}
	snap["gauges"] = gauges

	timings := make(map[string]map[string]any)
	for k, v := range m.timings {
		count, avg, min, max := v.Stats()
		timings[k] = map[string]any{
			"count": count,
			"avg":   avg.String(),
			"min":   min.String(),
			"max":   max.String(),
		}
	}
	snap["timings"] = timings

	return snap
}
