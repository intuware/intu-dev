package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/intuware/intu/pkg/config"
)

type MessageRecord struct {
	ID            string
	CorrelationID string
	ChannelID     string
	Stage         string
	Content       []byte
	Status        string
	Timestamp     time.Time
	DurationMs    int64          `json:"DurationMs,omitempty"`
	Metadata      map[string]any
}

type MessageStore interface {
	Save(record *MessageRecord) error
	Get(id string) (*MessageRecord, error)
	GetStage(id, stage string) (*MessageRecord, error)
	Query(opts QueryOpts) ([]*MessageRecord, error)
	Delete(id string) error
	Prune(before time.Time, channel string) (int, error)
}

type QueryOpts struct {
	ChannelID      string
	Status         string
	Stage          string
	Since          time.Time
	Before         time.Time
	Limit          int
	Offset         int
	ExcludeContent bool
}

func NewMessageStore(cfg *config.MessageStorageConfig) (MessageStore, error) {
	if cfg == nil {
		return NewMemoryStore(), nil
	}

	var inner MessageStore
	var err error

	switch cfg.Driver {
	case "", "memory":
		inner = NewMemoryStore()
	case "postgres":
		if cfg.Postgres == nil {
			return nil, fmt.Errorf("postgres config is required when driver is postgres")
		}
		inner, err = NewPostgresStore(cfg.Postgres)
		if err != nil {
			return nil, fmt.Errorf("create postgres store: %w", err)
		}
	case "s3":
		if cfg.S3 == nil {
			return nil, fmt.Errorf("s3 config is required when driver is s3")
		}
		inner, err = NewS3Store(cfg.S3)
		if err != nil {
			return nil, fmt.Errorf("create s3 store: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported message store driver: %s", cfg.Driver)
	}

	mode := cfg.Mode
	if mode != "" && mode != "full" {
		return NewCompositeStore(inner, mode, cfg.Stages), nil
	}
	if len(cfg.Stages) > 0 {
		return NewCompositeStore(inner, "full", cfg.Stages), nil
	}

	return inner, nil
}

type MemoryStore struct {
	mu      sync.RWMutex
	records map[string]*MessageRecord
	order   []string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		records: make(map[string]*MessageRecord),
	}
}

func (m *MemoryStore) Save(record *MessageRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := record.ID + "." + record.Stage
	if _, exists := m.records[key]; !exists {
		m.order = append(m.order, key)
	}
	m.records[key] = record
	return nil
}

func (m *MemoryStore) Get(id string) (*MessageRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, key := range m.order {
		if rec, ok := m.records[key]; ok && rec.ID == id {
			return rec, nil
		}
	}
	return nil, fmt.Errorf("message %s not found", id)
}

func (m *MemoryStore) GetStage(id, stage string) (*MessageRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := id + "." + stage
	if rec, ok := m.records[key]; ok {
		return rec, nil
	}
	return nil, fmt.Errorf("message %s stage %s not found", id, stage)
}

func (m *MemoryStore) Query(opts QueryOpts) ([]*MessageRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*MessageRecord
	skipped := 0

	for i := len(m.order) - 1; i >= 0; i-- {
		rec := m.records[m.order[i]]
		if opts.ChannelID != "" && rec.ChannelID != opts.ChannelID {
			continue
		}
		if opts.Status != "" && rec.Status != opts.Status {
			continue
		}
		if opts.Stage != "" && rec.Stage != opts.Stage {
			continue
		}
		if !opts.Since.IsZero() && rec.Timestamp.Before(opts.Since) {
			continue
		}
		if !opts.Before.IsZero() && rec.Timestamp.After(opts.Before) {
			continue
		}

		if opts.Offset > 0 && skipped < opts.Offset {
			skipped++
			continue
		}

		if opts.ExcludeContent {
			clone := *rec
			clone.Content = nil
			results = append(results, &clone)
		} else {
			results = append(results, rec)
		}
		if opts.Limit > 0 && len(results) >= opts.Limit {
			break
		}
	}

	return results, nil
}

func (m *MemoryStore) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var newOrder []string
	for _, key := range m.order {
		if rec, ok := m.records[key]; ok && rec.ID == id {
			delete(m.records, key)
			continue
		}
		newOrder = append(newOrder, key)
	}
	m.order = newOrder
	return nil
}

func (m *MemoryStore) Prune(before time.Time, channel string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pruned := 0
	var newOrder []string
	for _, key := range m.order {
		rec := m.records[key]
		if rec.Timestamp.Before(before) && (channel == "" || rec.ChannelID == channel) {
			delete(m.records, key)
			pruned++
			continue
		}
		newOrder = append(newOrder, key)
	}
	m.order = newOrder
	return pruned, nil
}
