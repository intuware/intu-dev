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
	Metadata      map[string]any
}

type MessageStore interface {
	Save(record *MessageRecord) error
	Get(id string) (*MessageRecord, error)
	Query(opts QueryOpts) ([]*MessageRecord, error)
	Delete(id string) error
	Prune(before time.Time, channel string) (int, error)
}

type QueryOpts struct {
	ChannelID string
	Status    string
	Since     time.Time
	Before    time.Time
	Limit     int
	Offset    int
}

func NewMessageStore(cfg *config.MessageStorageConfig) (MessageStore, error) {
	if cfg == nil {
		return NewMemoryStore(), nil
	}
	switch cfg.Driver {
	case "", "memory":
		return NewMemoryStore(), nil
	case "postgres":
		return nil, fmt.Errorf("postgres message store not yet implemented")
	case "s3":
		return nil, fmt.Errorf("s3 message store not yet implemented")
	default:
		return nil, fmt.Errorf("unsupported message store driver: %s", cfg.Driver)
	}
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

func (m *MemoryStore) Query(opts QueryOpts) ([]*MessageRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*MessageRecord
	skipped := 0

	for _, key := range m.order {
		rec := m.records[key]
		if opts.ChannelID != "" && rec.ChannelID != opts.ChannelID {
			continue
		}
		if opts.Status != "" && rec.Status != opts.Status {
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

		results = append(results, rec)
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
