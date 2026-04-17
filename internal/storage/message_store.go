package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/intuware/intu-dev/pkg/config"
)

type MessageRecord struct {
	ID            string
	CorrelationID string
	ChannelID     string
	Stage         string
	Content       []byte
	ContentSize   int    `json:"ContentSize,omitempty"`
	Status        string
	Timestamp     time.Time
	DurationMs    int64 `json:"DurationMs,omitempty"`
	Metadata      map[string]any
}

type MessageStore interface {
	Save(record *MessageRecord) error
	Get(id string) (*MessageRecord, error)
	GetStage(id, stage string) (*MessageRecord, error)
	Query(opts QueryOpts) ([]*MessageRecord, error)
	Count(opts QueryOpts) (int64, error)
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

const (
	defaultMaxRecords = 100000
	defaultMaxBytes   = 512 * 1024 * 1024 // 512 MB
)

func NewMessageStore(cfg *config.MessageStorageConfig) (MessageStore, error) {
	if cfg == nil {
		return NewMemoryStore(0, 0), nil
	}

	var inner MessageStore
	var err error

	switch cfg.Driver {
	case "", "memory":
		var maxRec, maxBytes int
		if cfg.Memory != nil {
			maxRec = cfg.Memory.MaxRecords
			maxBytes = cfg.Memory.MaxBytes
		}
		inner = NewMemoryStore(maxRec, maxBytes)
	case "postgres":
		if cfg.Postgres == nil {
			return nil, fmt.Errorf("postgres config is required when driver is postgres")
		}
		inner, err = NewPostgresStore(cfg.Postgres)
		if err != nil {
			return nil, fmt.Errorf("create postgres store: %w", err)
		}
	case "mysql", "mssql", "sqlserver", "sqlite", "sqlite3":
		dsn, prefix, maxOpen, maxIdle := resolveSQLStorageConfig(cfg)
		if dsn == "" {
			return nil, fmt.Errorf("database dsn is required when driver is %s", cfg.Driver)
		}
		inner, err = NewSQLStore(cfg.Driver, dsn, prefix, maxOpen, maxIdle)
		if err != nil {
			return nil, fmt.Errorf("create %s store: %w", cfg.Driver, err)
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

// resolveSQLStorageConfig extracts DSN and pool settings from config,
// checking the generic Database block first, then the top-level DSN field.
func resolveSQLStorageConfig(cfg *config.MessageStorageConfig) (dsn, prefix string, maxOpen, maxIdle int) {
	if cfg.Database != nil {
		return cfg.Database.DSN, cfg.Database.TablePrefix, cfg.Database.MaxOpenConns, cfg.Database.MaxIdleConns
	}
	return cfg.DSN, "", 0, 0
}

type MemoryStore struct {
	mu         sync.RWMutex
	records    map[string]*MessageRecord
	order      []string
	totalBytes int
	maxRecords int
	maxBytes   int
}

func NewMemoryStore(maxRecords, maxBytes int) *MemoryStore {
	if maxRecords <= 0 {
		maxRecords = defaultMaxRecords
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}
	return &MemoryStore{
		records:    make(map[string]*MessageRecord),
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
	}
}

func recordSize(rec *MessageRecord) int {
	n := len(rec.ID) + len(rec.CorrelationID) + len(rec.ChannelID) +
		len(rec.Stage) + len(rec.Content) + len(rec.Status) + 64
	return n
}

func (m *MemoryStore) evictOldest() {
	if len(m.order) == 0 {
		return
	}
	key := m.order[0]
	m.order = m.order[1:]
	if rec, ok := m.records[key]; ok {
		m.totalBytes -= recordSize(rec)
		delete(m.records, key)
	}
}

func (m *MemoryStore) Save(record *MessageRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if record.ContentSize == 0 && len(record.Content) > 0 {
		record.ContentSize = len(record.Content)
	}

	key := record.ID + "." + record.Stage
	if existing, exists := m.records[key]; exists {
		m.totalBytes -= recordSize(existing)
	} else {
		m.order = append(m.order, key)
	}

	size := recordSize(record)
	m.records[key] = record
	m.totalBytes += size

	for len(m.records) > m.maxRecords || m.totalBytes > m.maxBytes {
		if len(m.order) <= 1 {
			break
		}
		evictKey := m.order[0]
		if evictKey == key {
			break
		}
		m.evictOldest()
	}

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
		if rec == nil {
			continue
		}
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

func (m *MemoryStore) Count(opts QueryOpts) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int64
	for i := len(m.order) - 1; i >= 0; i-- {
		rec := m.records[m.order[i]]
		if rec == nil {
			continue
		}
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
		count++
	}
	return count, nil
}

func (m *MemoryStore) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var newOrder []string
	for _, key := range m.order {
		if rec, ok := m.records[key]; ok && rec.ID == id {
			m.totalBytes -= recordSize(rec)
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
			m.totalBytes -= recordSize(rec)
			delete(m.records, key)
			pruned++
			continue
		}
		newOrder = append(newOrder, key)
	}
	m.order = newOrder
	return pruned, nil
}

func (m *MemoryStore) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.records)
}

func (m *MemoryStore) BytesUsed() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.totalBytes
}
