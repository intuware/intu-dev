package auth

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/intuware/intu/pkg/config"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type AuditStore interface {
	Save(entry *AuditEntry) error
	Query(opts AuditQueryOpts) ([]AuditEntry, error)
	Close() error
}

type AuditQueryOpts struct {
	Event  string
	User   string
	Since  time.Time
	Before time.Time
	Limit  int
	Offset int
}

type AuditLogger struct {
	cfg     *config.AuditConfig
	logger  *slog.Logger
	mu      sync.Mutex
	entries []AuditEntry
	store   AuditStore
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

func (al *AuditLogger) SetStore(store AuditStore) {
	al.mu.Lock()
	defer al.mu.Unlock()
	al.store = store
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
	store := al.store
	al.mu.Unlock()

	if store != nil {
		if err := store.Save(&entry); err != nil {
			al.logger.Warn("failed to persist audit entry", "error", err)
		}
	}

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

	if al.store != nil {
		entries, err := al.store.Query(AuditQueryOpts{Limit: limit})
		if err == nil {
			return entries
		}
		al.logger.Warn("audit store query failed, falling back to in-memory", "error", err)
	}

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

func (al *AuditLogger) Close() error {
	al.mu.Lock()
	defer al.mu.Unlock()
	if al.store != nil {
		return al.store.Close()
	}
	return nil
}

type MemoryAuditStore struct {
	mu      sync.RWMutex
	entries []AuditEntry
}

func NewMemoryAuditStore() *MemoryAuditStore {
	return &MemoryAuditStore{}
}

func (m *MemoryAuditStore) Save(entry *AuditEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, *entry)
	return nil
}

func (m *MemoryAuditStore) Query(opts AuditQueryOpts) ([]AuditEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []AuditEntry
	skipped := 0

	for i := len(m.entries) - 1; i >= 0; i-- {
		e := m.entries[i]
		if opts.Event != "" && e.Event != opts.Event {
			continue
		}
		if opts.User != "" && e.User != opts.User {
			continue
		}
		if !opts.Since.IsZero() && e.Timestamp.Before(opts.Since) {
			continue
		}
		if !opts.Before.IsZero() && e.Timestamp.After(opts.Before) {
			continue
		}
		if opts.Offset > 0 && skipped < opts.Offset {
			skipped++
			continue
		}
		results = append(results, e)
		if opts.Limit > 0 && len(results) >= opts.Limit {
			break
		}
	}
	return results, nil
}

func (m *MemoryAuditStore) Close() error {
	return nil
}

type PostgresAuditStore struct {
	db          *sql.DB
	tablePrefix string
}

func NewPostgresAuditStore(dsn, tablePrefix string) (*PostgresAuditStore, error) {
	if dsn == "" {
		return nil, fmt.Errorf("postgres DSN is required for audit store")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres connection: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(3)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres ping failed: %w", err)
	}

	if tablePrefix == "" {
		tablePrefix = "intu_"
	}

	store := &PostgresAuditStore{
		db:          db,
		tablePrefix: tablePrefix,
	}

	if err := store.ensureTable(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ensure audit table: %w", err)
	}

	return store, nil
}

func (p *PostgresAuditStore) tableName() string {
	return p.tablePrefix + "audit_log"
}

func (p *PostgresAuditStore) ensureTable() error {
	table := p.tableName()
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			event TEXT NOT NULL,
			username TEXT NOT NULL,
			details JSONB,
			source_ip TEXT
		)`, table)
	if _, err := p.db.Exec(query); err != nil {
		return fmt.Errorf("create audit table: %w", err)
	}

	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%saudit_event ON %s (event)", p.tablePrefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%saudit_user ON %s (username)", p.tablePrefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%saudit_ts ON %s (timestamp)", p.tablePrefix, table),
	}
	for _, idx := range indexes {
		if _, err := p.db.Exec(idx); err != nil {
			return fmt.Errorf("create audit index: %w", err)
		}
	}

	return nil
}

func (p *PostgresAuditStore) Save(entry *AuditEntry) error {
	detailsJSON, err := json.Marshal(entry.Details)
	if err != nil {
		detailsJSON = []byte("{}")
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (timestamp, event, username, details, source_ip)
		VALUES ($1, $2, $3, $4, $5)`, p.tableName())

	_, err = p.db.Exec(query,
		entry.Timestamp,
		entry.Event,
		entry.User,
		detailsJSON,
		entry.SourceIP,
	)
	if err != nil {
		return fmt.Errorf("save audit entry: %w", err)
	}
	return nil
}

func (p *PostgresAuditStore) Query(opts AuditQueryOpts) ([]AuditEntry, error) {
	var conditions []string
	var args []any
	argIdx := 1

	if opts.Event != "" {
		conditions = append(conditions, fmt.Sprintf("event = $%d", argIdx))
		args = append(args, opts.Event)
		argIdx++
	}
	if opts.User != "" {
		conditions = append(conditions, fmt.Sprintf("username = $%d", argIdx))
		args = append(args, opts.User)
		argIdx++
	}
	if !opts.Since.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", argIdx))
		args = append(args, opts.Since)
		argIdx++
	}
	if !opts.Before.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= $%d", argIdx))
		args = append(args, opts.Before)
		argIdx++
	}

	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + joinStrings(conditions, " AND ")
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf("SELECT timestamp, event, username, details, source_ip FROM %s%s ORDER BY timestamp DESC LIMIT %d",
		p.tableName(), where, limit)

	if opts.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", opts.Offset)
	}

	rows, err := p.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query audit log: %w", err)
	}
	defer rows.Close()

	var entries []AuditEntry
	for rows.Next() {
		var entry AuditEntry
		var detailsJSON []byte
		var sourceIP sql.NullString

		if err := rows.Scan(&entry.Timestamp, &entry.Event, &entry.User, &detailsJSON, &sourceIP); err != nil {
			return nil, fmt.Errorf("scan audit entry: %w", err)
		}

		if len(detailsJSON) > 0 {
			_ = json.Unmarshal(detailsJSON, &entry.Details)
		}
		if sourceIP.Valid {
			entry.SourceIP = sourceIP.String
		}

		entries = append(entries, entry)
	}

	return entries, rows.Err()
}

func (p *PostgresAuditStore) Close() error {
	return p.db.Close()
}

func joinStrings(parts []string, sep string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += sep
		}
		result += p
	}
	return result
}
