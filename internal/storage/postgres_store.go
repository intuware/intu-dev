package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/intuware/intu/pkg/config"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresStore struct {
	db          *sql.DB
	tablePrefix string
}

func NewPostgresStore(cfg *config.StoragePostgresConfig) (*PostgresStore, error) {
	if cfg == nil || cfg.DSN == "" {
		return nil, fmt.Errorf("postgres DSN is required")
	}

	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open postgres connection: %w", err)
	}

	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 25
	}
	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 5
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres ping failed: %w", err)
	}

	prefix := cfg.TablePrefix
	if prefix == "" {
		prefix = "intu_"
	}

	store := &PostgresStore{
		db:          db,
		tablePrefix: prefix,
	}

	if err := store.ensureTable(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ensure table: %w", err)
	}

	return store, nil
}

func (p *PostgresStore) tableName() string {
	return p.tablePrefix + "messages"
}

func (p *PostgresStore) ensureTable() error {
	table := p.tableName()
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT NOT NULL,
			correlation_id TEXT,
			channel_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			content BYTEA,
			status TEXT NOT NULL,
			timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			metadata JSONB,
			PRIMARY KEY (id, stage)
		)`, table)
	if _, err := p.db.Exec(query); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%smsg_channel_status ON %s (channel_id, status)", p.tablePrefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%smsg_channel_ts ON %s (channel_id, timestamp)", p.tablePrefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%smsg_ts ON %s (timestamp)", p.tablePrefix, table),
	}
	for _, idx := range indexes {
		if _, err := p.db.Exec(idx); err != nil {
			return fmt.Errorf("create index: %w", err)
		}
	}

	return nil
}

func (p *PostgresStore) Save(record *MessageRecord) error {
	metadataJSON, err := json.Marshal(record.Metadata)
	if err != nil {
		metadataJSON = []byte("{}")
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (id, correlation_id, channel_id, stage, content, status, timestamp, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id, stage) DO UPDATE SET
			correlation_id = EXCLUDED.correlation_id,
			content = EXCLUDED.content,
			status = EXCLUDED.status,
			timestamp = EXCLUDED.timestamp,
			metadata = EXCLUDED.metadata`, p.tableName())

	_, err = p.db.Exec(query,
		record.ID,
		record.CorrelationID,
		record.ChannelID,
		record.Stage,
		record.Content,
		record.Status,
		record.Timestamp,
		metadataJSON,
	)
	if err != nil {
		return fmt.Errorf("save message: %w", err)
	}
	return nil
}

func (p *PostgresStore) Get(id string) (*MessageRecord, error) {
	query := fmt.Sprintf(`
		SELECT id, correlation_id, channel_id, stage, content, status, timestamp, metadata
		FROM %s WHERE id = $1
		ORDER BY timestamp DESC LIMIT 1`, p.tableName())

	row := p.db.QueryRow(query, id)
	return p.scanRecord(row)
}

func (p *PostgresStore) Query(opts QueryOpts) ([]*MessageRecord, error) {
	var conditions []string
	var args []any
	argIdx := 1

	if opts.ChannelID != "" {
		conditions = append(conditions, fmt.Sprintf("channel_id = $%d", argIdx))
		args = append(args, opts.ChannelID)
		argIdx++
	}
	if opts.Status != "" {
		conditions = append(conditions, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, opts.Status)
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
		where = " WHERE " + strings.Join(conditions, " AND ")
	}

	query := fmt.Sprintf("SELECT id, correlation_id, channel_id, stage, content, status, timestamp, metadata FROM %s%s ORDER BY timestamp DESC",
		p.tableName(), where)

	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}
	if opts.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", opts.Offset)
	}

	rows, err := p.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()

	var records []*MessageRecord
	for rows.Next() {
		rec, err := p.scanRows(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	return records, rows.Err()
}

func (p *PostgresStore) Delete(id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", p.tableName())
	_, err := p.db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	return nil
}

func (p *PostgresStore) Prune(before time.Time, channel string) (int, error) {
	var query string
	var args []any

	if channel != "" {
		query = fmt.Sprintf("DELETE FROM %s WHERE timestamp < $1 AND channel_id = $2", p.tableName())
		args = []any{before, channel}
	} else {
		query = fmt.Sprintf("DELETE FROM %s WHERE timestamp < $1", p.tableName())
		args = []any{before}
	}

	result, err := p.db.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("prune messages: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return int(count), nil
}

func (p *PostgresStore) Close() error {
	return p.db.Close()
}

func (p *PostgresStore) scanRecord(row *sql.Row) (*MessageRecord, error) {
	var rec MessageRecord
	var metadataJSON []byte
	var correlationID sql.NullString

	err := row.Scan(
		&rec.ID,
		&correlationID,
		&rec.ChannelID,
		&rec.Stage,
		&rec.Content,
		&rec.Status,
		&rec.Timestamp,
		&metadataJSON,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("message not found")
		}
		return nil, fmt.Errorf("scan record: %w", err)
	}

	if correlationID.Valid {
		rec.CorrelationID = correlationID.String
	}
	if len(metadataJSON) > 0 {
		_ = json.Unmarshal(metadataJSON, &rec.Metadata)
	}
	return &rec, nil
}

func (p *PostgresStore) scanRows(rows *sql.Rows) (*MessageRecord, error) {
	var rec MessageRecord
	var metadataJSON []byte
	var correlationID sql.NullString

	err := rows.Scan(
		&rec.ID,
		&correlationID,
		&rec.ChannelID,
		&rec.Stage,
		&rec.Content,
		&rec.Status,
		&rec.Timestamp,
		&metadataJSON,
	)
	if err != nil {
		return nil, fmt.Errorf("scan record: %w", err)
	}

	if correlationID.Valid {
		rec.CorrelationID = correlationID.String
	}
	if len(metadataJSON) > 0 {
		_ = json.Unmarshal(metadataJSON, &rec.Metadata)
	}
	return &rec, nil
}
