package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/intuware/intu-dev/internal/dbutil"
)

// SQLStore is a database-backed MessageStore that works with any SQL
// backend supported by the dbutil.Dialect abstraction.
type SQLStore struct {
	db          *sql.DB
	tablePrefix string
	dialect     dbutil.Dialect
}

// NewSQLStore opens a connection using the given config-level driver name
// and DSN, then ensures the messages table exists.
func NewSQLStore(driver, dsn, tablePrefix string, maxOpen, maxIdle int) (*SQLStore, error) {
	if dsn == "" {
		return nil, dbutil.ErrDSNRequired
	}

	dialect, err := dbutil.DialectFor(driver)
	if err != nil {
		return nil, err
	}

	db, err := dbutil.OpenDB(driver, dsn, &dbutil.DBOptions{
		MaxOpenConns: maxOpen,
		MaxIdleConns: maxIdle,
	})
	if err != nil {
		return nil, fmt.Errorf("open database connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("database ping failed: %w", err)
	}

	if tablePrefix == "" {
		tablePrefix = "intu_"
	}

	store := &SQLStore{
		db:          db,
		tablePrefix: tablePrefix,
		dialect:     dialect,
	}

	if err := store.ensureTable(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ensure table: %w", err)
	}

	return store, nil
}

func (s *SQLStore) tableName() string {
	return s.tablePrefix + "messages"
}

func (s *SQLStore) ensureTable() error {
	table := s.tableName()
	query := s.dialect.CreateMessagesTable(table)
	if _, err := s.db.Exec(query); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	for _, m := range s.dialect.MigrateColumns(table) {
		s.db.Exec(m) // best-effort migration for existing tables
	}

	for _, idx := range s.dialect.CreateIndexes(table, s.tablePrefix) {
		if _, err := s.db.Exec(idx); err != nil {
			return fmt.Errorf("create index: %w", err)
		}
	}

	return nil
}

func (s *SQLStore) Save(record *MessageRecord) error {
	metadataJSON, err := json.Marshal(record.Metadata)
	if err != nil {
		metadataJSON = []byte("{}")
	}

	contentSize := record.ContentSize
	if contentSize == 0 && len(record.Content) > 0 {
		contentSize = len(record.Content)
	}

	query := s.dialect.UpsertMessage(s.tableName())

	_, err = s.db.Exec(query,
		record.ID,
		record.CorrelationID,
		record.ChannelID,
		record.Stage,
		record.Content,
		contentSize,
		record.Status,
		record.Timestamp,
		record.DurationMs,
		metadataJSON,
	)
	if err != nil {
		return fmt.Errorf("save message: %w", err)
	}
	return nil
}

func (s *SQLStore) Get(id string) (*MessageRecord, error) {
	p := s.dialect.Placeholder
	query := fmt.Sprintf(`
		SELECT id, correlation_id, channel_id, stage, content, content_size, status, timestamp, duration_ms, metadata
		FROM %s WHERE id = %s
		ORDER BY timestamp DESC LIMIT 1`, s.tableName(), p(1))

	row := s.db.QueryRow(query, id)
	return s.scanRecord(row)
}

func (s *SQLStore) GetStage(id, stage string) (*MessageRecord, error) {
	p := s.dialect.Placeholder
	query := fmt.Sprintf(`
		SELECT id, correlation_id, channel_id, stage, content, content_size, status, timestamp, duration_ms, metadata
		FROM %s WHERE id = %s AND stage = %s
		LIMIT 1`, s.tableName(), p(1), p(2))

	row := s.db.QueryRow(query, id, stage)
	return s.scanRecord(row)
}

func (s *SQLStore) Query(opts QueryOpts) ([]*MessageRecord, error) {
	p := s.dialect.Placeholder
	var conditions []string
	var args []any
	argIdx := 1

	if opts.ChannelID != "" {
		conditions = append(conditions, fmt.Sprintf("channel_id = %s", p(argIdx)))
		args = append(args, opts.ChannelID)
		argIdx++
	}
	if opts.Status != "" {
		conditions = append(conditions, fmt.Sprintf("status = %s", p(argIdx)))
		args = append(args, opts.Status)
		argIdx++
	}
	if opts.Stage != "" {
		conditions = append(conditions, fmt.Sprintf("stage = %s", p(argIdx)))
		args = append(args, opts.Stage)
		argIdx++
	}
	if !opts.Since.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= %s", p(argIdx)))
		args = append(args, opts.Since)
		argIdx++
	}
	if !opts.Before.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= %s", p(argIdx)))
		args = append(args, opts.Before)
		argIdx++
	}

	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}

	columns := "id, correlation_id, channel_id, stage, content, content_size, status, timestamp, duration_ms, metadata"
	if opts.ExcludeContent {
		columns = "id, correlation_id, channel_id, stage, NULL AS content, content_size, status, timestamp, duration_ms, metadata"
	}

	query := fmt.Sprintf("SELECT %s FROM %s%s ORDER BY timestamp DESC",
		columns, s.tableName(), where)

	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}
	if opts.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", opts.Offset)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()

	var records []*MessageRecord
	for rows.Next() {
		rec, err := s.scanRows(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	return records, rows.Err()
}

func (s *SQLStore) Count(opts QueryOpts) (int64, error) {
	p := s.dialect.Placeholder
	var conditions []string
	var args []any
	argIdx := 1

	if opts.ChannelID != "" {
		conditions = append(conditions, fmt.Sprintf("channel_id = %s", p(argIdx)))
		args = append(args, opts.ChannelID)
		argIdx++
	}
	if opts.Status != "" {
		conditions = append(conditions, fmt.Sprintf("status = %s", p(argIdx)))
		args = append(args, opts.Status)
		argIdx++
	}
	if opts.Stage != "" {
		conditions = append(conditions, fmt.Sprintf("stage = %s", p(argIdx)))
		args = append(args, opts.Stage)
		argIdx++
	}
	if !opts.Since.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp >= %s", p(argIdx)))
		args = append(args, opts.Since)
		argIdx++
	}
	if !opts.Before.IsZero() {
		conditions = append(conditions, fmt.Sprintf("timestamp <= %s", p(argIdx)))
		args = append(args, opts.Before)
		argIdx++
	}

	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", s.tableName(), where)
	var count int64
	err := s.db.QueryRow(query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count messages: %w", err)
	}
	return count, nil
}

func (s *SQLStore) Delete(id string) error {
	p := s.dialect.Placeholder
	query := fmt.Sprintf("DELETE FROM %s WHERE id = %s", s.tableName(), p(1))
	_, err := s.db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	return nil
}

func (s *SQLStore) Prune(before time.Time, channel string) (int, error) {
	p := s.dialect.Placeholder
	var query string
	var args []any

	if channel != "" {
		query = fmt.Sprintf("DELETE FROM %s WHERE timestamp < %s AND channel_id = %s", s.tableName(), p(1), p(2))
		args = []any{before, channel}
	} else {
		query = fmt.Sprintf("DELETE FROM %s WHERE timestamp < %s", s.tableName(), p(1))
		args = []any{before}
	}

	result, err := s.db.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("prune messages: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return int(count), nil
}

func (s *SQLStore) Close() error {
	return s.db.Close()
}

func (s *SQLStore) scanRecord(row *sql.Row) (*MessageRecord, error) {
	var rec MessageRecord
	var metadataJSON []byte
	var correlationID sql.NullString

	err := row.Scan(
		&rec.ID,
		&correlationID,
		&rec.ChannelID,
		&rec.Stage,
		&rec.Content,
		&rec.ContentSize,
		&rec.Status,
		&rec.Timestamp,
		&rec.DurationMs,
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

func (s *SQLStore) scanRows(rows *sql.Rows) (*MessageRecord, error) {
	var rec MessageRecord
	var metadataJSON []byte
	var correlationID sql.NullString

	err := rows.Scan(
		&rec.ID,
		&correlationID,
		&rec.ChannelID,
		&rec.Stage,
		&rec.Content,
		&rec.ContentSize,
		&rec.Status,
		&rec.Timestamp,
		&rec.DurationMs,
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
