package dbutil

import (
	"fmt"
	"strings"
)

// Dialect abstracts database-specific SQL generation so that storage and
// audit implementations can work across Postgres, MySQL, SQLite, and MSSQL.
type Dialect interface {
	// DriverName returns the database/sql registered driver name.
	DriverName() string

	// Placeholder returns the n-th parameter placeholder (1-indexed).
	Placeholder(index int) string

	// CreateMessagesTable returns DDL for the messages table.
	CreateMessagesTable(table string) string

	// CreateAuditTable returns DDL for the audit log table.
	CreateAuditTable(table string) string

	// UpsertMessage returns an upsert statement for the messages table.
	// The statement must accept the same parameter order as the Postgres
	// original: id, correlation_id, channel_id, stage, content,
	// content_size, status, timestamp, duration_ms, metadata.
	UpsertMessage(table string) string

	// InsertAudit returns an INSERT for the audit table.
	// Parameters: timestamp, event, username, details, source_ip.
	InsertAudit(table string) string

	// MigrateColumns returns best-effort ALTER TABLE statements to add
	// columns that may be missing in older schemas.
	MigrateColumns(table string) []string

	// CreateIndexes returns CREATE INDEX statements for the messages table.
	CreateIndexes(table, prefix string) []string

	// CreateAuditIndexes returns CREATE INDEX statements for the audit table.
	CreateAuditIndexes(table, prefix string) []string
}

// DialectFor returns the Dialect matching a config-level driver name
// (e.g. "postgres", "mysql", "mssql", "sqlite").
func DialectFor(configDriver string) (Dialect, error) {
	switch strings.ToLower(configDriver) {
	case "postgres", "postgresql":
		return PostgresDialect{}, nil
	case "mysql":
		return MySQLDialect{}, nil
	case "mssql", "sqlserver":
		return MSSQLDialect{}, nil
	case "sqlite", "sqlite3":
		return SQLiteDialect{}, nil
	default:
		return nil, fmt.Errorf("unsupported database dialect: %q", configDriver)
	}
}
