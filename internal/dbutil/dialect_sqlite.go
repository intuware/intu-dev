package dbutil

import "fmt"

// SQLiteDialect implements Dialect for SQLite (modernc.org/sqlite).
type SQLiteDialect struct{}

func (SQLiteDialect) DriverName() string { return "sqlite" }

func (SQLiteDialect) Placeholder(_ int) string { return "?" }

func (SQLiteDialect) CreateMessagesTable(table string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT NOT NULL,
			correlation_id TEXT,
			channel_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			content BLOB,
			content_size INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL,
			timestamp TEXT NOT NULL DEFAULT (datetime('now')),
			duration_ms INTEGER NOT NULL DEFAULT 0,
			metadata TEXT,
			PRIMARY KEY (id, stage)
		)`, table)
}

func (SQLiteDialect) CreateAuditTable(table string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp TEXT NOT NULL DEFAULT (datetime('now')),
			event TEXT NOT NULL,
			username TEXT NOT NULL,
			details TEXT,
			source_ip TEXT
		)`, table)
}

func (SQLiteDialect) UpsertMessage(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (id, correlation_id, channel_id, stage, content, content_size, status, timestamp, duration_ms, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id, stage) DO UPDATE SET
			correlation_id = excluded.correlation_id,
			content = excluded.content,
			content_size = excluded.content_size,
			status = excluded.status,
			timestamp = excluded.timestamp,
			duration_ms = excluded.duration_ms,
			metadata = excluded.metadata`, table)
}

func (SQLiteDialect) InsertAudit(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (timestamp, event, username, details, source_ip)
		VALUES (?, ?, ?, ?, ?)`, table)
}

func (SQLiteDialect) MigrateColumns(_ string) []string {
	// SQLite does not support ADD COLUMN IF NOT EXISTS in older versions;
	// the columns are present in the CREATE TABLE so this is only needed
	// for upgrades from very early schemas. Skip for now.
	return nil
}

func (SQLiteDialect) CreateIndexes(table, prefix string) []string {
	return []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%smsg_channel_status ON %s (channel_id, status)", prefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%smsg_channel_ts ON %s (channel_id, timestamp DESC)", prefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%smsg_ts ON %s (timestamp DESC)", prefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%smsg_status ON %s (status, timestamp DESC)", prefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%smsg_stage ON %s (stage, timestamp DESC)", prefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%smsg_corr ON %s (correlation_id)", prefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%smsg_channel_stage_ts ON %s (channel_id, stage, timestamp DESC)", prefix, table),
	}
}

func (SQLiteDialect) CreateAuditIndexes(table, prefix string) []string {
	return []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%saudit_event ON %s (event)", prefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%saudit_user ON %s (username)", prefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%saudit_ts ON %s (timestamp)", prefix, table),
	}
}
