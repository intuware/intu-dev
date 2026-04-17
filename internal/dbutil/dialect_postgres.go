package dbutil

import "fmt"

// PostgresDialect implements Dialect for PostgreSQL (via pgx).
type PostgresDialect struct{}

func (PostgresDialect) DriverName() string { return "pgx" }

func (PostgresDialect) Placeholder(index int) string {
	return fmt.Sprintf("$%d", index)
}

func (PostgresDialect) CreateMessagesTable(table string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id TEXT NOT NULL,
			correlation_id TEXT,
			channel_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			content BYTEA,
			content_size INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL,
			timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			duration_ms BIGINT NOT NULL DEFAULT 0,
			metadata JSONB,
			PRIMARY KEY (id, stage)
		)`, table)
}

func (PostgresDialect) CreateAuditTable(table string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			event TEXT NOT NULL,
			username TEXT NOT NULL,
			details JSONB,
			source_ip TEXT
		)`, table)
}

func (d PostgresDialect) UpsertMessage(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (id, correlation_id, channel_id, stage, content, content_size, status, timestamp, duration_ms, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id, stage) DO UPDATE SET
			correlation_id = EXCLUDED.correlation_id,
			content = EXCLUDED.content,
			content_size = EXCLUDED.content_size,
			status = EXCLUDED.status,
			timestamp = EXCLUDED.timestamp,
			duration_ms = EXCLUDED.duration_ms,
			metadata = EXCLUDED.metadata`, table)
}

func (PostgresDialect) InsertAudit(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (timestamp, event, username, details, source_ip)
		VALUES ($1, $2, $3, $4, $5)`, table)
}

func (PostgresDialect) MigrateColumns(table string) []string {
	return []string{
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS content_size INTEGER NOT NULL DEFAULT 0", table),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS duration_ms BIGINT NOT NULL DEFAULT 0", table),
	}
}

func (PostgresDialect) CreateIndexes(table, prefix string) []string {
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

func (PostgresDialect) CreateAuditIndexes(table, prefix string) []string {
	return []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%saudit_event ON %s (event)", prefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%saudit_user ON %s (username)", prefix, table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%saudit_ts ON %s (timestamp)", prefix, table),
	}
}
