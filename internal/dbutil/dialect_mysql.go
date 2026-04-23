package dbutil

import "fmt"

// MySQLDialect implements Dialect for MySQL.
type MySQLDialect struct{}

func (MySQLDialect) DriverName() string { return "mysql" }

func (MySQLDialect) Placeholder(_ int) string { return "?" }

func (MySQLDialect) CreateMessagesTable(table string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(255) NOT NULL,
			correlation_id VARCHAR(255),
			channel_id VARCHAR(255) NOT NULL,
			stage VARCHAR(255) NOT NULL,
			content LONGBLOB,
			content_size INT NOT NULL DEFAULT 0,
			status VARCHAR(64) NOT NULL,
			timestamp DATETIME(6) NOT NULL DEFAULT NOW(6),
			duration_ms BIGINT NOT NULL DEFAULT 0,
			metadata JSON,
			PRIMARY KEY (id, stage),
			INDEX idx_channel_status (channel_id, status),
			INDEX idx_channel_ts (channel_id, timestamp),
			INDEX idx_ts (timestamp),
			INDEX idx_status (status, timestamp),
			INDEX idx_stage (stage, timestamp),
			INDEX idx_corr (correlation_id),
			INDEX idx_channel_stage_ts (channel_id, stage, timestamp)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, table)
}

func (MySQLDialect) CreateAuditTable(table string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			timestamp DATETIME(6) NOT NULL DEFAULT NOW(6),
			event VARCHAR(255) NOT NULL,
			username VARCHAR(255) NOT NULL,
			details JSON,
			source_ip VARCHAR(45),
			INDEX idx_event (event),
			INDEX idx_user (username),
			INDEX idx_ts (timestamp)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, table)
}

func (MySQLDialect) UpsertMessage(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (id, correlation_id, channel_id, stage, content, content_size, status, timestamp, duration_ms, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			correlation_id = VALUES(correlation_id),
			content = VALUES(content),
			content_size = VALUES(content_size),
			status = VALUES(status),
			timestamp = VALUES(timestamp),
			duration_ms = VALUES(duration_ms),
			metadata = VALUES(metadata)`, table)
}

func (MySQLDialect) InsertAudit(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (timestamp, event, username, details, source_ip)
		VALUES (?, ?, ?, ?, ?)`, table)
}

func (MySQLDialect) MigrateColumns(table string) []string {
	// MySQL does not support IF NOT EXISTS for ADD COLUMN; use a
	// procedure or just ignore errors at the call site.
	return nil
}

func (MySQLDialect) CreateIndexes(table, prefix string) []string {
	// Indexes are created inline in CreateMessagesTable for MySQL.
	return nil
}

func (MySQLDialect) CreateAuditIndexes(table, prefix string) []string {
	return nil
}
