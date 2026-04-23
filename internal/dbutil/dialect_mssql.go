package dbutil

import "fmt"

// MSSQLDialect implements Dialect for Microsoft SQL Server.
type MSSQLDialect struct{}

func (MSSQLDialect) DriverName() string { return "sqlserver" }

func (MSSQLDialect) Placeholder(index int) string {
	return fmt.Sprintf("@p%d", index)
}

func (MSSQLDialect) CreateMessagesTable(table string) string {
	// SQL Server does not support CREATE TABLE IF NOT EXISTS, so we
	// wrap in an existence check.
	return fmt.Sprintf(`
		IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '%s')
		CREATE TABLE %s (
			id NVARCHAR(255) NOT NULL,
			correlation_id NVARCHAR(255),
			channel_id NVARCHAR(255) NOT NULL,
			stage NVARCHAR(255) NOT NULL,
			content VARBINARY(MAX),
			content_size INT NOT NULL DEFAULT 0,
			status NVARCHAR(64) NOT NULL,
			timestamp DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
			duration_ms BIGINT NOT NULL DEFAULT 0,
			metadata NVARCHAR(MAX),
			CONSTRAINT PK_%s PRIMARY KEY (id, stage)
		)`, table, table, table)
}

func (MSSQLDialect) CreateAuditTable(table string) string {
	return fmt.Sprintf(`
		IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '%s')
		CREATE TABLE %s (
			id BIGINT IDENTITY(1,1) PRIMARY KEY,
			timestamp DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
			event NVARCHAR(255) NOT NULL,
			username NVARCHAR(255) NOT NULL,
			details NVARCHAR(MAX),
			source_ip NVARCHAR(45)
		)`, table, table)
}

func (MSSQLDialect) UpsertMessage(table string) string {
	// MERGE is the idiomatic upsert for SQL Server.
	return fmt.Sprintf(`
		MERGE %s AS target
		USING (SELECT @p1 AS id, @p2 AS correlation_id, @p3 AS channel_id,
		              @p4 AS stage, @p5 AS content, @p6 AS content_size,
		              @p7 AS status, @p8 AS timestamp, @p9 AS duration_ms,
		              @p10 AS metadata) AS source
		ON target.id = source.id AND target.stage = source.stage
		WHEN MATCHED THEN UPDATE SET
			correlation_id = source.correlation_id,
			content = source.content,
			content_size = source.content_size,
			status = source.status,
			timestamp = source.timestamp,
			duration_ms = source.duration_ms,
			metadata = source.metadata
		WHEN NOT MATCHED THEN INSERT
			(id, correlation_id, channel_id, stage, content, content_size, status, timestamp, duration_ms, metadata)
			VALUES (source.id, source.correlation_id, source.channel_id, source.stage,
			        source.content, source.content_size, source.status, source.timestamp,
			        source.duration_ms, source.metadata);`, table)
}

func (MSSQLDialect) InsertAudit(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (timestamp, event, username, details, source_ip)
		VALUES (@p1, @p2, @p3, @p4, @p5)`, table)
}

func (MSSQLDialect) MigrateColumns(_ string) []string {
	// SQL Server migration is complex; columns are present in CREATE TABLE.
	return nil
}

func (MSSQLDialect) CreateIndexes(table, prefix string) []string {
	// Wrapped in IF NOT EXISTS checks for idempotency.
	idx := func(name, cols string) string {
		return fmt.Sprintf(
			"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '%s') CREATE INDEX %s ON %s (%s)",
			name, name, table, cols)
	}
	return []string{
		idx(fmt.Sprintf("idx_%smsg_channel_status", prefix), "channel_id, status"),
		idx(fmt.Sprintf("idx_%smsg_channel_ts", prefix), "channel_id, timestamp DESC"),
		idx(fmt.Sprintf("idx_%smsg_ts", prefix), "timestamp DESC"),
		idx(fmt.Sprintf("idx_%smsg_status", prefix), "status, timestamp DESC"),
		idx(fmt.Sprintf("idx_%smsg_stage", prefix), "stage, timestamp DESC"),
		idx(fmt.Sprintf("idx_%smsg_corr", prefix), "correlation_id"),
		idx(fmt.Sprintf("idx_%smsg_channel_stage_ts", prefix), "channel_id, stage, timestamp DESC"),
	}
}

func (MSSQLDialect) CreateAuditIndexes(table, prefix string) []string {
	idx := func(name, cols string) string {
		return fmt.Sprintf(
			"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '%s') CREATE INDEX %s ON %s (%s)",
			name, name, table, cols)
	}
	return []string{
		idx(fmt.Sprintf("idx_%saudit_event", prefix), "event"),
		idx(fmt.Sprintf("idx_%saudit_user", prefix), "username"),
		idx(fmt.Sprintf("idx_%saudit_ts", prefix), "timestamp"),
	}
}
