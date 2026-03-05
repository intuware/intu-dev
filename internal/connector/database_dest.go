package connector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/intuware/intu/internal/message"
	"github.com/intuware/intu/pkg/config"
)

type DatabaseDest struct {
	name   string
	cfg    *config.DBDestMapConfig
	logger *slog.Logger

	mu sync.Mutex
	db *sql.DB
}

func NewDatabaseDest(name string, cfg *config.DBDestMapConfig, logger *slog.Logger) *DatabaseDest {
	return &DatabaseDest{name: name, cfg: cfg, logger: logger}
}

func (d *DatabaseDest) driverName() string {
	switch strings.ToLower(d.cfg.Driver) {
	case "postgres", "postgresql":
		return "postgres"
	case "mysql":
		return "mysql"
	case "mssql", "sqlserver":
		return "sqlserver"
	case "sqlite", "sqlite3":
		return "sqlite3"
	default:
		return d.cfg.Driver
	}
}

func (d *DatabaseDest) getDB() (*sql.DB, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.db != nil {
		return d.db, nil
	}

	db, err := sql.Open(d.driverName(), d.cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	maxConns := 5
	if d.cfg.MaxConns > 0 {
		maxConns = d.cfg.MaxConns
	}
	db.SetMaxOpenConns(maxConns)
	db.SetConnMaxLifetime(5 * time.Minute)

	d.db = db
	return db, nil
}

func (d *DatabaseDest) Send(ctx context.Context, msg *message.Message) (*message.Response, error) {
	if d.cfg.Statement == "" {
		d.logger.Error("database dest: no statement configured", "destination", d.name)
		return &message.Response{
			StatusCode: 400,
			Error:      fmt.Errorf("database destination %s: no statement configured", d.name),
		}, nil
	}

	db, err := d.getDB()
	if err != nil {
		d.logger.Error("database dest connect failed", "destination", d.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("database connect: %w", err)}, nil
	}

	if err := db.PingContext(ctx); err != nil {
		d.mu.Lock()
		d.db = nil
		d.mu.Unlock()
		d.logger.Error("database dest ping failed", "destination", d.name, "error", err)
		return &message.Response{StatusCode: 502, Error: fmt.Errorf("database ping: %w", err)}, nil
	}

	stmt := d.cfg.Statement

	// Replace template placeholders with message data
	stmt = strings.ReplaceAll(stmt, "${messageId}", msg.ID)
	stmt = strings.ReplaceAll(stmt, "${channelId}", msg.ChannelID)
	stmt = strings.ReplaceAll(stmt, "${correlationId}", msg.CorrelationID)
	stmt = strings.ReplaceAll(stmt, "${timestamp}", msg.Timestamp.Format(time.RFC3339))

	// If the message body is JSON, extract fields for parameterized replacement
	var jsonData map[string]any
	if json.Unmarshal(msg.Raw, &jsonData) == nil {
		for k, v := range jsonData {
			placeholder := "${" + k + "}"
			stmt = strings.ReplaceAll(stmt, placeholder, fmt.Sprintf("%v", v))
		}
	}

	stmt = strings.ReplaceAll(stmt, "${raw}", string(msg.Raw))

	result, err := db.ExecContext(ctx, stmt)
	if err != nil {
		d.logger.Error("database dest exec failed",
			"destination", d.name,
			"error", err,
			"messageId", msg.ID,
		)
		return &message.Response{StatusCode: 500, Error: fmt.Errorf("database exec: %w", err)}, nil
	}

	rowsAffected, _ := result.RowsAffected()
	d.logger.Debug("database dest statement executed",
		"destination", d.name,
		"rows_affected", rowsAffected,
		"messageId", msg.ID,
	)

	body, _ := json.Marshal(map[string]any{
		"status":        "executed",
		"rows_affected": rowsAffected,
	})

	return &message.Response{StatusCode: 200, Body: body}, nil
}

func (d *DatabaseDest) Stop(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.db != nil {
		d.db.Close()
		d.db = nil
	}
	return nil
}

func (d *DatabaseDest) Type() string {
	return "database"
}
