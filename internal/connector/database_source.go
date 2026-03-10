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

	"github.com/intuware/intu-dev/internal/message"
	"github.com/intuware/intu-dev/pkg/config"
)

type DatabaseSource struct {
	cfg    *config.DBListener
	logger *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewDatabaseSource(cfg *config.DBListener, logger *slog.Logger) *DatabaseSource {
	return &DatabaseSource{cfg: cfg, logger: logger}
}

func (d *DatabaseSource) Start(ctx context.Context, handler MessageHandler) error {
	interval := 1 * time.Second
	if d.cfg.PollInterval != "" {
		if dur, err := time.ParseDuration(d.cfg.PollInterval); err == nil {
			interval = dur
		}
	}

	ctx, d.cancel = context.WithCancel(ctx)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := d.poll(ctx, handler); err != nil {
					d.logger.Error("database poll error", "error", err)
				}
			}
		}
	}()

	d.logger.Info("database source started",
		"driver", d.cfg.Driver,
		"poll_interval", interval.String(),
	)
	return nil
}

func (d *DatabaseSource) poll(ctx context.Context, handler MessageHandler) error {
	db, err := sql.Open(d.driverName(), d.cfg.DSN)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(5)
	db.SetConnMaxLifetime(30 * time.Second)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	query := strings.TrimSpace(d.cfg.Query)
	if query == "" {
		return nil
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns: %w", err)
	}

	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			d.logger.Error("scan row failed", "error", err)
			continue
		}

		row := make(map[string]any, len(columns))
		for i, col := range columns {
			val := values[i]
			switch v := val.(type) {
			case []byte:
				row[col] = string(v)
			default:
				row[col] = v
			}
		}

		data, err := json.Marshal(row)
		if err != nil {
			d.logger.Error("marshal row failed", "error", err)
			continue
		}

		msg := message.New("", data)
		msg.ContentType = "json"
		msg.Metadata["source"] = "database"
		msg.Metadata["driver"] = d.cfg.Driver

		if id, ok := row["id"]; ok {
			msg.Metadata["row_id"] = id
		}

		if err := handler(ctx, msg); err != nil {
			d.logger.Error("database handler error", "error", err)
			continue
		}

		if d.cfg.PostProcessStatement != "" {
			d.executePostProcess(ctx, db, row)
		}
	}

	return rows.Err()
}

func (d *DatabaseSource) executePostProcess(ctx context.Context, db *sql.DB, row map[string]any) {
	stmt := d.cfg.PostProcessStatement

	stmt = strings.ReplaceAll(stmt, ":id", fmt.Sprintf("%v", row["id"]))

	for col, val := range row {
		placeholder := ":" + col
		stmt = strings.ReplaceAll(stmt, placeholder, fmt.Sprintf("'%v'", val))
	}

	if _, err := db.ExecContext(ctx, stmt); err != nil {
		d.logger.Error("post-process statement failed", "error", err)
	}
}

func (d *DatabaseSource) driverName() string {
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

func (d *DatabaseSource) Stop(ctx context.Context) error {
	if d.cancel != nil {
		d.cancel()
	}
	d.wg.Wait()
	return nil
}

func (d *DatabaseSource) Type() string {
	return "database"
}
