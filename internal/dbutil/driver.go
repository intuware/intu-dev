package dbutil

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "modernc.org/sqlite"
)

// ErrDSNRequired is returned when a nil config or empty DSN is provided.
var ErrDSNRequired = errors.New("database DSN is required")

// ResolveDriverName maps a user-facing config driver name to the
// database/sql registered driver name.
func ResolveDriverName(configDriver string) string {
	switch strings.ToLower(configDriver) {
	case "postgres", "postgresql":
		return "pgx"
	case "mysql":
		return "mysql"
	case "mssql", "sqlserver":
		return "sqlserver"
	case "sqlite", "sqlite3":
		return "sqlite"
	default:
		return configDriver
	}
}

// DBOptions controls connection pool settings for OpenDB.
type DBOptions struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

var defaultOpts = DBOptions{
	MaxOpenConns:    5,
	MaxIdleConns:    2,
	ConnMaxLifetime: 5 * time.Minute,
}

// OpenDB opens a *sql.DB using the config-level driver name, resolving it
// to the registered database/sql driver. An optional DBOptions overrides
// connection pool defaults.
func OpenDB(configDriver, dsn string, opts *DBOptions) (*sql.DB, error) {
	driverName := ResolveDriverName(configDriver)

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("open database (%s): %w", driverName, err)
	}

	o := defaultOpts
	if opts != nil {
		if opts.MaxOpenConns > 0 {
			o.MaxOpenConns = opts.MaxOpenConns
		}
		if opts.MaxIdleConns > 0 {
			o.MaxIdleConns = opts.MaxIdleConns
		}
		if opts.ConnMaxLifetime > 0 {
			o.ConnMaxLifetime = opts.ConnMaxLifetime
		}
	}
	db.SetMaxOpenConns(o.MaxOpenConns)
	db.SetMaxIdleConns(o.MaxIdleConns)
	db.SetConnMaxLifetime(o.ConnMaxLifetime)

	return db, nil
}
