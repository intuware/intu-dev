package storage

import (
	"github.com/intuware/intu-dev/internal/dbutil"
	"github.com/intuware/intu-dev/pkg/config"
)

// PostgresStore is a backward-compatible alias for SQLStore configured
// with the PostgreSQL dialect.
type PostgresStore = SQLStore

// NewPostgresStore creates a PostgreSQL-backed message store using the
// pgx driver. This is a convenience wrapper around NewSQLStore.
func NewPostgresStore(cfg *config.StoragePostgresConfig) (*PostgresStore, error) {
	if cfg == nil {
		return nil, dbutil.ErrDSNRequired
	}
	return NewSQLStore("postgres", cfg.DSN, cfg.TablePrefix, cfg.MaxOpenConns, cfg.MaxIdleConns)
}
