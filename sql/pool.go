package sql

import (
	"database/sql"
	"time"
)

// PoolConfig tunes the underlying database/sql connection pool. The *sql.DB is
// itself the pool; these settings control how many physical connections it keeps
// open and how long they live. A zero-valued field is left at the database/sql
// default, so callers set only what they care about.
type PoolConfig struct {
	// MaxOpenConns caps the total open connections (in-use + idle). 0 = unlimited.
	MaxOpenConns int
	// MaxIdleConns caps the idle connections kept warm for reuse.
	MaxIdleConns int
	// ConnMaxLifetime is the maximum age of a connection before it is recycled.
	ConnMaxLifetime time.Duration
	// ConnMaxIdleTime is how long a connection may sit idle before being closed.
	ConnMaxIdleTime time.Duration
}

// Apply sets the non-zero fields of the config on db, leaving the rest at their
// database/sql defaults.
func (c PoolConfig) Apply(db *sql.DB) {
	if c.MaxOpenConns != 0 {
		db.SetMaxOpenConns(c.MaxOpenConns)
	}
	if c.MaxIdleConns != 0 {
		db.SetMaxIdleConns(c.MaxIdleConns)
	}
	if c.ConnMaxLifetime != 0 {
		db.SetConnMaxLifetime(c.ConnMaxLifetime)
	}
	if c.ConnMaxIdleTime != 0 {
		db.SetConnMaxIdleTime(c.ConnMaxIdleTime)
	}
}
