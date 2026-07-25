package core

import (
	"context"
	"database/sql"
	"sync"
)

// StmtCache memoises prepared statements by their SQL text for the non-transaction
// execution path. A *sql.Stmt is safe for concurrent use and manages its own
// per-connection preparation across the pool, so caching by query string reuses
// server-side prepared plans for recurring statements. It is opt-in because
// prepared statements hold resources on the server.
type StmtCache struct {
	mu sync.RWMutex
	m  map[string]*sql.Stmt
}

// NewStmtCache returns an empty statement cache.
func NewStmtCache() *StmtCache {
	return &StmtCache{m: map[string]*sql.Stmt{}}
}

// prepare returns a cached prepared statement for query, preparing and storing
// one on first use.
func (c *StmtCache) prepare(ctx context.Context, db *sql.DB, query string) (*sql.Stmt, error) {
	c.mu.RLock()
	stmt, ok := c.m[query]
	c.mu.RUnlock()
	if ok {
		return stmt, nil
	}

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.m[query]; ok {
		// Another goroutine won the race; discard ours and reuse theirs.
		_ = stmt.Close()
		return existing, nil
	}
	c.m[query] = stmt
	return stmt, nil
}

// QueryContext runs a cached-prepared query.
func (c *StmtCache) QueryContext(ctx context.Context, db *sql.DB, query string, args ...any) (*sql.Rows, error) {
	stmt, err := c.prepare(ctx, db, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

// QueryRowContext runs a cached-prepared single-row query.
func (c *StmtCache) QueryRowContext(ctx context.Context, db *sql.DB, query string, args ...any) (*sql.Row, error) {
	stmt, err := c.prepare(ctx, db, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryRowContext(ctx, args...), nil
}

// ExecContext runs a cached-prepared statement.
func (c *StmtCache) ExecContext(ctx context.Context, db *sql.DB, query string, args ...any) (sql.Result, error) {
	stmt, err := c.prepare(ctx, db, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}
