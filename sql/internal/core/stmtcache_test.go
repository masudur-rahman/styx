package core

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

func TestStmtCache_reusesPreparedStatement(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:stmtcache?mode=memory&cache=shared")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()

	_, err = db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)`)
	require.NoError(t, err)

	c := NewStmtCache()
	const q = `INSERT INTO t (n) VALUES (?)`

	first, err := c.prepare(ctx, db, q)
	require.NoError(t, err)
	second, err := c.prepare(ctx, db, q)
	require.NoError(t, err)

	// Same SQL text returns the identical cached *sql.Stmt.
	assert.Same(t, first, second)

	// A different query gets its own entry.
	other, err := c.prepare(ctx, db, `SELECT n FROM t WHERE id = ?`)
	require.NoError(t, err)
	assert.NotSame(t, first, other)
}

func TestStmtCache_execAndQueryWork(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:stmtcache2?mode=memory&cache=shared")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.Close()

	_, err = db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)`)
	require.NoError(t, err)

	c := NewStmtCache()
	_, err = c.ExecContext(ctx, db, `INSERT INTO t (n) VALUES (?)`, 42)
	require.NoError(t, err)

	// Repeated cached read returns the stored value.
	for i := 0; i < 2; i++ {
		row, err := c.QueryRowContext(ctx, db, `SELECT n FROM t WHERE id = ?`, 1)
		require.NoError(t, err)
		var n int
		require.NoError(t, row.Scan(&n))
		assert.Equal(t, 42, n)
	}
}
