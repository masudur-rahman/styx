package examples_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"

	isql "github.com/masudur-rahman/styx/sql"
	"github.com/masudur-rahman/styx/sql/sqlite"

	_ "modernc.org/sqlite"
)

// dbCounter gives every example its own isolated in-memory database.
var dbCounter int64

// openDB returns a fresh in-memory SQLite engine. Each call uses a uniquely
// named shared-cache database and a single connection so the schema created by
// Sync persists for the lifetime of the example.
func openDB() isql.Engine {
	n := atomic.AddInt64(&dbCounter, 1)
	dsn := fmt.Sprintf("file:example%d?mode=memory&cache=shared", n)
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		panic(err)
	}
	conn.SetMaxOpenConns(1)
	return sqlite.NewSQLite(conn)
}

// ctx is a shared background context for the examples.
var ctx = context.Background()

// Account is the shared model used across CRUD and query examples.
type Account struct {
	ID    int64  `db:"id,pk autoincr"`
	Name  string `db:"name"`
	Email string `db:"email"`
	Age   int    `db:"age"`
}
