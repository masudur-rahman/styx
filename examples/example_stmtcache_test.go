package examples_test

import (
	"database/sql"
	"fmt"

	isql "github.com/masudur-rahman/styx/v2/sql"
	"github.com/masudur-rahman/styx/v2/sql/sqlite"
)

// Example_stmtCache enables prepared-statement caching with WithStmtCache.
// Recurring queries (same SQL text) reuse a cached prepared statement on the
// non-transaction path; results are identical to the uncached path.
func Example_stmtCache() {
	conn, err := sql.Open("sqlite", "file:stmtcacheexample?mode=memory&cache=shared")
	if err != nil {
		panic(err)
	}
	conn.SetMaxOpenConns(1)

	db := sqlite.NewSQLite(conn, isql.WithStmtCache())
	db.Sync(ctx, Account{})

	db.Table("account").InsertMany(ctx, []any{
		&Account{Name: "alice"},
		&Account{Name: "bob"},
	})

	// The same lookup runs twice; the second reuses the cached statement.
	for id := int64(1); id <= 2; id++ {
		var a Account
		db.Table("account").ID(id).FindOne(ctx, &a)
		fmt.Println(a.Name)
	}

	// Output:
	// alice
	// bob
}
