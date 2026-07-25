package examples_test

import (
	"database/sql"
	"fmt"
	"time"

	isql "github.com/masudur-rahman/styx/v2/sql"
	"github.com/masudur-rahman/styx/v2/sql/sqlite"
)

// Example_pool shows tuning the underlying database/sql connection pool with
// PoolConfig and reading live pool stats back through the engine. The *sql.DB
// is the pool; PoolConfig only sets the fields you specify.
func Example_pool() {
	conn, err := sql.Open("sqlite", "file:poolexample?mode=memory&cache=shared")
	if err != nil {
		panic(err)
	}

	isql.PoolConfig{
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
	}.Apply(conn)

	db := sqlite.NewSQLite(conn)
	fmt.Println("max open conns:", db.Stats().MaxOpenConnections)

	// Output:
	// max open conns: 5
}
