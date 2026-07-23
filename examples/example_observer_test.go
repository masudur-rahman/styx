package examples_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	isql "github.com/masudur-rahman/styx/sql"
	"github.com/masudur-rahman/styx/sql/sqlite"
)

// recordingObserver captures the verb and error of every executed statement.
type recordingObserver struct {
	events []string
}

func (o *recordingObserver) OnQuery(_ context.Context, query string, _ []any, _ time.Duration, err error) {
	o.events = append(o.events, fmt.Sprintf("%s err=%v", strings.Fields(query)[0], err))
}

// Example_observer shows wiring an Observer at construction to trace every
// statement the engine runs. Schema sync uses its own DDL path and is not
// reported; only data statements flow through the observer.
func Example_observer() {
	obs := &recordingObserver{}

	conn, err := sql.Open("sqlite", "file:observer?mode=memory&cache=shared")
	if err != nil {
		panic(err)
	}
	conn.SetMaxOpenConns(1)
	db := sqlite.NewSQLite(conn, isql.WithObserver(obs))

	db.Sync(ctx, Account{})
	db.Table("account").InsertOne(ctx, &Account{Name: "alice"})
	var a Account
	db.Table("account").ID(1).FindOne(ctx, &a)

	for _, e := range obs.events {
		fmt.Println(e)
	}

	// Output:
	// INSERT err=<nil>
	// SELECT err=<nil>
}
