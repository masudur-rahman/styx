package examples_test

import (
	"context"
	"fmt"

	"github.com/masudur-rahman/styx/migrate"
	isql "github.com/masudur-rahman/styx/sql"
)

// Example_migrate shows the versioned migration runner for destructive changes
// that additive Sync will not do. Each migration is a plain Go func run in its
// own transaction; applied versions are recorded so Up is idempotent and Down
// reverses the latest.
func Example_migrate() {
	db := openDB()

	m := migrate.New(db).Register(
		migrate.Migration{
			Version: 1, Name: "create_widgets",
			Up: func(ctx context.Context, e isql.Engine) error {
				_, err := e.Exec(ctx, `CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT, legacy TEXT)`)
				return err
			},
			Down: func(ctx context.Context, e isql.Engine) error {
				_, err := e.Exec(ctx, `DROP TABLE widgets`)
				return err
			},
		},
		migrate.Migration{
			Version: 2, Name: "drop_legacy_column",
			Up: func(ctx context.Context, e isql.Engine) error {
				_, err := e.Exec(ctx, `ALTER TABLE widgets DROP COLUMN legacy`)
				return err
			},
			Down: func(ctx context.Context, e isql.Engine) error {
				_, err := e.Exec(ctx, `ALTER TABLE widgets ADD COLUMN legacy TEXT`)
				return err
			},
		},
	)

	if err := m.Up(ctx); err != nil {
		panic(err)
	}

	statuses, _ := m.Status(ctx)
	for _, s := range statuses {
		fmt.Printf("v%d %s applied=%t\n", s.Version, s.Name, s.Applied)
	}

	// Output:
	// v1 create_widgets applied=true
	// v2 drop_legacy_column applied=true
}
