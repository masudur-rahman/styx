package sqlite

import (
	"context"
	"database/sql"

	isql "github.com/masudur-rahman/styx/v2/sql"

	_ "modernc.org/sqlite"
)

func GetSQLiteConnection(dbPath string, pool ...isql.PoolConfig) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	if len(pool) > 0 {
		pool[0].Apply(db)
	}

	if err = db.PingContext(context.Background()); err != nil {
		return nil, err
	}

	return db, nil
}

// IsZeroValue checks if a value is its type's zero value.
// Deprecated: Use dberr.IsZeroValue instead.
