package lib

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

type probeV1 struct {
	ID   int64  `db:"id,pk autoincr"`
	Name string `db:"name"`
}

func (probeV1) TableName() string { return "probe" }

type probeV2 struct {
	ID    int64  `db:"id,pk autoincr"`
	Name  string `db:"name"`
	Email string `db:"email"`
}

func (probeV2) TableName() string { return "probe" }

// TestSyncTable_sqliteAddsColumn exercises ExistingColumnsQuery against a real
// SQLite database, which the golden tests cannot reach.
func TestSyncTable_sqliteAddsColumn(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	require.NoError(t, SyncTable(ctx, SQLite, db, probeV1{}))
	require.NoError(t, SyncTable(ctx, SQLite, db, probeV2{}))

	cols, err := getExistingColumns(ctx, SQLite, db, "probe")
	require.NoError(t, err)
	require.Equal(t, []string{"id", "name", "email"}, cols)

	// idempotent: a second sync must not error or duplicate
	require.NoError(t, SyncTable(ctx, SQLite, db, probeV2{}))
	cols, err = getExistingColumns(ctx, SQLite, db, "probe")
	require.NoError(t, err)
	require.Equal(t, []string{"id", "name", "email"}, cols)
}
