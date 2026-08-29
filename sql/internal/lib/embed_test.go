package lib

import (
	"context"
	"database/sql"
	"testing"
	"time"

	core "github.com/masudur-rahman/styx/v2/sql/internal/core"
	"github.com/masudur-rahman/styx/v2/sql/internal/sqltest"
	"github.com/masudur-rahman/styx/v2/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

type embeddedRow struct {
	sqltest.Identifiable
	Name string `db:"name,notnull"`
	sqltest.Auditable
}

func (embeddedRow) TableName() string { return "embedded_row" }

// TestEmbedded_roundTrip is the test the whole phase exists for. DDL, INSERT
// and scanning each walk the struct separately, so flattening only half of
// them yields a column-count mismatch at runtime rather than a clean error.
func TestEmbedded_roundTrip(t *testing.T) {
	created := time.Date(2026, 8, 21, 10, 30, 0, 0, time.UTC)

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	require.NoError(t, SyncTable(ctx, SQLite, db, embeddedRow{}))

	cols, err := getExistingColumns(ctx, SQLite, db, "embedded_row")
	require.NoError(t, err)
	// Column order follows the walk: a level's own fields come before the
	// embeds it descends into, which is what makes shallowest-wins shadowing
	// work. Here Name is declared between the two embeds but is emitted first.
	assert.Equal(t,
		[]string{"name", "id", "created_at", "created_by", "updated_at", "updated_by", "deleted_at"},
		cols, "every embedded field must become a column")

	want := embeddedRow{Name: "ada"}
	want.ID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	want.CreatedAt = created
	want.CreatedBy = "root"

	stmt := NewStatement(SQLite)
	query := stmt.GenerateInsertQuery(want)
	_, err = db.ExecContext(ctx, query, stmt.Args()...)
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx,
		`SELECT id, name, created_at, created_by FROM embedded_row`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var gotID, gotName, gotBy string
	var gotAt time.Time
	require.NoError(t, rows.Scan(&gotID, &gotName, &gotAt, &gotBy))

	assert.Equal(t, string(want.ID), gotID)
	assert.Equal(t, "ada", gotName)
	assert.Equal(t, "root", gotBy)
	assert.True(t, created.Equal(gotAt), "want %v, got %v", created, gotAt)
}

// TestEmbedded_updateAndFilter checks the two remaining struct walks: the SET
// list of an UPDATE and the WHERE list built from a filter struct.
func TestEmbedded_updateAndFilter(t *testing.T) {
	row := embeddedRow{Name: "ada"}
	row.ID = "abc"
	row.CreatedBy = "root"

	update := NewStatement(SQLite)
	update.Table("embedded_row").Where("id = ?", "abc")
	got := update.GenerateUpdateQuery(row)
	assert.Contains(t, got, "name = ?")
	assert.Contains(t, got, "id = ?", "the embedded pk must be in the SET list")
	assert.Contains(t, got, "created_by = ?", "the embedded audit column must be too")

	filter := NewStatement(SQLite)
	where := filter.GenerateWhereClauseFromFilter(row)
	assert.Contains(t, where, "name = ?")
	assert.Contains(t, where, "created_by = ?")
	// The pk arg keeps its declared type: a named string reaches the driver
	// as-is, which database/sql converts for a String kind.
	assert.Equal(t, []any{"ada", types.UUID("abc"), "root"}, filter.Args())
}

// TestEmbedded_bulkInsert checks the column-union walk, which indexes fields by
// position and so was the most likely to break on a nested index path.
func TestEmbedded_bulkInsert(t *testing.T) {
	a := embeddedRow{Name: "ada"}
	a.ID = "1"
	b := embeddedRow{Name: "bob"}
	b.ID = "2"
	b.CreatedBy = "root"

	stmt := NewStatement(SQLite)
	query := stmt.Table("embedded_row").GenerateBulkInsertQuery([]any{a, b})

	assert.Contains(t, query, "name, id, created_by")
	assert.Contains(t, query, "VALUES (?, ?, ?), (?, ?, ?)")
	assert.Equal(t, []any{"ada", types.UUID("1"), "", "bob", types.UUID("2"), "root"}, stmt.Args())
}

// TestEmbedded_scanHydratesThroughEmbed checks the read path, which resolves
// columns through the nested index rather than a flat field number.
func TestEmbedded_scanHydratesThroughEmbed(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	require.NoError(t, SyncTable(ctx, SQLite, db, embeddedRow{}))
	_, err = db.ExecContext(ctx,
		`INSERT INTO embedded_row (id, name, created_by) VALUES ('xyz', 'ada', 'root')`)
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, `SELECT id, name, created_by FROM embedded_row`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var got embeddedRow
	require.NoError(t, core.ScanRow(rows, &got))

	assert.Equal(t, "xyz", string(got.ID), "embedded pk must hydrate")
	assert.Equal(t, "ada", got.Name)
	assert.Equal(t, "root", got.CreatedBy, "embedded audit column must hydrate")
}
