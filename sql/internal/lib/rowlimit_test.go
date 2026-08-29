package lib

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

type member struct {
	ID   int64  `db:"id,pk autoincr"`
	City string `db:"city"`
	Name string `db:"name"`
}

func (member) TableName() string { return "member" }

// guest declares no pk tag, so a capped statement has no key to select by.
type guest struct {
	City string `db:"city"`
	Name string `db:"name"`
}

func (guest) TableName() string { return "guest" }

// TestRowLimit_wrapsOnlyWhenNeeded checks which statements grow the single-row
// subquery. An ID() lookup already names one row, so wrapping it would only
// cost a scan.
func TestRowLimit_wrapsOnlyWhenNeeded(t *testing.T) {
	tests := []struct {
		name     string
		render   func(s *Statement) string
		wantWrap bool
	}{
		{
			name:     "update by filter",
			render:   func(s *Statement) string { return s.GenerateUpdateQuery(member{Name: "bob"}) },
			wantWrap: true,
		},
		{
			name:     "update many by filter",
			render:   func(s *Statement) string { return s.GenerateUpdateManyQuery(member{Name: "bob"}) },
			wantWrap: false,
		},
		{
			name:     "delete by filter",
			render:   func(s *Statement) string { return s.GenerateDeleteQuery() },
			wantWrap: true,
		},
		{
			name:     "delete many by filter",
			render:   func(s *Statement) string { return s.GenerateDeleteManyQuery() },
			wantWrap: false,
		},
		{
			name:     "soft delete by filter",
			render:   func(s *Statement) string { return s.GenerateSoftDeleteQuery() },
			wantWrap: true,
		},
		{
			name:     "soft delete many by filter",
			render:   func(s *Statement) string { return s.GenerateSoftDeleteManyQuery() },
			wantWrap: false,
		},
		{
			name:     "restore by filter",
			render:   func(s *Statement) string { return s.GenerateRestoreQuery() },
			wantWrap: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			byFilter := NewStatement(SQLite)
			byFilter.Table("member").SoftDeleteCol("deleted_at").Where("city = ?", "dhaka")
			assert.Equal(t, tc.wantWrap, strings.Contains(tc.render(&byFilter), "LIMIT 1"))

			byID := NewStatement(SQLite)
			byID.Table("member").SoftDeleteCol("deleted_at").ID(int64(3)).GenerateWhereClause()
			assert.NotContains(t, tc.render(&byID), "LIMIT 1", "ID() already names one row")
		})
	}
}

// TestRowLimit_prefersPrimaryKey checks which column the subquery selects. The
// primary key is preferred because it survives the UPDATE that follows, while a
// Postgres ctid does not, and because it makes the chosen row deterministic.
func TestRowLimit_prefersPrimaryKey(t *testing.T) {
	tests := []struct {
		name  string
		build func(s *Statement)
		want  string
	}{
		{
			name:  "primary key declared by the filter struct",
			build: func(s *Statement) { s.Table("member").GenerateWhereClause(member{City: "dhaka"}) },
			want:  `id IN (SELECT id FROM "member" WHERE city = ? ORDER BY id LIMIT 1)`,
		},
		{
			name:  "filter struct declares no primary key",
			build: func(s *Statement) { s.Table("guest").GenerateWhereClause(guest{City: "dhaka"}) },
			want:  `rowid IN (SELECT rowid FROM "guest" WHERE city = ? LIMIT 1)`,
		},
		{
			name:  "raw condition names no struct at all",
			build: func(s *Statement) { s.Table("member").Where("city = ?", "dhaka") },
			want:  `rowid IN (SELECT rowid FROM "member" WHERE city = ? LIMIT 1)`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := NewStatement(SQLite)
			tc.build(&stmt)
			assert.Contains(t, stmt.GenerateDeleteQuery(), tc.want)
		})
	}
}

// TestRowLimit_keepsArgOrder checks that relocating the WHERE clause into a
// subquery leaves SET arguments ahead of WHERE arguments.
func TestRowLimit_keepsArgOrder(t *testing.T) {
	stmt := NewStatement(Postgres)
	stmt.Table("member").Where("city = ?", "dhaka")

	query := stmt.GenerateUpdateQuery(member{Name: "bob"})
	assert.Equal(t,
		`UPDATE "member" SET name = $1 WHERE id IN (SELECT id FROM "member" WHERE city = $2 ORDER BY id LIMIT 1)`,
		query)
	assert.Equal(t, []any{"bob", "dhaka"}, stmt.Args())
}

// TestRowLimit_updatesOneRow runs the generated SQL against SQLite, which the
// golden files cannot do: three rows match the filter and exactly one changes.
func TestRowLimit_updatesOneRow(t *testing.T) {
	tests := []struct {
		name        string
		render      func(s *Statement) string
		wantChanged int64
	}{
		{
			name:        "update one",
			render:      func(s *Statement) string { return s.GenerateUpdateQuery(member{Name: "bob"}) },
			wantChanged: 1,
		},
		{
			name:        "update many",
			render:      func(s *Statement) string { return s.GenerateUpdateManyQuery(member{Name: "bob"}) },
			wantChanged: 3,
		},
		{
			name:        "delete one",
			render:      func(s *Statement) string { return s.GenerateDeleteQuery() },
			wantChanged: 1,
		},
		{
			name:        "delete many",
			render:      func(s *Statement) string { return s.GenerateDeleteManyQuery() },
			wantChanged: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := memberDB(t)

			stmt := NewStatement(SQLite)
			stmt.Table("member").Where("city = ?", "dhaka")
			result, err := db.Exec(tc.render(&stmt), stmt.Args()...)
			require.NoError(t, err)

			changed, err := result.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.wantChanged, changed)
		})
	}
}

// memberDB returns an in-memory database holding three rows in dhaka and one
// elsewhere, so a filter on the city matches more than one row.
func memberDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`CREATE TABLE member (
		id INTEGER PRIMARY KEY AUTOINCREMENT, city TEXT, name TEXT)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO member (city, name) VALUES
		('dhaka','alice'), ('dhaka','carol'), ('dhaka','dave'), ('ctg','erin')`)
	require.NoError(t, err)

	return db
}
