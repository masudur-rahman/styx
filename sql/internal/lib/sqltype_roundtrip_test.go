package lib

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/masudur-rahman/styx/v2/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

type uuidRow struct {
	ID      types.UUID `db:"id,pk"`
	OwnerID string     `db:"owner_id,type=uuid"`
	Name    string     `db:"name"`
}

func (uuidRow) TableName() string { return "uuid_row" }

// TestColumnTypes_roundTrip drives the resolved column types through a real
// database, which the golden files cannot do: they pin the DDL text, not
// whether a value survives a write and a read.
func TestColumnTypes_roundTrip(t *testing.T) {
	const id = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	const owner = "0b6dd39a-2f83-4a30-9f3c-2f4d0f0a1b2c"

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	require.NoError(t, SyncTable(ctx, SQLite, db, uuidRow{}))

	stmt := NewStatement(SQLite)
	query := stmt.GenerateInsertQuery(uuidRow{ID: id, OwnerID: owner, Name: "ada"})
	_, err = db.ExecContext(ctx, query, stmt.Args()...)
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, `SELECT id, owner_id, name FROM uuid_row`)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())
	var got uuidRow
	require.NoError(t, rows.Scan(&got.ID, &got.OwnerID, &got.Name))

	assert.Equal(t, types.UUID(id), got.ID)
	assert.Equal(t, owner, got.OwnerID)
	assert.Equal(t, "ada", got.Name)
}

// TestColumnTypes_registryBeatsKindSwitch checks precedence: a registered type
// wins over the dialect's Go-kind mapping even though both apply.
func TestColumnTypes_registryBeatsKindSwitch(t *testing.T) {
	type row struct {
		Plain string     `db:"plain"`
		Typed types.UUID `db:"typed"`
	}
	rt := rowFields(t, row{})

	assert.Equal(t, "VARCHAR(255)", columnType(Postgres, rt["Plain"], rt["Plain"].Type, false))
	assert.Equal(t, "UUID", columnType(Postgres, rt["Typed"], rt["Typed"].Type, false))
}

// TestColumnTypes_tagBeatsRegistryAndJSON checks that an explicit type= wins
// over both the registry and the json tag.
func TestColumnTypes_tagBeatsRegistryAndJSON(t *testing.T) {
	type row struct {
		OverriddenType types.UUID     `db:"a,type=text"`
		OverriddenJSON map[string]any `db:"b,json type=jsonb"`
		PlainJSON      map[string]any `db:"c,json"`
	}
	rt := rowFields(t, row{})

	assert.Equal(t, "text", columnType(Postgres, rt["OverriddenType"], rt["OverriddenType"].Type, false))
	assert.Equal(t, "jsonb", columnType(Postgres, rt["OverriddenJSON"], rt["OverriddenJSON"].Type, false))
	assert.Equal(t, "JSONB", columnType(Postgres, rt["PlainJSON"], rt["PlainJSON"].Type, false))
}

// TestColumnTypes_autoincrWins checks that an auto-incrementing column keeps
// the dialect's type: on SQLite it is a complete column definition, and an
// override would produce a column that does not auto-increment.
func TestColumnTypes_autoincrWins(t *testing.T) {
	type row struct {
		ID int64 `db:"id,pk autoincr type=uuid"`
	}
	rt := rowFields(t, row{})

	assert.Equal(t, "BIGSERIAL", columnType(Postgres, rt["ID"], rt["ID"].Type, true))
	assert.Equal(t, "INTEGER PRIMARY KEY AUTOINCREMENT", columnType(SQLite, rt["ID"], rt["ID"].Type, true))
}

// rowFields indexes a struct's fields by Go field name.
func rowFields(t *testing.T, v any) map[string]reflect.StructField {
	t.Helper()
	rt := reflect.TypeOf(v)
	out := make(map[string]reflect.StructField, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		out[rt.Field(i).Name] = rt.Field(i)
	}
	return out
}
