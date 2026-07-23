package migrate

import (
	"context"
	"database/sql"
	"testing"

	isql "github.com/masudur-rahman/styx/sql"
	"github.com/masudur-rahman/styx/sql/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

func newEngine(t *testing.T) isql.Engine {
	t.Helper()
	conn, err := sql.Open("sqlite", "file:migrate"+t.Name()+"?mode=memory&cache=shared")
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { conn.Close() })
	return sqlite.NewSQLite(conn)
}

func createUsers(ctx context.Context, e isql.Engine) error {
	_, err := e.Exec(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, legacy TEXT)`)
	return err
}

func dropUsers(ctx context.Context, e isql.Engine) error {
	_, err := e.Exec(ctx, `DROP TABLE users`)
	return err
}

func dropLegacy(ctx context.Context, e isql.Engine) error {
	_, err := e.Exec(ctx, `ALTER TABLE users DROP COLUMN legacy`)
	return err
}

func TestMigrator_Up_appliesPendingInOrder(t *testing.T) {
	ctx := context.Background()
	e := newEngine(t)
	m := New(e).Register(
		Migration{Version: 2, Name: "drop_legacy", Up: dropLegacy},
		Migration{Version: 1, Name: "create_users", Up: createUsers, Down: dropUsers},
	)

	require.NoError(t, m.Up(ctx))

	// Both recorded, ordered.
	st, err := m.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, []Status{
		{Version: 1, Name: "create_users", Applied: true},
		{Version: 2, Name: "drop_legacy", Applied: true},
	}, st)

	// legacy column is gone.
	_, err = e.Exec(ctx, `INSERT INTO users (id, legacy) VALUES (1, 'x')`)
	assert.Error(t, err)
}

func TestMigrator_Up_idempotent(t *testing.T) {
	ctx := context.Background()
	e := newEngine(t)
	m := New(e).Register(Migration{Version: 1, Name: "create_users", Up: createUsers, Down: dropUsers})

	require.NoError(t, m.Up(ctx))
	require.NoError(t, m.Up(ctx)) // second run applies nothing, no error

	st, err := m.Status(ctx)
	require.NoError(t, err)
	assert.Len(t, st, 1)
	assert.True(t, st[0].Applied)
}

func TestMigrator_Down_reversesLatest(t *testing.T) {
	ctx := context.Background()
	e := newEngine(t)
	m := New(e).Register(
		Migration{Version: 1, Name: "create_users", Up: createUsers, Down: dropUsers},
		Migration{Version: 2, Name: "drop_legacy", Up: dropLegacy, Down: func(ctx context.Context, e isql.Engine) error {
			_, err := e.Exec(ctx, `ALTER TABLE users ADD COLUMN legacy TEXT`)
			return err
		}},
	)
	require.NoError(t, m.Up(ctx))

	require.NoError(t, m.Down(ctx)) // reverts version 2

	st, err := m.Status(ctx)
	require.NoError(t, err)
	assert.True(t, st[0].Applied)
	assert.False(t, st[1].Applied)

	// legacy column is back.
	_, err = e.Exec(ctx, `INSERT INTO users (id, legacy) VALUES (1, 'x')`)
	assert.NoError(t, err)
}

func TestMigrator_Down_noAppliedIsNoOp(t *testing.T) {
	ctx := context.Background()
	e := newEngine(t)
	m := New(e).Register(Migration{Version: 1, Name: "create_users", Up: createUsers, Down: dropUsers})

	assert.NoError(t, m.Down(ctx))
}

func TestMigrator_Up_rollsBackFailedMigration(t *testing.T) {
	ctx := context.Background()
	e := newEngine(t)
	m := New(e).Register(Migration{Version: 1, Name: "bad", Up: func(ctx context.Context, e isql.Engine) error {
		if _, err := e.Exec(ctx, `CREATE TABLE t1 (id INTEGER)`); err != nil {
			return err
		}
		// invalid statement fails the migration mid-way
		_, err := e.Exec(ctx, `THIS IS NOT SQL`)
		return err
	}})

	require.Error(t, m.Up(ctx))

	// version not recorded
	st, err := m.Status(ctx)
	require.NoError(t, err)
	assert.False(t, st[0].Applied)
}
