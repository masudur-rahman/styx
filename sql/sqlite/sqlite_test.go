package sqlite

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/masudur-rahman/styx/v2/dberr"
	"github.com/masudur-rahman/styx/v2/sql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type User struct {
	ID       int64 `db:"id,pk"`
	Name     string
	FullName string
	Email    string `db:"email,uq"`
	Addr     string
}

// memDBCounter gives each test an isolated in-memory database.
var memDBCounter int64

// newMemEngine returns an isolated in-memory SQLite engine backed by a single
// connection so a synced schema persists for the test.
func newMemEngine(t *testing.T) sql.Engine {
	t.Helper()
	n := atomic.AddInt64(&memDBCounter, 1)
	dsn := fmt.Sprintf("file:sqlitetest%d?mode=memory&cache=shared", n)
	conn, err := stdsql.Open("sqlite", dsn)
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	t.Cleanup(func() { conn.Close() })
	return NewSQLite(conn)
}

// initializeDB returns a fresh in-memory engine with the User table synced.
func initializeDB(t *testing.T) (sql.Engine, func() error) {
	t.Helper()
	db := newMemEngine(t)
	require.NoError(t, db.Sync(context.Background(), User{}))
	return db, func() error { return nil }
}

func TestSQLite_Sync(t *testing.T) {
	db := newMemEngine(t)
	assert.Nil(t, db.Sync(context.Background(), User{}))
}

type indexedUser struct {
	ID    int64  `db:"id,pk autoincr"`
	Email string `db:"email"`
	Name  string `db:"name,idx"`
}

func TestSync_idempotent(t *testing.T) {
	ctx := context.Background()
	db := newMemEngine(t)

	// Repeated Sync must not error: table, columns, and indexes already exist.
	require.NoError(t, db.Sync(ctx, indexedUser{}))
	require.NoError(t, db.Sync(ctx, indexedUser{}))
	require.NoError(t, db.Sync(ctx, indexedUser{}))

	_, err := db.Table("indexed_user").InsertOne(ctx, &indexedUser{Email: "a@b.c", Name: "alice"})
	assert.NoError(t, err)
}

func TestSQLite_FindOne(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("user")
	_, err := db.InsertOne(ctx, &User{Name: "masud", Email: "masud@test.test"})
	require.NoError(t, err)

	var user User
	t.Run("find user by id", func(t *testing.T) {
		has, err := db.ID(1).FindOne(ctx, &user)
		assert.Nil(t, err)
		assert.True(t, has)
	})

	t.Run("find user by where", func(t *testing.T) {
		has, err := db.Where("email LIKE ?", "%@test.test").FindOne(ctx, &user)
		assert.Nil(t, err)
		assert.True(t, has)
	})

	t.Run("not found", func(t *testing.T) {
		has, err := db.ID(9999).FindOne(ctx, &user)
		assert.Nil(t, err)
		assert.False(t, has)
	})
}

func TestSQLite_FindMany(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("user")
	_, err := db.InsertMany(ctx, []any{
		&User{Name: "masud", Email: "masud@test.test"},
		&User{Name: "rahman", Email: "rahman@test.test"},
	})
	require.NoError(t, err)

	t.Run("find all", func(t *testing.T) {
		var users []User
		err := db.FindMany(ctx, &users)
		assert.Nil(t, err)
		assert.Len(t, users, 2)
	})

	t.Run("find by where", func(t *testing.T) {
		var users []User
		err := db.Where("name like 'masud%'").FindMany(ctx, &users)
		assert.Nil(t, err)
		assert.Len(t, users, 1)
	})
}

func TestSQLite_InsertOne(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("user")
	user := User{Name: "test", FullName: "Test Name", Email: "test@test.test"}
	id, err := db.InsertOne(ctx, &user)
	assert.Nil(t, err)
	assert.NotEqual(t, int64(0), id)
	assert.NotZero(t, user.ID)
}

func TestSQLite_UpdateOne(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("user")
	user := User{Name: "test", Email: "test@e.c"}
	id, err := db.InsertOne(ctx, &user)
	require.NoError(t, err)

	t.Run("update data", func(t *testing.T) {
		err := db.ID(id).UpdateOne(ctx, User{FullName: "Test Name 2"})
		assert.Nil(t, err)
	})
}

func TestSQLite_DeleteOne(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("user")

	t.Run("delete data by id", func(t *testing.T) {
		user := User{Name: "del", Email: "del@e.c"}
		id, err := db.InsertOne(ctx, &user)
		require.NoError(t, err)
		assert.Nil(t, db.ID(id).DeleteOne(ctx))
	})

	t.Run("delete data from filter", func(t *testing.T) {
		user := User{Name: "del2", Email: "del2@e.c"}
		id, err := db.InsertOne(ctx, &user)
		require.NoError(t, err)
		assert.Nil(t, db.DeleteOne(ctx, User{ID: id.(int64)}))
	})
}

func TestUpdateOne_nonExistentRow(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	err := db.Table("user").ID(999999).UpdateOne(ctx, User{FullName: "ghost"})
	assert.ErrorIs(t, err, dberr.ErrNotFound)
}

func TestDeleteOne_nonExistentRow(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	err := db.Table("user").ID(999999).DeleteOne(ctx)
	assert.ErrorIs(t, err, dberr.ErrNotFound)
}

// hookEvents records lifecycle hook invocations across hookPerson operations.
var hookEvents []string

type hookPerson struct {
	ID   int64  `db:"id,pk autoincr"`
	Name string `db:"name"`
}

func (p *hookPerson) BeforeCreate(ctx context.Context) error {
	hookEvents = append(hookEvents, "before:"+p.Name)
	return nil
}

func (p *hookPerson) AfterFind(ctx context.Context) error {
	hookEvents = append(hookEvents, "find:"+p.Name)
	return nil
}

func TestInsertMany_bulkSingleQuery(t *testing.T) {
	hookEvents = nil
	ctx := context.Background()
	db := newMemEngine(t)
	require.NoError(t, db.Sync(ctx, hookPerson{}))

	p1 := &hookPerson{Name: "alice"}
	p2 := &hookPerson{Name: "bob"}
	p3 := &hookPerson{Name: "carol"}

	ids, err := db.Table("hook_person").InsertMany(ctx, []any{p1, p2, p3})
	require.NoError(t, err)
	require.Len(t, ids, 3)

	// IDs assigned back, unique, and BeforeCreate ran per doc in order.
	assert.NotZero(t, p1.ID)
	assert.NotEqual(t, p1.ID, p2.ID)
	assert.Equal(t, []string{"before:alice", "before:bob", "before:carol"}, hookEvents)
}

func TestFindMany_runsAfterFindPerRow(t *testing.T) {
	ctx := context.Background()
	db := newMemEngine(t)
	require.NoError(t, db.Sync(ctx, hookPerson{}))

	_, err := db.Table("hook_person").InsertMany(ctx, []any{&hookPerson{Name: "a"}, &hookPerson{Name: "b"}})
	require.NoError(t, err)

	hookEvents = nil
	var people []hookPerson
	require.NoError(t, db.Table("hook_person").OrderBy("id", "ASC").FindMany(ctx, &people))
	require.Len(t, people, 2)
	assert.Equal(t, []string{"find:a", "find:b"}, hookEvents)
}
