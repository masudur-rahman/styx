package sqlite

import (
	"context"
	"fmt"
	"testing"

	"github.com/masudur-rahman/styx/dberr"
	"github.com/masudur-rahman/styx/sql"
	"github.com/masudur-rahman/styx/sql/sqlite/lib"

	"github.com/rs/xid"
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

func initializeDB(t *testing.T) (sql.Engine, func() error) {
	conn, err := lib.GetSQLiteConnection("test.db")
	require.Nil(t, err)

	return NewSQLite(conn), conn.Close
}

func TestPostgres_Sync(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()
	err := db.Sync(context.Background(), User{})
	assert.Nil(t, err)
}

func TestPostgres_FindOne(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	db, err := db.BeginTx(ctx)
	assert.Nil(t, err)
	defer func() {
		err = db.Commit()
		assert.Nil(t, err)
	}()

	user := User{}
	db = db.Table("user")

	t.Run("find user by id", func(t *testing.T) {
		has, err := db.ID(1).FindOne(ctx, &user)
		assert.Nil(t, err)
		assert.True(t, has)
	})

	t.Run("find user by filter", func(t *testing.T) {
		has, err := db.Where("email LIKE ?", "%@test.test").FindOne(ctx, &user, User{})
		assert.Nil(t, err)
		assert.True(t, has)
	})
}

func TestPostgres_FindMany(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	var users []User
	//db = db.Table("user")

	t.Run("find all", func(t *testing.T) {
		err := db.FindMany(ctx, &users)
		assert.Nil(t, err)
	})

	t.Run("find by filter", func(t *testing.T) {
		err := db.FindMany(ctx, &users, User{Email: "masudjuly02@gmail.com"})
		assert.Nil(t, err)
	})

	t.Run("find by where", func(t *testing.T) {
		err := db.Where("name like 'masud%'").FindMany(ctx, &users)
		assert.Nil(t, err)
	})
}

func TestPostgres_InsertOne(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	db, err := db.BeginTx(ctx)
	assert.Nil(t, err)

	db = db.Table("user")
	t.Run("insert data", func(t *testing.T) {
		suffix := xid.New().String()
		user := User{
			Name:     "test-" + suffix,
			FullName: "Test Name",
			Email:    fmt.Sprintf("test-%v@test.test", suffix),
		}
		id, err := db.InsertOne(ctx, &user)
		assert.Nil(t, err)
		assert.NotEqual(t, 0, id)
		if err != nil {
			err = db.Rollback()
			assert.Nil(t, err)
		}

		err = db.Commit()
		assert.Nil(t, err)
	})
}

func TestSQLite_UpdateOne(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("user")
	user := User{Name: "test", Email: "test@e.c"}
	id, _ := db.InsertOne(ctx, &user)

	t.Run("update data", func(t *testing.T) {
		update := User{
			FullName: "Test Name 2",
		}
		err := db.ID(id).UpdateOne(ctx, update)
		assert.Nil(t, err)
	})
}

func TestSQLite_DeleteOne(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("user")
	user := User{Name: "del", Email: "del@e.c"}
	id, _ := db.InsertOne(ctx, &user)

	t.Run("delete data", func(t *testing.T) {
		err := db.ID(id).DeleteOne(ctx)
		assert.Nil(t, err)
	})
	t.Run("delete data from filter", func(t *testing.T) {
		user2 := User{Name: "del2", Email: "del2@e.c"}
		id2, _ := db.InsertOne(ctx, &user2)
		err := db.DeleteOne(ctx, User{ID: id2.(int64)})
		assert.Nil(t, err)
	})
}

func TestUpdateOne_nonExistentRow(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	err := db.Sync(ctx, User{})
	require.Nil(t, err)

	err = db.Table("user").ID(999999).UpdateOne(ctx, User{FullName: "ghost"})
	assert.ErrorIs(t, err, dberr.ErrNotFound)
}

func TestDeleteOne_nonExistentRow(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	err := db.Sync(ctx, User{})
	require.Nil(t, err)

	err = db.Table("user").ID(999999).DeleteOne(ctx)
	assert.ErrorIs(t, err, dberr.ErrNotFound)
}
