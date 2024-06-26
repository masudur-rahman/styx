package sqlite

import (
	"context"
	"fmt"
	"testing"

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

	return NewSQLite(context.Background(), conn), conn.Close
}

func TestPostgres_Sync(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()
	err := db.Sync(User{})
	assert.Nil(t, err)
}

func TestPostgres_FindOne(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	db, err := db.BeginTx()
	assert.Nil(t, err)
	defer func() {
		err = db.Commit()
		assert.Nil(t, err)
	}()

	user := User{}
	db = db.Table("user")

	t.Run("find user by id", func(t *testing.T) {
		has, err := db.ID(1).FindOne(&user)
		assert.Nil(t, err)
		assert.True(t, has)
	})

	t.Run("find user by filter", func(t *testing.T) {
		has, err := db.Where("email LIKE ?", "%@test.test").FindOne(&user, User{})
		assert.Nil(t, err)
		assert.True(t, has)
	})
}

func TestPostgres_FindMany(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	var users []User
	//db = db.Table("user")

	t.Run("find all", func(t *testing.T) {
		err := db.FindMany(&users)
		assert.Nil(t, err)
	})

	t.Run("find by filter", func(t *testing.T) {
		err := db.FindMany(&users, User{Email: "masudjuly02@gmail.com"})
		assert.Nil(t, err)
	})

	t.Run("find by where", func(t *testing.T) {
		err := db.Where("name like 'masud%'").FindMany(&users)
		assert.Nil(t, err)
	})
}

func TestPostgres_InsertOne(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	db, err := db.BeginTx()
	assert.Nil(t, err)

	db = db.Table("user")
	t.Run("insert data", func(t *testing.T) {
		suffix := xid.New().String()
		user := User{
			Name:     "test-" + suffix,
			FullName: "Test Name",
			Email:    fmt.Sprintf("test-%v@test.test", suffix),
		}
		id, err := db.InsertOne(&user)
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

func TestPostgres_UpdateOne(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("user")
	t.Run("update data", func(t *testing.T) {
		user := User{
			FullName: "Test Name 2",
		}
		err := db.ID(1).UpdateOne(user)
		assert.Nil(t, err)
	})
}

func TestPostgres_DeleteOne(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("user")
	t.Run("delete data", func(t *testing.T) {
		err := db.ID(4).DeleteOne()
		assert.Nil(t, err)
	})
	t.Run("delete data from filter", func(t *testing.T) {
		err := db.DeleteOne(User{ID: 3})
		assert.Nil(t, err)
	})
}
