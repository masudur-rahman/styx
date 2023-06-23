package postgres_test

import (
	"context"
	"testing"

	"github.com/masudur-rahman/database/sql"
	"github.com/masudur-rahman/database/sql/postgres"
	"github.com/masudur-rahman/database/sql/postgres/lib"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type User struct {
	ID       int64
	Name     string
	FullName string `db:"full_name"`
	Email    string
}

func initializeDB(t *testing.T) (sql.Database, func() error) {
	cfg := lib.PostgresConfig{
		Name:     "test",
		Host:     "localhost",
		Port:     "5432",
		User:     "postgres",
		Password: "postgres",
		SSLMode:  "disable",
	}

	conn, err := lib.GetPostgresConnection(cfg)
	require.Nil(t, err)

	return postgres.NewPostgres(context.Background(), conn), conn.Close
}

func TestPostgres_FindOne(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	user := User{}
	db = db.Table("user")

	t.Run("find user by id", func(t *testing.T) {
		has, err := db.ID(1).FindOne(&user)
		assert.Nil(t, err)
		assert.True(t, has)
	})

	t.Run("find user by filter", func(t *testing.T) {
		has, err := db.Where("email='masudjuly02@gmail.com'").FindOne(&user, User{Name: "masud", FullName: "Masudur Rahman"})
		assert.Nil(t, err)
		assert.True(t, has)
	})
}

func TestPostgres_FindMany(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	var users []User
	db = db.Table("user")

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

	db = db.Table("user")
	t.Run("insert data", func(t *testing.T) {
		user := User{
			Name:     "test",
			FullName: "Test Name",
			Email:    "test@test.test",
		}
		id, err := db.InsertOne(user)
		assert.Nil(t, err)
		assert.NotEqual(t, 0, id)
	})
}

func TestPostgres_UpdateOne(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("user")
	t.Run("insert data", func(t *testing.T) {
		user := User{
			FullName: "Test Name 2",
		}
		err := db.Where("name='test'").UpdateOne(user)
		assert.Nil(t, err)
	})
}

func TestPostgres_DeleteOne(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("user")
	t.Run("delete data", func(t *testing.T) {
		err := db.ID(8).DeleteOne()
		assert.Nil(t, err)
	})
	t.Run("delete data from filter", func(t *testing.T) {
		err := db.DeleteOne(User{ID: 7})
		assert.Nil(t, err)
	})
}
