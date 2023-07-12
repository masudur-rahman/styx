package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/masudur-rahman/database/sql"
	"github.com/masudur-rahman/database/sql/postgres"
	"github.com/masudur-rahman/database/sql/postgres/lib"

	"github.com/rs/xid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestUser struct {
	ID        int64     `db:"id,pk autoincr"`
	Name      string    `db:"name,uq"`
	FullName  string    `db:"full_name,uqs"`
	Email     string    `db:",uqs"`
	CreatedAt time.Time `db:"created_at"`
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

	return postgres.NewPostgres(context.Background(), conn).ShowSQL(true), conn.Close
}

func TestPostgres_Sync(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	err := db.Sync(TestUser{})
	assert.Nil(t, err)
}

func TestPostgres_FindOne(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	user := TestUser{}
	db = db.Table("test_user")

	t.Run("find user by id", func(t *testing.T) {
		has, err := db.ID(1).FindOne(&user)
		assert.Nil(t, err)
		assert.True(t, has)
	})

	t.Run("find user by filter", func(t *testing.T) {
		has, err := db.Where("email=?", "test@test.test").FindOne(&user, TestUser{Name: "test"})
		assert.Nil(t, err)
		assert.True(t, has)
	})
}

func TestPostgres_FindMany(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	var users []TestUser
	db = db.Table("test_user")

	t.Run("find all", func(t *testing.T) {
		err := db.FindMany(&users)
		assert.Nil(t, err)
	})

	t.Run("find by filter", func(t *testing.T) {
		err := db.FindMany(&users, TestUser{Email: "masudjuly02@gmail.com"})
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

	db = db.Table("test_user")
	t.Run("insert data", func(t *testing.T) {
		suffix := xid.New().String()
		user := TestUser{
			Name:     "test-" + suffix,
			FullName: "Test Name",
			Email:    fmt.Sprintf("test%v@test.test", suffix),
		}
		id, err := db.InsertOne(&user)
		assert.Nil(t, err)
		assert.NotEqual(t, 0, id)
	})
}

func TestPostgres_UpdateOne(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("test_user")
	t.Run("insert data", func(t *testing.T) {
		user := TestUser{
			FullName: "Test Name 2",
		}
		err := db.Where("name='test'").UpdateOne(user)
		assert.Nil(t, err)
	})
}

func TestPostgres_DeleteOne(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("test_user")
	t.Run("delete data", func(t *testing.T) {
		err := db.ID(8).DeleteOne()
		assert.Nil(t, err)
	})
	t.Run("delete data from filter", func(t *testing.T) {
		err := db.DeleteOne(TestUser{ID: 7})
		assert.Nil(t, err)
	})
}
