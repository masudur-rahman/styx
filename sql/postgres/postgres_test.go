package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/masudur-rahman/styx/v2/sql"
	"github.com/masudur-rahman/styx/v2/sql/postgres"

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

// requiredEnv names the variable CI sets so that a database it provisioned can
// never be quietly skipped past.
const requiredEnv = "STYX_POSTGRES_REQUIRED"

// initializeDB returns an engine with the test table synced, or skips the test
// when no database is reachable.
//
// These tests talk to a real Postgres rather than a mock, so a contributor
// without one running — or a containerised `make test`, which cannot see the
// host's — would otherwise get a wall of dial failures. Syncing here rather
// than in one test keeps each test able to run on its own.
func initializeDB(t *testing.T) (sql.Engine, func() error) {
	t.Helper()

	cfg := postgres.PostgresConfig{
		Name:     "test",
		Host:     envOr("POSTGRES_HOST", "localhost"),
		Port:     envOr("POSTGRES_PORT", "5432"),
		User:     "postgres",
		Password: "postgres",
		SSLMode:  "disable",
	}

	conn, err := postgres.GetPostgresConnection(cfg)
	if err != nil {
		if os.Getenv(requiredEnv) != "" {
			require.NoError(t, err, "%s is set, so a database was expected", requiredEnv)
		}
		t.Skipf("postgres unreachable at %s:%s, skipping: %v", cfg.Host, cfg.Port, err)
	}

	db := postgres.NewPostgres(conn).ShowSQL(true)
	require.NoError(t, db.Sync(context.Background(), TestUser{}))

	return db, conn.Close
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// newTestUser returns a user whose unique columns cannot collide with another
// test, or with a row left behind by an earlier run.
func newTestUser(name string) TestUser {
	suffix := xid.New().String()
	return TestUser{
		Name:     fmt.Sprintf("%s-%s", name, suffix),
		FullName: "Test Name",
		Email:    fmt.Sprintf("%s%s@test.test", name, suffix),
	}
}

func TestPostgres_Sync(t *testing.T) {
	db, closer := initializeDB(t)
	defer closer()

	err := db.Sync(context.Background(), TestUser{})
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

	user := TestUser{}
	//db = db.Table("test_user")

	t.Run("find user by id", func(t *testing.T) {
		seeded := newTestUser("find")
		id, err := db.InsertOne(ctx, &seeded)
		require.NoError(t, err)

		has, err := db.ID(id).FindOne(ctx, &user)
		assert.Nil(t, err)
		assert.True(t, has)
		assert.Equal(t, seeded.Name, user.Name)
	})

	t.Run("find user by filter", func(t *testing.T) {
		has, err := db.Where("email=?", "test@test.test").FindOne(ctx, &user, TestUser{Name: "test"})
		assert.Nil(t, err)
		assert.False(t, has)
	})
}

func TestPostgres_FindMany(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	var users []TestUser
	//db = db.Table("test_user")

	t.Run("find all", func(t *testing.T) {
		err := db.FindMany(ctx, &users)
		assert.Nil(t, err)
	})

	t.Run("find by filter", func(t *testing.T) {
		err := db.FindMany(ctx, &users, TestUser{Email: "masudjuly02@gmail.com"})
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

	db = db.Table("test_user")
	t.Run("insert data", func(t *testing.T) {
		suffix := xid.New().String()
		//suffix := "hello"
		user := TestUser{
			Name:     "test-" + suffix,
			FullName: "Test Name",
			Email:    fmt.Sprintf("test%v@test.test", suffix),
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

func TestPostgres_UpdateOne(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("test_user")
	user := TestUser{
		Name:     "test",
		FullName: "Test Name",
		Email:    "test@example.com",
	}
	db.InsertOne(ctx, &user)

	t.Run("update data", func(t *testing.T) {
		update := TestUser{
			FullName: "Test Name 2",
		}
		err := db.Where("name='test'").UpdateOne(ctx, update)
		assert.Nil(t, err)
	})
}

func TestPostgres_DeleteOne(t *testing.T) {
	ctx := context.Background()
	db, closer := initializeDB(t)
	defer closer()

	db = db.Table("test_user")
	user := TestUser{Name: "del", Email: "del@e.c"}
	id, _ := db.InsertOne(ctx, &user)

	t.Run("delete data", func(t *testing.T) {
		err := db.ID(id).DeleteOne(ctx)
		assert.Nil(t, err)
	})
	t.Run("delete data from filter", func(t *testing.T) {
		user2 := TestUser{Name: "del2", Email: "del2@e.c"}
		id2, _ := db.InsertOne(ctx, &user2)
		err := db.DeleteOne(ctx, TestUser{ID: id2.(int64)})
		assert.Nil(t, err)
	})
}
