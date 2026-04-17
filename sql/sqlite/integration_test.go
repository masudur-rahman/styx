package sqlite_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/masudur-rahman/styx/sql"
	"github.com/masudur-rahman/styx/sql/sqlite"
	"github.com/masudur-rahman/styx/sql/sqlite/lib"

	"github.com/stretchr/testify/assert"
)

type User struct {
	ID        int64      `db:"id,pk autoincr"`
	Name      string     `db:"name,uq"`
	Email     string     `db:"email,uq"`
	Age       int        `db:"age"`
	DeletedAt *time.Time `db:"deleted_at,soft_delete"`
}

type Post struct {
	ID     int64  `db:"id,pk autoincr"`
	UserID int64  `db:"user_id"`
	Title  string `db:"title"`
	Body   string `db:"body"`
}

func setupDB(t *testing.T) sql.Engine {
	conn, err := lib.GetSQLiteConnection(":memory:")
	assert.NoError(t, err)

	db := sqlite.NewSQLite(conn)
	db.ShowSQL(true)
	err = db.Sync(context.Background(), User{}, Post{})
	assert.NoError(t, err)

	return db
}

func TestIntegration_AllFeatures(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)

	// 1. Validation (Indirectly tested via Insert)
	user := &User{Name: "Masud", Email: "masud@example.com", Age: 30}
	id, err := db.InsertOne(ctx, user)
	assert.NoError(t, err)
	assert.NotNil(t, id)
	assert.Equal(t, int64(1), user.ID)

	// 2. Pagination
	for i := 2; i <= 10; i++ {
		db.InsertOne(ctx, &User{Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("user%d@example.com", i), Age: 20 + i})
	}

	var users []User
	err = db.Table("user").OrderBy("id", "ASC").Limit(2).Offset(0).FindMany(ctx, &users)
	assert.NoError(t, err)
	assert.Len(t, users, 2)
	assert.Equal(t, int64(1), users[0].ID)

	// 3. Soft Delete
	err = db.Table("user").DeleteOne(ctx, User{ID: 1})
	assert.NoError(t, err)

	var u User
	found, err := db.Table("user").ID(1).FindOne(ctx, &u)
	assert.NoError(t, err)
	assert.False(t, found, "User should be soft deleted and not found by default")

	found, err = db.Table("user").ID(1).WithDeleted().FindOne(ctx, &u)
	assert.NoError(t, err)
	assert.True(t, found, "User should be found with WithDeleted")
	assert.NotNil(t, u.DeletedAt)

	// 4. Join
	db.Table("post").InsertOne(ctx, &Post{UserID: 2, Title: "Hello", Body: "World"})

	type UserPost struct {
		UserName  string `db:"name"`
		PostTitle string `db:"title"`
	}
	var results []UserPost
	err = db.Table("user").
		Join("post", "user.id = post.user_id").
		Columns("user.name", "post.title").
		FindMany(ctx, &results)

	assert.NoError(t, err)
	assert.NotEmpty(t, results)
	assert.Equal(t, "User2", results[0].UserName)
	assert.Equal(t, "Hello", results[0].PostTitle)

	// 5. Aggregates
	db = setupDB(t) // reset
	db.InsertOne(ctx, &User{Name: "A", Email: "a@e.c", Age: 10})
	db.InsertOne(ctx, &User{Name: "B", Email: "b@e.c", Age: 20})

	type Stats struct {
		AvgAge float64 `db:"avg_age"`
	}
	var stats Stats
	err = db.Table("user").Avg("age", "avg_age").FindMany(ctx, &stats)
	assert.NoError(t, err)
	// FindMany into a non-slice might be tricky, usually it expects a slice.
	// Let's use a slice.
	var statsList []Stats
	err = db.Table("user").Avg("age", "avg_age").FindMany(ctx, &statsList)
	assert.NoError(t, err)
	assert.NotEmpty(t, statsList)
	assert.Equal(t, 15.0, statsList[0].AvgAge)
}
