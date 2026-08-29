package sqlite_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/masudur-rahman/styx/v2/sql"
	"github.com/masudur-rahman/styx/v2/sql/sqlite"

	"github.com/stretchr/testify/assert"
)

type User struct {
	ID        int64      `db:"id,pk autoincr"`
	Name      string     `db:"name,uq"`
	Email     string     `db:"email,uq"`
	Age       int        `db:"age"`
	DeletedAt *time.Time `db:"deleted_at,archive"`
}

type Post struct {
	ID     int64  `db:"id,pk autoincr"`
	UserID int64  `db:"user_id"`
	Title  string `db:"title"`
	Body   string `db:"body"`
}

func setupDB(t *testing.T) sql.Engine {
	conn, err := sqlite.GetSQLiteConnection(":memory:")
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
	err = db.Table("user").Select(sql.Avg("age").As("avg_age")).FindMany(ctx, &stats)
	assert.NoError(t, err)
	// FindMany into a non-slice might be tricky, usually it expects a slice.
	// Let's use a slice.
	var statsList []Stats
	err = db.Table("user").Select(sql.Avg("age").As("avg_age")).FindMany(ctx, &statsList)
	assert.NoError(t, err)
	assert.NotEmpty(t, statsList)
	assert.Equal(t, 15.0, statsList[0].AvgAge)
}

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Event struct {
	ID       int64           `db:"id,pk autoincr"`
	Name     string          `db:"name"`
	Payload  json.RawMessage `db:"payload"`
	Location Location        `db:"location,json"`
	Extra    *Location       `db:"extra,json"`
}

func TestIntegration_JSONFields(t *testing.T) {
	ctx := context.Background()
	conn, err := sqlite.GetSQLiteConnection(":memory:")
	assert.NoError(t, err)
	db := sqlite.NewSQLite(conn)
	assert.NoError(t, db.Sync(ctx, Event{}))

	ev := &Event{
		Name:     "visit",
		Payload:  json.RawMessage(`{"note":"first"}`),
		Location: Location{Lat: 23.8, Lon: 90.4},
	}
	_, err = db.InsertOne(ctx, ev)
	assert.NoError(t, err)

	// Round-trip: RawMessage and json-tagged struct come back intact
	var got Event
	found, err := db.Table("event").ID(1).FindOne(ctx, &got)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.JSONEq(t, `{"note":"first"}`, string(got.Payload))
	assert.Equal(t, Location{Lat: 23.8, Lon: 90.4}, got.Location)
	assert.Nil(t, got.Extra, "NULL column stays nil pointer")

	// Update through the json path
	err = db.Table("event").ID(1).UpdateOne(ctx, Event{
		Payload: json.RawMessage(`{"note":"second"}`),
		Extra:   &Location{Lat: 1, Lon: 2},
	})
	assert.NoError(t, err)

	var updated Event
	_, err = db.Table("event").ID(1).FindOne(ctx, &updated)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"note":"second"}`, string(updated.Payload))
	assert.Equal(t, &Location{Lat: 1, Lon: 2}, updated.Extra)
}

func TestCount_filtersAndSoftDelete(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)

	for _, u := range []*User{
		{Name: "a", Email: "a@e.c", Age: 20},
		{Name: "b", Email: "b@e.c", Age: 30},
		{Name: "c", Email: "c@e.c", Age: 40},
	} {
		_, err := db.Table("user").InsertOne(ctx, u)
		assert.NoError(t, err)
	}

	total, err := db.Table("user").Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), total)

	filtered, err := db.Table("user").Where("age >= ?", 30).Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), filtered)

	// Soft-delete one row: the default count excludes it, WithDeleted includes it.
	err = db.Table("user").DeleteOne(ctx, User{Name: "a"})
	assert.NoError(t, err)

	live, err := db.Table("user").Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), live)

	all, err := db.Table("user").WithDeleted().Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), all)
}

type widget struct {
	ID       int64  `db:"id,pk autoincr"`
	Name     string `db:"name"`
	Computed string `db:"-"` // in-memory only, never persisted
}

func TestIgnoreTag_notPersisted(t *testing.T) {
	ctx := context.Background()
	conn, err := sqlite.GetSQLiteConnection(":memory:")
	assert.NoError(t, err)
	db := sqlite.NewSQLite(conn)

	// Sync succeeds: a db:"-" field must not emit an invalid "-" column in DDL.
	assert.NoError(t, db.Sync(ctx, widget{}))

	w := &widget{Name: "gear", Computed: "transient"}
	_, err = db.Table("widget").InsertOne(ctx, w)
	assert.NoError(t, err)

	// The ignored field is neither stored nor scanned back: it round-trips as zero
	// while persistent columns survive.
	var got widget
	found, err := db.Table("widget").ID(w.ID).FindOne(ctx, &got)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "gear", got.Name)
	assert.Empty(t, got.Computed, "ignored field is never stored or scanned")
}

type account struct {
	ID   int64  `db:"id,pk autoincr"`
	Name string `db:"name,notnull"`
}

func TestNotNull_rejectsMissingValue(t *testing.T) {
	ctx := context.Background()
	conn, err := sqlite.GetSQLiteConnection(":memory:")
	assert.NoError(t, err)
	db := sqlite.NewSQLite(conn)
	assert.NoError(t, db.Sync(ctx, account{}))

	// Name omitted (zero value) → column left NULL → NOT NULL constraint fails.
	_, err = db.Table("account").InsertOne(ctx, &account{})
	assert.Error(t, err, "NOT NULL column must reject a missing value")

	// A populated NOT NULL column inserts fine.
	_, err = db.Table("account").InsertOne(ctx, &account{Name: "acme"})
	assert.NoError(t, err)
}

// seedAges inserts one user per age, so a filter on the age matches as many
// rows as the age repeats.
func seedAges(t *testing.T, db sql.Engine, ages ...int) {
	t.Helper()
	ctx := context.Background()
	for i, age := range ages {
		_, err := db.Table("user").InsertOne(ctx, &User{
			Name:  fmt.Sprintf("user%d", i),
			Email: fmt.Sprintf("user%d@e.c", i),
			Age:   age,
		})
		assert.NoError(t, err)
	}
}

// countAge returns how many live rows carry the given age.
func countAge(t *testing.T, db sql.Engine, age int) int64 {
	t.Helper()
	n, err := db.Table("user").Where("age = ?", age).Count(context.Background())
	assert.NoError(t, err)
	return n
}

// TestUpdateOne_changesOnlyOneMatch is the behaviour this pair exists for: a
// filter matching three rows used to update all three.
func TestUpdateOne_changesOnlyOneMatch(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)
	seedAges(t, db, 30, 30, 30, 40)

	err := db.Table("user").Where("age = ?", 30).UpdateOne(ctx, User{Age: 31})
	assert.NoError(t, err)

	assert.Equal(t, int64(2), countAge(t, db, 30))
	assert.Equal(t, int64(1), countAge(t, db, 31))
	assert.Equal(t, int64(1), countAge(t, db, 40), "rows outside the filter are untouched")
}

func TestUpdateMany_changesEveryMatch(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)
	seedAges(t, db, 30, 30, 30, 40)

	changed, err := db.Table("user").Where("age = ?", 30).UpdateMany(ctx, User{Age: 31})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), changed)

	assert.Equal(t, int64(0), countAge(t, db, 30))
	assert.Equal(t, int64(3), countAge(t, db, 31))
	assert.Equal(t, int64(1), countAge(t, db, 40), "rows outside the filter are untouched")
}

func TestUpdateMany_noMatchReturnsZero(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)
	seedAges(t, db, 30)

	changed, err := db.Table("user").Where("age = ?", 99).UpdateMany(ctx, User{Age: 31})
	assert.NoError(t, err, "matching nothing is not an error for a bulk update")
	assert.Equal(t, int64(0), changed)
}

// TestDeleteOne_removesOnlyOneMatch covers the soft-delete path, since User
// carries an archive column.
func TestDeleteOne_removesOnlyOneMatch(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)
	seedAges(t, db, 30, 30, 30, 40)

	err := db.Table("user").DeleteOne(ctx, User{Age: 30})
	assert.NoError(t, err)

	assert.Equal(t, int64(2), countAge(t, db, 30))

	all, err := db.Table("user").WithDeleted().Count(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), all, "soft delete marks the row, it does not remove it")
}

func TestDeleteMany_removesEveryMatch(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)
	seedAges(t, db, 30, 30, 30, 40)

	removed, err := db.Table("user").DeleteMany(ctx, User{Age: 30})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), removed)

	assert.Equal(t, int64(0), countAge(t, db, 30))
	assert.Equal(t, int64(1), countAge(t, db, 40), "rows outside the filter are untouched")
}

func TestDeleteMany_noMatchReturnsZero(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)
	seedAges(t, db, 30)

	removed, err := db.Table("user").DeleteMany(ctx, User{Age: 99})
	assert.NoError(t, err, "matching nothing is not an error for a bulk delete")
	assert.Equal(t, int64(0), removed)
}
