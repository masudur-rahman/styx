package examples

import (
	"context"
	"time"

	"github.com/masudur-rahman/styx/sql"
	"github.com/masudur-rahman/styx/sql/sqlite"
	"github.com/masudur-rahman/styx/sql/sqlite/lib"
)

type User struct {
	ID        int64     `db:"id,pk autoincr"`
	Name      string    `db:"name,uq"`
	FullName  string    `db:"full_name,uqs"`
	Email     string    `db:",uqs"`
	CreatedAt time.Time `db:"created_at"`
}

func main() {
	ctx := context.Background()

	// Create sqlite connection
	conn, _ := lib.GetSQLiteConnection("test.db")

	// Start a database engine
	var db sql.Engine
	db = sqlite.NewSQLite(conn)

	// Migrate database
	db.Sync(ctx, User{})

	db = db.Table("user")

	// Insert
	db.InsertOne(ctx, &User{Name: "masud", FullName: "Masudur Rahman", Email: "masud@example.com"})

	// Read
	var user User
	db.ID(1).FindOne(ctx, &user)
	db.Where("email=?", "masud@example.com").FindOne(ctx, &user)
	db.FindOne(ctx, &user, User{Name: "masud"})
	db.Columns("name", "email").FindOne(ctx, &user, User{Name: "masud"}) // fetch only name, email columns

	// Update
	db.ID(user.ID).UpdateOne(ctx, User{Email: "test@example.com"})
	db.Where("email=?", "test@example.com").UpdateOne(ctx, User{FullName: "Test User"})

	// Delete
	db.ID(1).DeleteOne(ctx)                // delete by id
	db.DeleteOne(ctx, User{Name: "masud"}) // delete using filter
}
