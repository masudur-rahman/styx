package examples_test

import (
	"fmt"
	"time"
)

// Note has a soft-delete column: deletes set the timestamp instead of removing rows.
type Note struct {
	ID        int64      `db:"id,pk autoincr"`
	Title     string     `db:"title"`
	DeletedAt *time.Time `db:"deleted_at,soft_delete"`
}

// Example_softDelete shows soft delete, WithDeleted, and Restore.
func Example_softDelete() {
	db := openDB()
	db.Sync(ctx, Note{})
	db.Table("note").InsertMany(ctx, []any{&Note{Title: "a"}, &Note{Title: "b"}})

	// Soft delete row 1 — sets deleted_at instead of removing it.
	db.Table("note").DeleteOne(ctx, Note{ID: 1})

	var live []Note
	db.Table("note").OrderBy("id", "ASC").FindMany(ctx, &live)
	fmt.Println("live rows:", len(live))

	var all []Note
	db.Table("note").WithDeleted().OrderBy("id", "ASC").FindMany(ctx, &all)
	fmt.Println("including deleted:", len(all))

	// Restore clears the soft-delete marker.
	db.Table("note").Restore(ctx, Note{ID: 1})
	var restored []Note
	db.Table("note").OrderBy("id", "ASC").FindMany(ctx, &restored)
	fmt.Println("after restore:", len(restored))

	// Output:
	// live rows: 1
	// including deleted: 2
	// after restore: 2
}
