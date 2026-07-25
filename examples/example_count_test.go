package examples_test

import "fmt"

// Example_count uses the terminal Count to get row counts directly as an int64,
// without scanning results into structs. Soft-deleted rows are excluded by
// default (the table's soft-delete column is registered at Sync time).
func Example_count() {
	db := openDB()
	db.Sync(ctx, Account{})
	db.Table("account").InsertMany(ctx, []any{
		&Account{Name: "alice", Age: 30},
		&Account{Name: "bob", Age: 25},
		&Account{Name: "carol", Age: 35},
	})

	total, _ := db.Table("account").Count(ctx)
	fmt.Println("total:", total)

	adults, _ := db.Table("account").Where("age >= ?", 30).Count(ctx)
	fmt.Println("age>=30:", adults)

	// Output:
	// total: 3
	// age>=30: 2
}

// Example_countSoftDelete shows that Count excludes soft-deleted rows by default
// (the soft-delete column is registered at Sync time) and includes them with
// WithDeleted.
func Example_countSoftDelete() {
	db := openDB()
	db.Sync(ctx, Note{})
	db.Table("note").InsertMany(ctx, []any{&Note{Title: "a"}, &Note{Title: "b"}})

	// Soft delete row 1.
	db.Table("note").DeleteOne(ctx, Note{ID: 1})

	live, _ := db.Table("note").Count(ctx)
	fmt.Println("live:", live)

	all, _ := db.Table("note").WithDeleted().Count(ctx)
	fmt.Println("including deleted:", all)

	// Output:
	// live: 1
	// including deleted: 2
}
