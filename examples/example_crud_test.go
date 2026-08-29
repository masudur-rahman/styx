package examples_test

import "fmt"

// Example_crud shows the basic Create, Read, Update, Delete lifecycle.
func Example_crud() {
	db := openDB()
	if err := db.Sync(ctx, Account{}); err != nil {
		panic(err)
	}

	// Create — InsertOne returns the generated primary key.
	id, _ := db.Table("account").InsertOne(ctx, &Account{Name: "alice", Email: "alice@example.com", Age: 30})
	fmt.Println("inserted id:", id)

	// Read by primary key.
	var acc Account
	found, _ := db.Table("account").ID(1).FindOne(ctx, &acc)
	fmt.Println("found:", found, acc.Name, acc.Email)

	// Update — only non-zero fields are written.
	db.Table("account").ID(1).UpdateOne(ctx, Account{Email: "alice@new.com"})
	db.Table("account").ID(1).FindOne(ctx, &acc)
	fmt.Println("updated email:", acc.Email)

	// Delete.
	db.Table("account").ID(1).DeleteOne(ctx)
	found, _ = db.Table("account").ID(1).FindOne(ctx, &acc)
	fmt.Println("exists after delete:", found)

	// Output:
	// inserted id: 1
	// found: true alice alice@example.com
	// updated email: alice@new.com
	// exists after delete: false
}

// Example_filters shows the different ways to filter a query.
func Example_filters() {
	db := openDB()
	db.Sync(ctx, Account{})
	db.Table("account").InsertMany(ctx, []any{
		&Account{Name: "alice", Email: "alice@example.com", Age: 30},
		&Account{Name: "bob", Email: "bob@example.com", Age: 25},
	})

	var acc Account

	// Raw WHERE condition with ? placeholders.
	db.Table("account").Where("email = ?", "bob@example.com").FindOne(ctx, &acc)
	fmt.Println("by where:", acc.Name)

	// Struct filter — zero-value fields are skipped automatically.
	db.Table("account").FindOne(ctx, &acc, Account{Name: "alice"})
	fmt.Println("by struct filter:", acc.Name)

	// Column projection — fetch only selected columns.
	var partial Account
	db.Table("account").Columns("name").FindOne(ctx, &partial, Account{Name: "alice"})
	fmt.Println("projected:", partial.Name, "email empty:", partial.Email == "")

	// Output:
	// by where: bob
	// by struct filter: alice
	// projected: alice email empty: true
}

// Example_updateManyDeleteMany shows the difference between the One and Many
// forms. A filter matching three rows changes one of them with UpdateOne and
// all three with UpdateMany.
func Example_updateManyDeleteMany() {
	db := openDB()
	db.Sync(ctx, Account{})
	db.Table("account").InsertMany(ctx, []any{
		&Account{Name: "alice", Email: "alice@example.com", Age: 30},
		&Account{Name: "bob", Email: "bob@example.com", Age: 30},
		&Account{Name: "carol", Email: "carol@example.com", Age: 30},
		&Account{Name: "dave", Email: "dave@example.com", Age: 40},
	})

	// UpdateOne changes a single row, even though three match.
	db.Table("account").Where("age = ?", 30).UpdateOne(ctx, Account{Age: 31})
	left, _ := db.Table("account").Where("age = ?", 30).Count(ctx)
	fmt.Println("still aged 30:", left)

	// UpdateMany changes the rest, and reports how many.
	changed, _ := db.Table("account").Where("age = ?", 30).UpdateMany(ctx, Account{Age: 31})
	fmt.Println("changed by UpdateMany:", changed)

	// DeleteMany removes every match and leaves the others alone.
	removed, _ := db.Table("account").DeleteMany(ctx, Account{Age: 31})
	fmt.Println("removed:", removed)

	remaining, _ := db.Table("account").Count(ctx)
	fmt.Println("remaining:", remaining)

	// Matching nothing is not an error for the Many forms.
	none, err := db.Table("account").Where("age = ?", 99).DeleteMany(ctx)
	fmt.Println("no match:", none, err)

	// Output:
	// still aged 30: 2
	// changed by UpdateMany: 2
	// removed: 3
	// remaining: 1
	// no match: 0 <nil>
}
