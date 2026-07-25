package examples_test

import "fmt"

// Example_bulkInsert shows InsertMany, which emits a single multi-row INSERT and
// assigns the generated primary keys back onto each document.
func Example_bulkInsert() {
	db := openDB()
	db.Sync(ctx, Account{})

	accounts := []any{
		&Account{Name: "alice", Age: 30},
		&Account{Name: "bob", Age: 25},
		&Account{Name: "carol", Age: 35},
	}
	ids, _ := db.Table("account").InsertMany(ctx, accounts)
	fmt.Println("returned ids:", ids)
	fmt.Println("first doc id assigned:", accounts[0].(*Account).ID)

	var all []Account
	db.Table("account").OrderBy("id", "ASC").FindMany(ctx, &all)
	fmt.Println("row count:", len(all))

	// Output:
	// returned ids: [1 2 3]
	// first doc id assigned: 1
	// row count: 3
}
