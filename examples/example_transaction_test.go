package examples_test

import "fmt"

// Example_transaction shows commit and rollback semantics. BeginTx returns a
// transaction-scoped engine; Commit or Rollback ends it.
func Example_transaction() {
	db := openDB()
	db.Sync(ctx, Account{})

	// Rolled-back work leaves no trace.
	tx, _ := db.BeginTx(ctx)
	tx.Table("account").InsertOne(ctx, &Account{Name: "alice"})
	tx.Rollback()

	var after []Account
	db.Table("account").FindMany(ctx, &after)
	fmt.Println("rows after rollback:", len(after))

	// Committed work persists.
	tx, _ = db.BeginTx(ctx)
	tx.Table("account").InsertOne(ctx, &Account{Name: "bob"})
	tx.Commit()

	var committed []Account
	db.Table("account").FindMany(ctx, &committed)
	fmt.Println("rows after commit:", len(committed))

	// Output:
	// rows after rollback: 0
	// rows after commit: 1
}
