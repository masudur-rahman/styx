package examples_test

import "fmt"

// Order is a purchase row used by the CTE example.
type Order struct {
	ID     int64 `db:"id,pk autoincr"`
	UserID int64 `db:"user_id"`
	Total  int64 `db:"total"`
}

// Example_cte shows With: a filtered aggregate CTE referenced as a table by the
// outer query via Join. The CTE compiles to a subquery without executing on its
// own, and its args are spliced into the final statement.
func Example_cte() {
	db := openDB()
	db.Sync(ctx, Account{}, Order{})

	db.Table("account").InsertMany(ctx, []any{
		&Account{Name: "alice", Email: "a@x.io"},
		&Account{Name: "bob", Email: "b@x.io"},
	})
	db.Table("order").InsertMany(ctx, []any{
		&Order{UserID: 1, Total: 700},
		&Order{UserID: 1, Total: 500},
		&Order{UserID: 2, Total: 100},
	})

	// CTE: users whose orders sum to more than 1000.
	spenders := db.Table("order").Columns("user_id").
		GroupBy("user_id").Having("SUM(total) > ?", 1000)

	var big []Account
	db.With("big_spenders", spenders).
		Table("account").
		Join("big_spenders", "account.id = big_spenders.user_id").
		FindMany(ctx, &big)

	fmt.Printf("%d big spender(s):", len(big))
	for _, a := range big {
		fmt.Print(" ", a.Name)
	}
	fmt.Println()

	// Output:
	// 1 big spender(s): alice
}
