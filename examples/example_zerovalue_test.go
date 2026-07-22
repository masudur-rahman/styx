package examples_test

import "fmt"

// Budget shows intentional zero values: an empty CategoryID means the overall
// budget, so it must not be skipped. The `req` tag keeps it in every query.
type Budget struct {
	ID         int64  `db:"id,pk autoincr"`
	UserID     int64  `db:"user_id"`
	CategoryID string `db:"category_id,req"`
	Amount     int    `db:"amount"`
}

// Example_zeroValueControl shows req and MustFilterCols keeping zero values.
func Example_zeroValueControl() {
	db := openDB()
	db.Sync(ctx, Budget{})

	// CategoryID "" is intentional; req keeps it in the INSERT.
	db.Table("budget").InsertOne(ctx, &Budget{UserID: 1, CategoryID: "", Amount: 500})

	// MustFilterCols forces the zero-valued category_id into the WHERE clause.
	var b Budget
	db.Table("budget").
		MustFilterCols("category_id").
		FindOne(ctx, &b, Budget{UserID: 1, CategoryID: ""})
	fmt.Println("overall budget amount:", b.Amount)

	// Output:
	// overall budget amount: 500
}
