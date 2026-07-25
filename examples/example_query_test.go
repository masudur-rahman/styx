package examples_test

import (
	"fmt"

	isql "github.com/masudur-rahman/styx/v2/sql"
)

// Example_orderingAndPagination shows OrderBy together with Paginate.
func Example_orderingAndPagination() {
	db := openDB()
	db.Sync(ctx, Account{})
	db.Table("account").InsertMany(ctx, []any{
		&Account{Name: "alice", Age: 30},
		&Account{Name: "bob", Age: 25},
		&Account{Name: "carol", Age: 35},
		&Account{Name: "dave", Age: 28},
	})

	var byAge []Account
	db.Table("account").OrderBy("age", "ASC").FindMany(ctx, &byAge)
	fmt.Print("by age asc:")
	for _, a := range byAge {
		fmt.Print(" ", a.Name)
	}
	fmt.Println()

	// Page 1 with 2 rows per page, oldest first.
	var page []Account
	db.Table("account").OrderBy("age", "DESC").Paginate(1, 2).FindMany(ctx, &page)
	fmt.Print("page 1:")
	for _, a := range page {
		fmt.Print(" ", a.Name)
	}
	fmt.Println()

	// Output:
	// by age asc: bob dave alice carol
	// page 1: carol alice
}

// Example_likeAndIn shows the LIKE and IN filters.
func Example_likeAndIn() {
	db := openDB()
	db.Sync(ctx, Account{})
	db.Table("account").InsertMany(ctx, []any{
		&Account{Name: "alice"}, &Account{Name: "alan"}, &Account{Name: "bob"},
	})

	var like []Account
	db.Table("account").Like("name", "al%").OrderBy("name", "ASC").FindMany(ctx, &like)
	fmt.Print("like al%:")
	for _, a := range like {
		fmt.Print(" ", a.Name)
	}
	fmt.Println()

	var in []Account
	db.Table("account").In("name", "alice", "bob").OrderBy("name", "ASC").FindMany(ctx, &in)
	fmt.Print("in [alice,bob]:")
	for _, a := range in {
		fmt.Print(" ", a.Name)
	}
	fmt.Println()

	// Output:
	// like al%: alan alice
	// in [alice,bob]: alice bob
}

// Sale is used to demonstrate aggregates and grouping.
type Sale struct {
	ID      int64  `db:"id,pk autoincr"`
	Product string `db:"product"`
	Amount  int    `db:"amount"`
}

type productTotal struct {
	Product string `db:"product"`
	Total   int    `db:"total"`
}

// Example_aggregatesAndGrouping shows Sum with GroupBy.
func Example_aggregatesAndGrouping() {
	db := openDB()
	db.Sync(ctx, Sale{})
	db.Table("sale").InsertMany(ctx, []any{
		&Sale{Product: "book", Amount: 10},
		&Sale{Product: "book", Amount: 15},
		&Sale{Product: "pen", Amount: 3},
	})

	var totals []productTotal
	db.Table("sale").
		Columns("product").
		Select(isql.Sum("amount").As("total")).
		GroupBy("product").
		OrderBy("product", "ASC").
		FindMany(ctx, &totals)

	for _, t := range totals {
		fmt.Printf("%s=%d\n", t.Product, t.Total)
	}

	// Output:
	// book=25
	// pen=3
}
