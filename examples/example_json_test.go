package examples_test

import (
	"encoding/json"
	"fmt"
)

// Address is stored as a JSON column via the `json` tag option.
type Address struct {
	Street string `json:"street"`
	City   string `json:"city"`
}

// Contact demonstrates both JSON storage styles.
type Contact struct {
	ID      int64           `db:"id,pk autoincr"`
	Name    string          `db:"name"`
	Address Address         `db:"address,json"` // marshaled/unmarshaled by Styx
	Extra   json.RawMessage `db:"extra"`        // json.RawMessage is JSON automatically
}

// Example_jsonColumns shows writing and reading JSON columns.
func Example_jsonColumns() {
	db := openDB()
	db.Sync(ctx, Contact{})

	db.Table("contact").InsertOne(ctx, &Contact{
		Name:    "alice",
		Address: Address{Street: "Road 1", City: "Dhaka"},
		Extra:   json.RawMessage(`{"vip":true}`),
	})

	var c Contact
	db.Table("contact").ID(1).FindOne(ctx, &c)
	fmt.Println(c.Name, c.Address.City, string(c.Extra))

	// Output:
	// alice Dhaka {"vip":true}
}
