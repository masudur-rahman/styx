package examples_test

import "fmt"

// Session shows two struct tags: db:"-" marks Cache as in-memory only (never
// persisted), and notnull makes Token a NOT NULL column.
type Session struct {
	ID    int64  `db:"id,pk autoincr"`
	Token string `db:"token,notnull"`
	Cache string `db:"-"`
}

// Example_ignoreAndNotNull shows the db:"-" ignore tag and the notnull tag.
func Example_ignoreAndNotNull() {
	db := openDB()
	db.Sync(ctx, Session{})

	s := &Session{Token: "abc", Cache: "transient"}
	db.Table("session").InsertOne(ctx, s)

	// Cache is never stored or scanned; Token round-trips normally.
	var got Session
	db.Table("session").ID(s.ID).FindOne(ctx, &got)
	fmt.Println("token:", got.Token)
	fmt.Println("cache persisted:", got.Cache != "")

	// The NOT NULL column rejects a missing value.
	_, err := db.Table("session").InsertOne(ctx, &Session{})
	fmt.Println("missing token rejected:", err != nil)

	// Output:
	// token: abc
	// cache persisted: false
	// missing token rejected: true
}
