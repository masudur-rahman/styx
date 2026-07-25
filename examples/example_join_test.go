package examples_test

import "fmt"

// Post belongs to an Account via AuthorID.
type Post struct {
	ID       int64  `db:"id,pk autoincr"`
	AuthorID int64  `db:"author_id"`
	Title    string `db:"title"`
}

// postWithAuthor receives the projected columns from a joined query.
type postWithAuthor struct {
	Title string `db:"title"`
	Name  string `db:"name"`
}

// Example_join shows a JOIN projecting columns from both tables into a flat
// result struct. Select the columns you need with Columns.
func Example_join() {
	db := openDB()
	db.Sync(ctx, Account{}, Post{})
	db.Table("account").InsertOne(ctx, &Account{Name: "alice"})
	db.Table("post").InsertOne(ctx, &Post{AuthorID: 1, Title: "hello"})

	var rows []postWithAuthor
	db.Table("post").
		Columns("post.title", "account.name").
		Join("account", "account.id = post.author_id").
		FindMany(ctx, &rows)

	for _, r := range rows {
		fmt.Println(r.Title, "by", r.Name)
	}

	// Output:
	// hello by alice
}

// PostWithAuthor embeds a nested Author struct hydrated from a JOIN.
type PostWithAuthor struct {
	ID     int64   `db:"id"`
	Title  string  `db:"title"`
	Author Account `db:"author"`
}

// Example_joinNested shows JOIN columns aliased as "author.<col>" hydrating a
// nested struct field named by the alias prefix.
func Example_joinNested() {
	db := openDB()
	db.Sync(ctx, Account{}, Post{})
	db.Table("account").InsertOne(ctx, &Account{Name: "alice", Email: "alice@example.com"})
	db.Table("post").InsertOne(ctx, &Post{AuthorID: 1, Title: "hello"})

	var rows []PostWithAuthor
	db.Table("post").
		Columns(`post.id AS id`, `post.title AS title`,
			`account.name AS "author.name"`, `account.email AS "author.email"`).
		Join("account", "account.id = post.author_id").
		FindMany(ctx, &rows)

	for _, r := range rows {
		fmt.Printf("%q by %s <%s>\n", r.Title, r.Author.Name, r.Author.Email)
	}

	// Output:
	// "hello" by alice <alice@example.com>
}
