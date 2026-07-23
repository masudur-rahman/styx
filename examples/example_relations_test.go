package examples_test

import "fmt"

// Author has many Books (foreign key author_id on the books table).
type Author struct {
	ID    int64  `db:"id,pk autoincr"`
	Name  string `db:"name"`
	Books []Book `db:"-,o2m fk:author_id"`
}

// Book belongs to an Author and has many Tags through the book_tags join table.
type Book struct {
	ID       int64   `db:"id,pk autoincr"`
	AuthorID int64   `db:"author_id"`
	Title    string  `db:"title"`
	Author   *Author `db:"-,m2o fk:author_id"`
	Tags     []Tag   `db:"-,m2m join:book_tags fk:book_id ref:tag_id"`
}

type Tag struct {
	ID   int64  `db:"id,pk autoincr"`
	Name string `db:"name"`
}

// BookTag is the many-to-many join table.
type BookTag struct {
	ID     int64 `db:"id,pk autoincr"`
	BookID int64 `db:"book_id"`
	TagID  int64 `db:"tag_id"`
}

// TableName overrides the default snake-case name (book_tag).
func (BookTag) TableName() string { return "book_tags" }

// Example_relations shows Preload for o2m, m2o, and m2m.
// Each preload runs a single batched query (no N+1).
func Example_relations() {
	db := openDB()
	db.Sync(ctx, Author{}, Book{}, Tag{}, BookTag{})

	db.Table("author").InsertOne(ctx, &Author{Name: "alice"})
	db.Table("book").InsertMany(ctx, []any{
		&Book{AuthorID: 1, Title: "go"},
		&Book{AuthorID: 1, Title: "rust"},
	})
	db.Table("tag").InsertMany(ctx, []any{&Tag{Name: "prog"}, &Tag{Name: "sys"}})
	db.Table("book_tags").InsertMany(ctx, []any{
		&BookTag{BookID: 1, TagID: 1},
		&BookTag{BookID: 1, TagID: 2},
		&BookTag{BookID: 2, TagID: 2},
	})

	// o2m: load an author's books.
	var author Author
	db.Table("author").Preload("Books").ID(1).FindOne(ctx, &author)
	fmt.Printf("%s has %d books\n", author.Name, len(author.Books))

	// m2o + m2m: load a book's author and tags.
	var book Book
	db.Table("book").Preload("Author").Preload("Tags").ID(1).FindOne(ctx, &book)
	fmt.Printf("%q by %s, tags:", book.Title, book.Author.Name)
	for _, t := range book.Tags {
		fmt.Print(" ", t.Name)
	}
	fmt.Println()

	// Output:
	// alice has 2 books
	// "go" by alice, tags: prog sys
}
