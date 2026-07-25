# Styx

Database Engine for different SQL and NoSQL databases.

## Install

```shell
go get -u github.com/masudur-rahman/styx
```

## Supported Databases

| Database   | Package              | Status     |
|------------|----------------------|------------|
| SQLite     | `sql/sqlite`         | Stable       |
| PostgreSQL | `sql/postgres`       | Stable       |
| Supabase   | `sql/supabase`       | Experimental |
| ArangoDB   | `nosql/arango`       | Stable       |
| MongoDB    | `nosql/mongo`        | Experimental |

> The SQL engines (SQLite, PostgreSQL) are the primary, fully supported targets.
> Supabase is experimental and only partially implemented.

## Examples

Every feature below has a runnable example under [`examples/`](examples/), written
as Go [example tests](https://pkg.go.dev/testing#hdr-Examples). Run them all with:

```shell
go test ./examples/
```

| Feature | Example |
|---------|---------|
| CRUD + filters (ID / Where / struct filter / Columns) | [`example_crud_test.go`](examples/example_crud_test.go) |
| Ordering, pagination, LIKE / IN, aggregates + GROUP BY | [`example_query_test.go`](examples/example_query_test.go) |
| Row count (`Count`, soft-delete aware) | [`example_count_test.go`](examples/example_count_test.go) |
| Bulk insert (single multi-row `INSERT`) | [`example_bulk_test.go`](examples/example_bulk_test.go) |
| Lifecycle hooks (`BeforeCreate`, `AfterFind`, …) | [`example_hooks_test.go`](examples/example_hooks_test.go) |
| Validation (tag rules + custom `Validate()`) | [`example_validation_test.go`](examples/example_validation_test.go) |
| Soft delete / `WithDeleted` / `Restore` | [`example_softdelete_test.go`](examples/example_softdelete_test.go) |
| JSON columns | [`example_json_test.go`](examples/example_json_test.go) |
| JOIN with column projection + nested struct hydration | [`example_join_test.go`](examples/example_join_test.go) |
| Relationships + `Preload` (o2m / m2o / m2m) | [`example_relations_test.go`](examples/example_relations_test.go) |
| Transactions (commit / rollback) | [`example_transaction_test.go`](examples/example_transaction_test.go) |
| Zero-value control (`req` / `MustFilterCols`) | [`example_zerovalue_test.go`](examples/example_zerovalue_test.go) |
| CTE query builder (`With`) | [`example_cte_test.go`](examples/example_cte_test.go) |
| Connection pool config + `Stats()` | [`example_pool_test.go`](examples/example_pool_test.go) |
| Query observer (logging / metrics / tracing) | [`example_observer_test.go`](examples/example_observer_test.go) |
| Prepared-statement caching (`WithStmtCache`) | [`example_stmtcache_test.go`](examples/example_stmtcache_test.go) |
| Versioned migrations (destructive changes) | [`example_migrate_test.go`](examples/example_migrate_test.go) |

## Quickstart

Check out the [Quickstart Example](examples/quickstart.go) for a complete guide.

```go
package main

import (
	"context"
	"time"

	"github.com/masudur-rahman/styx/sql"
	"github.com/masudur-rahman/styx/sql/sqlite"
	"github.com/masudur-rahman/styx/sql/sqlite/lib"
)

type User struct {
	ID        int64     `db:"id,pk autoincr"`
	Name      string    `db:"name,uq"`
	FullName  string    `db:"full_name,uqs"`
	Email     string    `db:",uqs"`
	CreatedAt time.Time `db:"created_at"`
}

func main() {
	ctx := context.Background()
	conn, _ := lib.GetSQLiteConnection("test.db")

	db := sqlite.NewSQLite(conn)

	// Migrate database
	db.Sync(ctx, User{})

	// Fluent CRUD
	db.Table("user").InsertOne(ctx, &User{Name: "masud", FullName: "Masudur Rahman", Email: "masud@example.com"})

	var user User
	db.Table("user").ID(1).FindOne(ctx, &user)
	db.Table("user").Where("email=?", "masud@example.com").FindOne(ctx, &user)

	db.Table("user").ID(user.ID).UpdateOne(ctx, User{Email: "test@example.com"})

	db.Table("user").ID(1).DeleteOne(ctx)
}
```

## Struct Tags

Styx uses the `db` struct tag to map Go struct fields to database columns and define schema constraints.

### Tag Format

```
db:"column_name,options"
```

- **column_name** (before the comma): Sets the database column name. If empty, the field name is converted to `snake_case` automatically.
- **options** (after the comma): Space-separated list of constraint/behavior flags.

### Available Options

| Tag        | Purpose                          | DDL Effect                                       | Query Effect |
|------------|----------------------------------|--------------------------------------------------|--------------|
| `pk`       | Primary key                      | Adds `PRIMARY KEY` constraint                    | -            |
| `autoincr` | Auto-increment                   | `INTEGER PRIMARY KEY AUTOINCREMENT` (SQLite) / `SERIAL`/`BIGSERIAL` (Postgres) | -            |
| `uq`       | Unique constraint (single column)| Adds `UNIQUE` constraint                         | -            |
| `uqs`      | Unique composite group           | Adds composite `UNIQUE(col1, col2, ...)` across all `uqs` fields | -            |
| `notnull`  | Non-nullable column              | Adds `NOT NULL` constraint                       | -            |
| `-`        | Ignore field (column name `-`)   | Excluded from the table entirely                 | Never written, read, or scanned — for in-memory-only fields |
| `req`      | Required (never skip zero-value) | None                                             | Always includes the field in WHERE, INSERT, and UPDATE queries, even when zero-valued |
| `json`     | Store field as JSON              | `JSONB` (Postgres) / `TEXT` (SQLite)             | Marshals the field on writes, unmarshals on reads |
| `archive`  | Soft-delete marker column        | Timestamp column                                 | `DeleteOne` sets it instead of removing the row; reads filter it out unless `WithDeleted()` |
| `idx`      | Secondary index                  | `CREATE INDEX` on `Sync`                         | -            |
| `uidx` | Unique secondary index         | `CREATE UNIQUE INDEX` on `Sync`                  | -            |

> Indexes can be named for composite coverage: fields sharing the same
> `idx:<name>` (or `uidx:<name>`) are combined into one multi-column index.
> `idx` / `uidx` without a name create a single-column index.

### Relationship Options

Declared on struct fields that hold related entities (the column part is usually
`-`). Excluded from DDL, INSERT, and UPDATE. See
[Relationships & Preload](#relationships--preload) for a full example.

| Option | Extra keys | Meaning |
|--------|-----------|---------|
| `m2o`   | `fk:<col>`                          | to-one; foreign key `<col>` is on **this** table |
| `o2m`     | `fk:<col>`                          | to-many; foreign key `<col>` is on the **child** table |
| `m2m` | `join:<table> fk:<col> ref:<col>`   | to-many through a join table (`fk` → this side, `ref` → other side) |

> Validation rules use a separate `validate:"..."` tag — see [Struct Validation](#struct-validation).

### Examples

```go
type Budget struct {
	ID         int64  `db:"id,pk autoincr"`     // primary key, auto-increment
	UserID     int64  `db:"user_id,uqs"`        // part of composite unique constraint
	CategoryID string `db:"category_id,uqs req"` // composite unique + required (never skipped)
	AlertAt    int64  `db:"alert_at,req"`        // required: always included even when 0
	Amount     int64  `db:"amount"`              // regular field, skipped when zero
	Label      string `db:"label,uq"`           // single-column unique constraint
	Meta       Detail `db:"meta,json"`          // any struct/map/slice stored as JSON
}
```

### JSON Columns

Fields typed `json.RawMessage` are stored as JSON automatically — no tag
needed. Any other type (struct, map, slice) can opt in with the `json`
option; Styx marshals it on INSERT/UPDATE and unmarshals it when scanning
rows. `nil` pointers and empty `json.RawMessage` values are stored as
`NULL`. Plain `[]byte` fields (without the tag) map to `BYTEA`/`BLOB`
instead.

```go
type Clinic struct {
	ID      int64           `db:"id,pk autoincr"`
	Name    string          `db:"name"`
	FHIR    json.RawMessage `db:"fhir_json"`     // JSONB automatically
	Address Address         `db:"address,json"`  // marshaled/unmarshaled by Styx
	Photo   []byte          `db:"photo"`         // BYTEA/BLOB, not JSON
}
```

### How Zero-Value Handling Works

By default, Styx skips zero-valued fields (`""`, `0`, `false`, `time.Time{}`) in:
- **WHERE clauses** (struct filters passed to `FindOne`, `FindMany`, `DeleteOne`)
- **INSERT** queries
- **UPDATE** queries

This is useful most of the time (you don't want `WHERE id=0 AND created_at='0001-01-01'`), but it causes bugs when a zero value is intentional (e.g., `CategoryID=""` means "overall budget").

There are three ways to override this:

#### 1. `req` tag (declarative, per-field)

Mark the field once in the struct definition. It applies to all operations automatically.

```go
type Budget struct {
	CategoryID string `db:"category_id,req"` // "" is always included
}

db.FindOne(ctx, &b, Budget{UserID: 99, CategoryID: ""})
// WHERE user_id=99 AND category_id=''

db.InsertOne(ctx, &Budget{UserID: 99, CategoryID: "", Amount: 500})
// INSERT INTO "budget" (user_id, category_id, amount) VALUES (99, '', 500)
```

#### 2. `MustFilterCols` (per-query, WHERE only)

Opt in per query for specific columns in WHERE clauses.

```go
db.MustFilterCols("category_id").FindOne(ctx, &b, Budget{UserID: 99, CategoryID: ""})
// WHERE user_id=99 AND category_id=''

db.MustFilterCols("category_id").DeleteOne(ctx, Budget{UserID: 99, CategoryID: ""})
// DELETE FROM "budget" WHERE user_id=99 AND category_id=''
```

#### 3. `MustCols` (per-query, INSERT/UPDATE only)

Opt in per query for specific columns in INSERT and UPDATE.

```go
db.MustCols("alert_at", "category_id").InsertOne(ctx, &budget)
// Includes alert_at and category_id even when zero
```

#### 4. `AllCols` (per-query, all fields)

Include every field regardless of zero value. Use with caution.

```go
db.AllCols().InsertOne(ctx, &budget)
// Includes all fields, including id=0, created_at=zero, etc.
```

## Engine API

All database engines implement the `sql.Engine` interface. Methods are chainable.

### Query Building

| Method                              | Description                                |
|-------------------------------------|--------------------------------------------|
| `Table(name string)`                | Set target table name                      |
| `ID(id any)`                        | Filter by primary key                      |
| `Where(cond string, args ...any)`   | Add raw WHERE condition with `?` placeholders |
| `In(col string, values ...any)`     | Add `col IN (...)` filter                  |
| `Columns(cols ...string)`           | Select specific columns (default: `*`)     |
| `OrderBy(col, dir)`                 | Sort results (`ASC` or `DESC`)             |
| `Paginate(page, perPage)`           | Automatic `LIMIT` and `OFFSET`             |
| `Join(table, on)`                   | Add `JOIN` (also `LeftJoin`, `InnerJoin`)  |
| `GroupBy(cols...)`                  | Add `GROUP BY` clause                      |
| `Having(cond, args...)`             | Add `HAVING` clause for groups             |
| `Distinct()`                        | Enable `SELECT DISTINCT`                   |
| `Select(aggs ...Aggregate)`         | Add aggregate columns (`Count/Sum/Avg/Min/Max`) |
| `Count(ctx)`                        | Return matching row count as `int64`       |
| `With(name, sub)`                   | Add a `WITH` CTE from a sub-Engine         |
| `Preload(assoc)`                    | Eager-load an association (no N+1)          |

### Features

#### Aggregates
Build aggregate columns with the `sql.Count/Sum/Avg/Min/Max` expression helpers
and `Select`, then scan them into a struct. Use `.As(alias)` to name a column:
```go
import isql "github.com/masudur-rahman/styx/sql"

db.Table("sale").
    Columns("product").
    Select(isql.Sum("amount").As("total")).
    GroupBy("product").
    FindMany(ctx, &totals)
// SELECT product, SUM(amount) as total FROM "sale" GROUP BY product
// Supported: Count, Sum, Avg, Min, Max
```

#### Count
Get a row count directly as an `int64`. Soft-deleted rows are excluded by default
(the table's soft-delete column is registered at `Sync` time); `WithDeleted`
includes them:
```go
n, err := db.Table("user").Count(ctx)
adults, err := db.Table("user").Where("age >= ?", 18).Count(ctx)
all, err := db.Table("user").WithDeleted().Count(ctx)
```

#### Joins
Join related tables and project their columns. Alias joined columns as
`"prefix.column"` to hydrate a nested struct field named by the prefix:

```go
type PostWithAuthor struct {
    Title  string  `db:"title"`
    Author Account `db:"author"` // filled from columns aliased "author.*"
}

db.Table("post").
    Columns(`post.title AS title`, `account.name AS "author.name"`).
    Join("account", "account.id = post.author_id").
    FindMany(ctx, &rows)
```

#### CTEs (`WITH`)
Compile any Engine chain into a named Common Table Expression with `With`. The
sub-Engine is turned into a subquery without executing; the CTE name is then
referenced as a table via `Table`/`Join`. Placeholders and args are spliced in
automatically for each dialect.

```go
spenders := db.Table("order").Columns("user_id").
    GroupBy("user_id").Having("SUM(total) > ?", 1000)

db.With("big_spenders", spenders).
    Table("account").
    Join("big_spenders", "account.id = big_spenders.user_id").
    FindMany(ctx, &accounts)
```

#### Relationships & Preload
Declare associations with db-tag options on struct fields, then eager-load them
with `Preload`. Each preload runs a single batched query (no N+1). Relation
fields are ignored by DDL, INSERT, and UPDATE.

```go
type Author struct {
    ID    int64  `db:"id,pk autoincr"`
    Name  string `db:"name"`
    Books []Book `db:"-,o2m fk:author_id"`
}
type Book struct {
    ID       int64   `db:"id,pk autoincr"`
    AuthorID int64   `db:"author_id"`
    Author   *Author `db:"-,m2o fk:author_id"`
    Tags     []Tag   `db:"-,m2m join:book_tags fk:book_id ref:tag_id"`
}

db.Table("author").Preload("Books").ID(1).FindOne(ctx, &author)
db.Table("book").Preload("Author").Preload("Tags").FindMany(ctx, &books)
```

| Option | Meaning |
|--------|---------|
| `o2m` + `fk:<col>`      | foreign key `<col>` lives on the child table |
| `m2o` + `fk:<col>`    | foreign key `<col>` lives on this table |
| `m2m` + `join:<table> fk:<col> ref:<col>` | linked through a join table |

#### Soft Delete
Declaratively enable soft deletes using struct tags:
```go
type User struct {
    ID        int64      `db:"id,pk"`
    DeletedAt *time.Time `db:"deleted_at,archive"`
}

db.DeleteOne(ctx, User{ID: 1})    // Sets deleted_at = CURRENT_TIMESTAMP
db.FindMany(ctx, &users)          // Filters out rows where deleted_at IS NOT NULL
db.WithDeleted().FindMany(ctx, &users) // Includes deleted rows
db.Restore(ctx, User{ID: 1})      // Clears deleted_at
```

#### Struct Validation
Integrate validation rules into your models with the `validate` tag. Rules run on
`InsertOne`, `InsertMany`, and `UpdateOne` when `EnableValidation(true)` is set,
and return a typed `*dberr.ValidationError`.

```go
type Signup struct {
    Email    string `db:"email"    validate:"required,email"`
    Age      int    `db:"age"      validate:"gt:0,lt:150"`
    Role     string `db:"role"     validate:"oneof:admin user"`
    Password string `db:"password" validate:"min:8"`
}

db.EnableValidation(true).InsertOne(ctx, &user) // returns error if validation fails
```

Built-in rules:

| Rule | Param | Applies to | Description |
|------|-------|-----------|-------------|
| `required`   | —                | any                        | Not the zero value; non-blank (trimmed) for strings |
| `min:<n>`    | int              | string, int                | String length ≥ n, or integer value ≥ n |
| `max:<n>`    | int              | string, int                | String length ≤ n, or integer value ≤ n |
| `len:<n>`    | int              | string, slice, map, array  | Exact length n |
| `gt:<n>`     | number           | int/uint/float             | Value greater than n |
| `lt:<n>`     | number           | int/uint/float             | Value less than n |
| `oneof:<…>`  | space-separated  | any                        | Value must equal one of the listed options |
| `email`      | —                | string                     | Valid email address |
| `url`        | —                | string                     | Valid URL (scheme + host) |
| `numeric`    | —                | string                     | Digits, `.`, and `-` only |
| `alpha`      | —                | string                     | Letters only |

Rules are comma-separated in the tag (`validate:"required,min:3"`), so `oneof`
options are **space**-separated (`oneof:admin user`), not comma-separated.
`email`, `url`, `numeric`, and `alpha` skip empty strings — combine with
`required` to forbid empties.

For custom or cross-field checks, implement `Validate() error` on the model — it
runs after the tag rules pass.

```go
func (s *Signup) Validate() error {
    if s.Password != s.Confirm {
        return errors.New("password confirmation does not match")
    }
    return nil
}
```

### Zero-Value Control

| Method                              | Description                                |
|-------------------------------------|--------------------------------------------|
| `AllCols()`                         | Include all fields (INSERT/UPDATE/WHERE)   |
| `MustCols(cols ...string)`          | Force specific columns in INSERT/UPDATE    |
| `MustFilterCols(cols ...string)`    | Force specific columns in WHERE clauses    |

### CRUD Operations

All operations take a `context.Context` as the first argument.

| Method                                                    | Description                          |
|-----------------------------------------------------------|--------------------------------------|
| `FindOne(ctx, doc any, filter ...any) (bool, error)`      | Find one record. Returns false if not found. |
| `FindMany(ctx, docs any, filter ...any) error`            | Find multiple records into a slice   |
| `InsertOne(ctx, doc any) (id any, err error)`             | Insert one record. Returns inserted ID. |
| `InsertMany(ctx, docs []any) ([]any, error)`              | Bulk insert as a single multi-row `INSERT`; returns generated IDs |
| `UpdateOne(ctx, doc any) error`                           | Update one record (requires WHERE)   |
| `DeleteOne(ctx, filter ...any) error`                     | Delete one record (requires WHERE)   |

#### Bulk Insert

`InsertMany` builds one multi-row `INSERT ... VALUES (..), (..)` statement and
assigns the generated primary keys back onto each document, in order. A column is
included when it is forced, `req`, or non-zero in any of the documents.

```go
ids, _ := db.Table("account").InsertMany(ctx, []any{
    &Account{Name: "alice"}, &Account{Name: "bob"},
})
// ids == [1 2]; alice.ID and bob.ID are set
```

#### Lifecycle Hooks

Implement any subset of the hook interfaces on your model; Styx invokes them
around the matching operation. A non-nil error from a `Before*` hook aborts it.

```go
func (a *AuditRecord) BeforeCreate(ctx context.Context) error { /* set defaults */ return nil }
func (a *AuditRecord) AfterFind(ctx context.Context) error    { /* post-process */ return nil }
```

Available: `BeforeCreate` / `AfterCreate`, `BeforeUpdate` / `AfterUpdate`,
`BeforeDelete` / `AfterDelete`, `AfterFind`.

### Transactions

`BeginTx` returns a transaction-scoped engine; call `Commit` or `Rollback` to end it.

```go
tx, err := db.BeginTx(ctx)
tx.Table("user").InsertOne(ctx, &user)
tx.Commit()   // or tx.Rollback()
```

### Schema Migration

```go
db.Sync(ctx, User{}, Budget{}, Wallet{})
```

`Sync` is **additive and idempotent**: it creates missing tables, adds missing
columns, and creates `idx` / `uidx` indexes. It never drops, renames, or
retypes columns.

For **destructive or order-sensitive changes**, use the versioned migration
runner in [`migrate`](migrate/). Migrations are plain Go functions with an `Up`
and a `Down`, applied in version order and recorded in a `schema_migrations`
table so each runs once; each runs in its own transaction.

```go
m := migrate.New(db).Register(
    migrate.Migration{
        Version: 1, Name: "drop_legacy_column",
        Up:   func(ctx context.Context, e sql.Engine) error {
            _, err := e.Exec(ctx, `ALTER TABLE users DROP COLUMN legacy`)
            return err
        },
        Down: func(ctx context.Context, e sql.Engine) error {
            _, err := e.Exec(ctx, `ALTER TABLE users ADD COLUMN legacy TEXT`)
            return err
        },
    },
)

m.Up(ctx)        // apply all pending
m.Down(ctx)      // reverse the latest
m.Status(ctx)    // per-migration applied state
```

See the [Migrations guide](docs/migrations.md) for the full strategy.

### Connection Pooling

The `*sql.DB` you pass to `NewSQLite`/`NewPostgres` **is** the pool. Tune it with
`PoolConfig` (only non-zero fields are applied) and read live usage via `Stats()`.

```go
isql.PoolConfig{MaxOpenConns: 25, MaxIdleConns: 5, ConnMaxLifetime: time.Hour}.Apply(conn)
db := sqlite.NewSQLite(conn)
stats := db.Stats() // sql.DBStats
```

`GetSQLiteConnection`/`GetPostgresConnection` also accept an optional `PoolConfig`.

### Observability

Pass an `Observer` at construction to receive a callback for every executed
statement — the integration point for logging, metrics, and tracing (e.g. an
OpenTelemetry adapter) with no external dependency.

```go
db := sqlite.NewSQLite(conn, isql.WithObserver(myObserver))
// myObserver.OnQuery(ctx, query, args, dur, err) fires per statement
```

### Prepared-Statement Caching

Opt in with `WithStmtCache` to memoise prepared statements by SQL text on the
non-transaction path; recurring queries reuse the cached statement.

```go
db := sqlite.NewSQLite(conn, isql.WithStmtCache())
```

Options compose: `sqlite.NewSQLite(conn, isql.WithObserver(o), isql.WithStmtCache())`.

### Raw Queries

```go
rows, err := db.Query(ctx, "SELECT * FROM user WHERE name = ?", "masud")
result, err := db.Exec(ctx, "DELETE FROM user WHERE id = ?", 1)
```
## Unit of Work

Styx provides a Unit of Work pattern to coordinate transactions across multiple database engines (SQL + NoSQL). See [Unit of Work Documentation](docs/unit_of_work.md) for more details.

```go
uow := styx.UnitOfWork{SQL: sqlEngine, NoSQL: nosqlEngine}

tx, err := uow.Begin(ctx)
if err != nil {
	return err
}
if _, err = tx.SQL.Table("user").InsertOne(ctx, &user); err != nil {
	_ = tx.Rollback()
	return err
}
return tx.Commit()
```

## Project Structure

```
sql/            SQL Engine interface + implementations
  sqlite/       SQLite (via modernc.org/sqlite, pure Go)
  postgres/     PostgreSQL (direct + gRPC remote access)
  supabase/     Supabase REST-based
  mock/         Mock SQL engine (GoMock)
nosql/          NoSQL Engine interface + implementations
  arango/       ArangoDB
  mongo/        MongoDB
  mock/         Mock NoSQL engine
dberr/          Shared error types (DataNotFound, RequirementMissing)
uow.go          Unit of Work coordinator
```

---

### Why the name Styx?

In Greek mythology, the River Styx separates the world of the living from the world of the dead.
Similarly, this ORM acts as a bridge between your application code and the database,
facilitating the flow of data between the two realms while ensuring data integrity and controlled access.
