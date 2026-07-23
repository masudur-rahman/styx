# Schema Migrations

Styx has two distinct concerns: **auto-sync** (safe, additive, built in) and
**destructive schema change** (unsafe, not built in — use a versioned migration
tool). This document explains the boundary and the recommended workflow.

## Auto-sync (built in)

`db.Sync(ctx, Models...)` reconciles the database with your structs. It is
**additive and idempotent** — safe to run on every startup:

- Creates tables that do not exist (`CREATE TABLE IF NOT EXISTS`).
- Adds columns that are present in the struct but missing in the table
  (`ALTER TABLE ... ADD COLUMN`).
- Creates indexes declared with the `idx` / `uidx` tags
  (`CREATE INDEX IF NOT EXISTS`).

```go
db.Sync(ctx, User{}, Book{}, Tag{})
```

Auto-sync intentionally does **not**:

- drop columns or tables,
- change a column's type,
- rename a column,
- add or remove constraints (NOT NULL, UNIQUE, FK) on existing columns.

These are destructive, order-sensitive, and often need data backfill, so they do
not belong in reflection-driven runtime sync.

## Destructive changes (recommended: versioned migrations)

For anything auto-sync will not do, use a **versioned migration system** rather
than trying to express the change through structs. This is the same model used
by Flyway, Alembic, and golang-migrate.

Principles:

1. **Ordered, reversible migration files.** One file per change, each with an
   `Up` and a `Down` (Go functions or `.sql` files), named with a monotonic
   version prefix (`0007_drop_legacy_email.sql`).
2. **A `schema_migrations` version table** recording which versions have been
   applied, so migrations run once and in order.
3. **Explicit commands**, not startup magic: `migrate-up`, `migrate-down`,
   `migrate-status` (wire these into the `Makefile`).
4. **Auto-sync stays additive.** Keep using `Sync` for new tables/columns/indexes
   in development; route every destructive change through a reviewed migration.

### Expand → migrate → contract

For an in-place change that cannot be done atomically (rename, type change, split
a column), avoid data loss by spreading it across migrations and deploys:

1. **Expand** — add the new column (auto-sync or a migration); deploy code that
   writes both old and new.
2. **Backfill** — copy/transform existing rows into the new column in a migration.
3. **Switch** — change reads to the new column; stop writing the old one.
4. **Contract** — in a *later* migration, drop the old column once nothing reads
   it.

Each step is independently deployable and reversible, so a bad step can be rolled
back without losing data.

## Summary

| Change                                   | How |
|------------------------------------------|-----|
| New table / column / index               | `db.Sync(ctx, ...)` |
| Drop column or table                     | versioned migration |
| Change column type                       | expand → migrate → contract |
| Rename column                            | expand → migrate → contract |
| Add/remove constraint on existing column | versioned migration |

> A built-in migration engine is not part of Styx today. Until it is, pair
> auto-sync with an external versioned migration tool for destructive changes.
