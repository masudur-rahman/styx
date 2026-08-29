# Changelog

## Unreleased

### ⚠️ Breaking Changes
- **`UpdateOne`, `DeleteOne` and `Restore` now affect one row.** They previously
  emitted an uncapped `WHERE`, so `db.Where("status=?", "active").UpdateOne(...)`
  updated *every* matching row. Call `UpdateMany`/`DeleteMany` where that was the
  intent. Statements narrowed by `ID()` are unchanged.
- **`sql.Engine` gained `UpdateMany` and `DeleteMany`.** Implementations outside
  this repository must add both.

### 🚀 New Features
- `UpdateMany(ctx, doc) (int64, error)` and `DeleteMany(ctx, filter...) (int64, error)`
  update or delete every matching row and return the count. Matching nothing
  returns `(0, nil)` rather than `ErrNotFound`.
- `core.DeclaredPKColumn` reports the column a `pk` tag names, separating it from
  `GetPKColumn`'s `"id"` fallback for callers that put the column into SQL.

### 🐛 Bug Fixes
- **SQLite used the Postgres dialect.** `NewSQLite` never initialised its
  statement, so every SQLite query fell through to the Postgres default and
  emitted `$1` placeholders — which SQLite happens to accept, hiding the bug.
- **Soft delete was skipped when no filter document was given.** The delete path
  only learned the `archive` column from a filter, so `db.Table("user").ID(1).
  DeleteOne(ctx)` removed the row outright from a soft-delete table. It now falls
  back to the column `Sync` registered, as reads already did. `Restore` and
  `Count` by ID are fixed the same way; `ForceDelete` still deletes for real.
- **Soft delete and restore now skip rows already on the target side.** With the
  single-row cap ordering by primary key, a filter matching one deleted row and
  two live ones re-stamped the deleted one and reported success. A delete that
  finds nothing live now returns `ErrNotFound`, and `DeleteMany` no longer counts
  rows it had already deleted.

## v1.4.0 (2026-04-17)

This release transforms Styx into a robust, production-ready database engine with a fluent query builder, advanced data integrity features, and significant performance optimizations.

### 🚀 New Features
- **Context-Aware API**: Full propagation of `context.Context` across all `Engine` operations.
- **Fluent Query Builder**:
    - Added `Join`, `LeftJoin`, `InnerJoin` for complex relational queries.
    - Built-in `Paginate(page, perPage)` support with automatic limit/offset calculation.
    - Expanded selection methods: `OrderBy`, `GroupBy`, `Having`, `Distinct`, `Limit`, `Offset`.
    - Integrated Aggregates: `Count`, `Sum`, `Avg`, `Min`, `Max`.
- **Data Integrity**:
    - **Soft Delete**: Declarative support via `archive` struct tag; automatic filtering with `WithDeleted()` override.
    - **Struct Validation**: Opt-in validation hook using `EnableValidation(true)`.
- **Performance Optimization**:
    - **Reflection Caching**: Introduced a thread-safe cache for struct metadata, significantly reducing overhead on every query.
    - **Pointer Receivers**: Optimized statement building to minimize memory copies during method chaining.
- **Enhanced Schema Sync**: Support for index creation from struct tags and `DropTable` operations.

### 🛡️ Security & Reliability
- **Parameterized Queries**: Switched to full parameterization for all INSERT, UPDATE, and WHERE clauses.
- **Postgres Placeholder Fix**: Fixed a critical bug in placeholder renumbering that affected complex updates.
- **Improved Type Scanning**: Enhanced `ScanRow` with automatic `time.Time` parsing and pointer field support.

### 🐛 Bug Fixes
- Fixed SQL injection vulnerabilities in raw query generation.
- Corrected `IsValidationError` to properly handle wrapped errors.
- Fixed zero-value skipping bugs with the `req` tag.

### 🧪 Testing
- Added comprehensive **Integration Test Suite** verifying all major features (JOINs, Pagination, Soft Delete, etc.) against a live database.

---
*For a full list of changes, see the [commit history](https://github.com/masudur-rahman/styx/compare/v1.3.0...v1.4.0).*
