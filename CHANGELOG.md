# Changelog

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
