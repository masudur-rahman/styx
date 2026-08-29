# Styx ORM: Zero-Value Field Handling Overhaul

## Problem

Styx silently drops zero-value fields (`""`, `0`, `false`) in **struct-based filters** (WHERE clauses). This is a correctness bug — not just an inconvenience.

### Root Cause

`GenerateWhereClauseFromFilter()` in both `sql/sqlite/lib/sqlite.go:116` and `sql/postgres/lib/pg.go:131`:

```go
if val.Field(idx).IsZero() {
    continue  // silently drops the condition
}
```

There is **no opt-in mechanism** (no `MustCols` equivalent) for WHERE clauses. The `MustCols`/`AllCols` mechanism only exists for INSERT and UPDATE queries.

### Real-World Impact (discovered in expense-tracker-bot)

```go
// Budget model: CategoryID="" means "overall budget" (valid, intentional)
type Budget struct {
    UserID     int64
    CategoryID string  // "" = overall, "food" = food category
}

// BUG: This silently becomes WHERE user_id=99 (no category_id filter!)
db.FindOne(&b, Budget{UserID: 99, CategoryID: ""})

// Returns the FIRST budget for user 99 (e.g. "food"), not the overall budget
// Same bug in DeleteOne — deletes the wrong row
```

**Workaround required:** explicit `.Where("category_id = ?", "")` for every query involving potentially-zero fields.

---

## Affected Operations

| Operation | Zero-value behavior | Has MustCols? | Fix needed? |
|-----------|-------------------|---------------|-------------|
| `FindOne(filter)` | Drops condition | No | Yes |
| `FindMany(filter)` | Drops condition | No | Yes |
| `DeleteOne(filter)` | Drops condition | No | Yes |
| `InsertOne(doc)` | Drops column (NULL) | Yes (`MustCols`/`AllCols`) | Partial — see below |
| `UpdateOne(doc)` | Drops column | Yes (`MustCols`/`AllCols`) | Partial — see below |

---

## Proposed Fix

### Option A: `MustFilterCols` (targeted, backward-compatible)

Add a `MustFilterCols(cols ...string)` method to the Engine interface — mirrors `MustCols` but applies to WHERE generation from struct filters.

```go
// Engine interface addition
MustFilterCols(cols ...string) Engine

// Usage
db.MustFilterCols("category_id").FindOne(&b, Budget{UserID: 99, CategoryID: ""})
// Generates: WHERE user_id=99 AND category_id=''
```

**Implementation:**

1. Add `mustFilterCols []string` and `mustFilterColMap map[string]bool` to `Statement` struct
2. Add `MustFilterCols(cols ...string) Statement` method
3. Modify `GenerateWhereClauseFromFilter()` to check `mustFilterColMap[col]` before skipping:

```go
func (stmt Statement) GenerateWhereClauseFromFilter(filter any) string {
    var conditions []string
    val := reflect.ValueOf(filter)
    for idx := 0; idx < val.NumField(); idx++ {
        field := val.Type().Field(idx)
        col := getFieldName(field)

        // Changed: respect mustFilterColMap (same pattern as INSERT/UPDATE)
        if !(stmt.allCols || stmt.mustFilterColMap[col] || !val.Field(idx).IsZero()) {
            continue
        }

        value := formatValues(val.Field(idx).Interface())
        condition := strings.Join([]string{col, value}, "=")
        conditions = append(conditions, condition)
    }
    return strings.Join(conditions, " AND ")
}
```

4. Thread `mustFilterColMap` through `GenerateWhereClause()` → `FindOne`/`FindMany`/`DeleteOne`

**Pros:** Zero breaking changes. Consistent API with existing `MustCols`.
**Cons:** Callers still need to remember to opt in for every zero-value filter.

---

### Option B: Struct tag `db:"...,filter"` (declarative, zero-effort per query)

Add a `filter` tag option that marks a field as "always include in WHERE even when zero."

```go
type Budget struct {
    UserID     int64  `db:"user_id"`
    CategoryID string `db:"category_id,uqs,filter"` // always include in WHERE
}

// No extra API needed:
db.FindOne(&b, Budget{UserID: 99, CategoryID: ""})
// Generates: WHERE user_id=99 AND category_id=''
```

**Implementation:**

1. Parse `filter` tag in `getFieldName()` / new helper `getFieldOptions()`
2. In `GenerateWhereClauseFromFilter()`, check tag before skipping:

```go
if val.Field(idx).IsZero() && !hasFilterTag(field) {
    continue
}
```

**Pros:** Declare once, works everywhere. No per-query ceremony.
**Cons:** Requires model changes. Tag only meaningful for WHERE, not INSERT/UPDATE (those already have `MustCols`).

---

### Option C: `AllFilterCols()` (brute force, simple)

Like `AllCols()` but for WHERE — include all struct fields in the filter regardless of zero value.

```go
db.AllFilterCols().FindOne(&b, Budget{UserID: 99, CategoryID: ""})
```

**Pros:** Trivial to implement (single boolean flag).
**Cons:** Too broad — includes *every* zero field (ID=0, CreatedAt=0, etc.), producing overly restrictive WHERE clauses.

---

### Recommendation: Option A + Option B together

- **Option A (`MustFilterCols`)** for backward-compatible immediate fix — callers opt in per query
- **Option B (`filter` tag)** for ergonomic long-term solution — models declare intent once

Both are small, independent changes. Ship A first, then B.

---

## Implementation Plan

### Phase 1: `MustFilterCols` (Option A)

**Files to modify (both SQLite and PostgreSQL libs are nearly identical — change both):**

| File | Change |
|------|--------|
| `sql/database.go` | Add `MustFilterCols(cols ...string) Engine` to interface |
| `sql/sqlite/lib/statement.go` | Add `mustFilterCols` field, `MustFilterCols()` method, `generateMustFilterColMap()` |
| `sql/sqlite/lib/sqlite.go` | Change `GenerateWhereClauseFromFilter` to accept `mustFilterColMap` or move it to Statement method |
| `sql/sqlite/sqlite.go` | Wire `MustFilterCols` on the SQLite Engine impl |
| `sql/postgres/lib/statement.go` | Same as sqlite statement.go |
| `sql/postgres/lib/pg.go` | Same as sqlite sqlite.go |
| `sql/postgres/postgres.go` | Wire `MustFilterCols` on the PostgreSQL Engine impl |
| `sql/mock/mock.go` | Add `MustFilterCols` to mock Engine |
| `sql/supabase/supabase.go` | Add `MustFilterCols` if supabase Engine impl exists |

**Current problem with `GenerateWhereClauseFromFilter`:**
It's a **package-level function** (`func GenerateWhereClauseFromFilter(filter any) string`), not a method on `Statement`. It has no access to statement-level config like `mustFilterColMap`.

**Fix:** Either:
1. Convert to a `Statement` method (breaking change within lib — but lib is internal), or
2. Pass `mustFilterColMap` as a parameter

Option 1 is cleaner. `GenerateWhereClauseFromFilter` is only called from `Statement.GenerateWhereClause()` (line 60-66), so making it a method is safe:

```go
// Before (package function)
func GenerateWhereClauseFromFilter(filter any) string { ... }

// After (Statement method)
func (stmt Statement) GenerateWhereClauseFromFilter(filter any) string {
    stmt.mustFilterColMap = stmt.generateMustFilterColMap()
    // ... use stmt.mustFilterColMap in the loop
}
```

**Tests to add:**

| Test | What |
|------|------|
| `TestGenerateWhereClause_skipsZeroValues` | Existing behavior preserved (no MustFilterCols) |
| `TestGenerateWhereClause_mustFilterColsIncludesZeroValues` | `MustFilterCols("category_id")` includes empty string |
| `TestFindOne_mustFilterCols_emptyString` | Integration: finds correct row when filtering by `""` |
| `TestDeleteOne_mustFilterCols_emptyString` | Integration: deletes correct row when filtering by `""` |

### Phase 2: `filter` struct tag (Option B)

**Files to modify:**

| File | Change |
|------|--------|
| `sql/sqlite/lib/table_sync.go` | Parse `filter` from `db` tag in new `getFieldOptions()` helper |
| `sql/sqlite/lib/sqlite.go` | In `GenerateWhereClauseFromFilter`, check `filter` tag before `IsZero()` skip |
| `sql/postgres/lib/table_sync.go` | Same |
| `sql/postgres/lib/pg.go` | Same |

**Tag parsing:**
```go
// Current tag format: db:"column_name,pk autoincr uq uqs"
// New: db:"column_name,pk autoincr uq uqs filter"

func hasFilterTag(field reflect.StructField) bool {
    dbTag := field.Tag.Get("db")
    if dbTag == "" {
        return false
    }
    parts := strings.SplitN(dbTag, ",", 2)
    if len(parts) < 2 {
        return false
    }
    return strings.Contains(parts[1], "filter")
}
```

**Tests to add:**

| Test | What |
|------|------|
| `TestGenerateWhereClause_filterTag_includesZeroString` | Struct with `filter` tag includes `""` in WHERE |
| `TestGenerateWhereClause_filterTag_includesZeroInt` | Struct with `filter` tag includes `0` in WHERE |
| `TestGenerateWhereClause_noFilterTag_skipsZero` | Without tag, existing behavior preserved |

### Phase 3: Also consider for INSERT/UPDATE (bonus)

The existing `MustCols` for INSERT/UPDATE has the same usability problem — callers must remember to call it every time. The `filter` tag could be extended to also affect INSERT/UPDATE:

```go
// New tag: db:"column_name,mustcol" or db:"column_name,noskip"
// Always include in INSERT/UPDATE even when zero-value

type Budget struct {
    CategoryID string `db:"category_id,uqs,noskip"` // never skip, even if ""
    AlertAt    int64  `db:"alert_at,noskip"`         // never skip, even if 0
}
```

This would eliminate `MustCols("alert_at", "category_id")` from every `InsertOne` call site. The tag name `noskip` (or `required` or `always`) makes intent clear across all operations.

---

## Migration Path for expense-tracker-bot

Once styx is fixed, the workarounds can be removed:

```go
// Before (current workaround)
db.Where("category_id = ?", categoryID).FindOne(&b, Budget{UserID: userID})
db.Where("category_id = ?", categoryID).DeleteOne(Budget{UserID: userID})
db.MustCols("alert_at", "category_id").InsertOne(budget)

// After Option A
db.MustFilterCols("category_id").FindOne(&b, Budget{UserID: userID, CategoryID: categoryID})
db.MustFilterCols("category_id").DeleteOne(Budget{UserID: userID, CategoryID: categoryID})
db.MustCols("alert_at", "category_id").InsertOne(budget)  // unchanged

// After Option B (with tag)
// Model: CategoryID string `db:"category_id,uqs,noskip"`
// AlertAt int64 `db:"alert_at,noskip"`
db.FindOne(&b, Budget{UserID: userID, CategoryID: categoryID})   // just works
db.DeleteOne(Budget{UserID: userID, CategoryID: categoryID})      // just works
db.InsertOne(budget)                                               // just works — no MustCols needed
```

---

## Testing Checklist

- [ ] All existing tests still pass (no regression)
- [ ] New unit tests for `MustFilterCols` (WHERE generation)
- [ ] New unit tests for `filter`/`noskip` tag (WHERE + INSERT + UPDATE)
- [ ] Integration tests with in-memory SQLite: FindOne/DeleteOne with zero-value string filter
- [ ] Integration tests with PostgreSQL (if CI has PG)
- [ ] expense-tracker-bot budgets work without `.Where()` workaround after upgrade

----------

## From GEMINI

1. Named Tag for Operations: In Phase 3, the plan suggests a noskip tag. I would recommend naming it db:"column,required" or db:"column,always". This makes it clear to the developer that this field is a
   core part of the record identity and should never be omitted, regardless of its value.
2. Explicit Conflict with omitempty: Many Go developers use json:",omitempty". The planning should explicitly mention that the db tag's filter or noskip behavior takes precedence over any other logic to
   avoid confusion when a field is skipped in JSON but required in SQL.
3. The "Optimistic Locking" Edge Case: In my audit of your wallets.go, we use a version field. The styx plan should ensure that the version field (often 0 on the very first insert) is also protected by
   these new tags so the first record creation doesn't fail if version is treated as a "skippable" zero.