# Unit of Work

## Overview

`UnitOfWork` (defined in `uow.go`) coordinates a SQL engine and an optional NoSQL engine under a single logical transaction boundary. It is the top-level object that service layers use when a sequence of database writes must succeed or fail atomically.

```
UnitOfWork
├── SQL   sql.Engine     — wraps PostgreSQL, SQLite, or Supabase
└── NoSQL nosql.Engine   — wraps ArangoDB, MongoDB (optional)
```

## Lifecycle

```
Begin → (write operations) → Commit
                           ↘ Rollback (on error)
```

`Begin` starts a SQL transaction via `sql.Engine.BeginTx` and returns a new `UnitOfWork` whose `SQL` field is the transaction-scoped engine. The original `UnitOfWork` is unchanged.

`Commit` calls `sql.Engine.Commit` on the transaction engine.

`Rollback` calls `sql.Engine.Rollback`. Safe to call even after `Commit` (returns `ErrTransactionNotStarted` which can be ignored).

NoSQL engines (ArangoDB, MongoDB) do not participate in transactions today — `Begin`/`Commit`/`Rollback` are no-ops on the NoSQL side.

## Usage

### Basic setup

```go
import (
    "github.com/masudur-rahman/styx"
    "github.com/masudur-rahman/styx/sql/sqlite"
    sqlitelib "github.com/masudur-rahman/styx/sql/sqlite/lib"
)

func main() {
    conn, err := sqlitelib.GetSQLiteConnection("app.db")
    if err != nil {
        log.Fatal(err)
    }

    uow := styx.UnitOfWork{
        SQL: sqlite.NewSQLite(conn),
    }
}
```

### Transactional service method

```go
func (s *OrderService) PlaceOrder(ctx context.Context, order Order, items []Item) error {
    tx, err := s.uow.Begin(ctx)
    if err != nil {
        return fmt.Errorf("begin tx: %w", err)
    }
    defer func() {
        if err != nil {
            _ = tx.Rollback()
        }
    }()

    // All writes use tx.SQL, which is the transaction-scoped engine.
    if _, err = tx.SQL.Table("orders").InsertOne(ctx, &order); err != nil {
        return fmt.Errorf("insert order: %w", err)
    }
    for i := range items {
        items[i].OrderID = order.ID
        if _, err = tx.SQL.Table("order_items").InsertOne(ctx, &items[i]); err != nil {
            return fmt.Errorf("insert item: %w", err)
        }
    }

    return tx.Commit()
}
```

### Mixed SQL + NoSQL writes

When both engines are present, SQL writes are transactional while NoSQL writes are best-effort. Structure the call so SQL commits before NoSQL to avoid partial state on SQL rollback:

```go
func (s *EventService) Publish(ctx context.Context, event Event) error {
    tx, err := s.uow.Begin(ctx)
    if err != nil {
        return err
    }
    defer func() {
        if err != nil {
            _ = tx.Rollback()
        }
    }()

    if _, err = tx.SQL.Table("events").InsertOne(ctx, &event); err != nil {
        return err
    }
    if err = tx.Commit(); err != nil {
        return err
    }

    // NoSQL write is outside the SQL transaction.
    return s.uow.NoSQL.Collection("event_log").InsertOne(ctx, &event)
}
```

## Error handling

| Error | Meaning |
|---|---|
| `dberr.ErrTransactionNotStarted` | `Commit`/`Rollback` called without a prior `Begin` |
| `dberr.ErrTransactionAlreadyStarted` | `Begin` called while a transaction is active |

## Notes

- `UnitOfWork` is a value type. `Begin` returns a new value; the caller must use the returned value, not the original, for transactional writes.
- The SQL engine inside `UnitOfWork.SQL` after `Begin` is a `*sql.Tx`-backed engine. It does not support nested transactions.
- To use only SQL without NoSQL, leave `NoSQL` nil.
