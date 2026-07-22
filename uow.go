package styx

import (
	"context"
	"log"

	"github.com/masudur-rahman/styx/dberr"
	"github.com/masudur-rahman/styx/nosql"
	"github.com/masudur-rahman/styx/sql"
)

// UnitOfWork coordinates work across a SQL and a NoSQL engine.
//
// Transactional guarantees apply to the SQL engine only. The NoSQL engine is
// NOT transactional: Begin/Commit/Rollback are no-ops for it, so NoSQL writes
// are durable immediately and are NOT undone when the SQL transaction is rolled
// back. Do not rely on cross-engine atomicity. For consistency across engines,
// order writes so the NoSQL write is idempotent/compensatable, or apply a saga
// pattern in application code.
type UnitOfWork struct {
	SQL   sql.Engine
	NoSQL nosql.Engine
}

// Begin starts a SQL transaction and returns a UnitOfWork whose SQL field is the
// transaction-scoped engine. The NoSQL engine is passed through unchanged; it
// does not begin a transaction.
func (uow UnitOfWork) Begin(ctx context.Context) (UnitOfWork, error) {
	cp := UnitOfWork{
		SQL:   uow.SQL,
		NoSQL: uow.NoSQL,
	}
	if uow.SQL != nil {
		sqlTx, err := uow.SQL.BeginTx(ctx)
		if err != nil {
			return UnitOfWork{}, err
		}
		cp.SQL = sqlTx
	}
	// NoSQL engines are not transactional; nothing to begin.
	return cp, nil
}

// Commit commits the SQL transaction. NoSQL writes were already durable.
func (uow UnitOfWork) Commit() error {
	if uow.SQL != nil {
		if err := uow.SQL.Commit(); err != nil {
			return err
		}
	}
	// NoSQL engines are not transactional; nothing to commit.
	return nil
}

// Rollback aborts the SQL transaction. Any NoSQL writes made during the unit of
// work are NOT rolled back; when a NoSQL engine is present this logs a warning
// so the caller is not misled about cross-engine atomicity. See
// dberr.ErrNoSQLNotTransactional.
func (uow UnitOfWork) Rollback() error {
	var err error
	if uow.SQL != nil {
		err = uow.SQL.Rollback()
	}
	if uow.NoSQL != nil {
		log.Printf("styx: UnitOfWork rolled back SQL transaction; NoSQL writes are not rolled back (%v)",
			dberr.ErrNoSQLNotTransactional)
	}
	return err
}
