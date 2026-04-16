package sql

import (
	"context"
	"database/sql"
)

type Engine interface {
	BeginTx(ctx context.Context) (Engine, error)
	Commit() error
	Rollback() error

	Table(name string) Engine

	ID(id any) Engine
	In(col string, values ...any) Engine
	Where(cond string, args ...any) Engine
	Columns(cols ...string) Engine
	AllCols() Engine
	MustCols(cols ...string) Engine
	MustFilterCols(cols ...string) Engine
	ShowSQL(showSQL bool) Engine

	FindOne(ctx context.Context, document any, filter ...any) (bool, error)
	FindMany(ctx context.Context, documents any, filter ...any) error

	InsertOne(ctx context.Context, document any) (id any, err error)
	InsertMany(ctx context.Context, documents []any) ([]any, error)

	UpdateOne(ctx context.Context, document any) error

	DeleteOne(ctx context.Context, filter ...any) error

	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)

	Sync(ctx context.Context, tables ...any) error

	Close() error
}
