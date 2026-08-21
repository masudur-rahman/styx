package postgres

import (
	"context"
	"database/sql"

	"github.com/masudur-rahman/styx/v2/dberr"
	isql "github.com/masudur-rahman/styx/v2/sql"
	core "github.com/masudur-rahman/styx/v2/sql/internal/core"
	lib "github.com/masudur-rahman/styx/v2/sql/internal/lib"
	"github.com/masudur-rahman/styx/v2/validation"
)

type Postgres struct {
	conn      *sql.DB
	tx        *sql.Tx
	statement lib.Statement
	observer  isql.Observer
	cache     *core.StmtCache
}

// NewPostgres returns a Postgres engine over conn. Options such as WithObserver
// and WithStmtCache configure cross-cutting behaviour.
func NewPostgres(conn *sql.DB, opts ...isql.Option) Postgres {
	cfg := isql.BuildConfig(opts...)
	pg := Postgres{conn: conn, observer: cfg.Observer}
	if cfg.StmtCache {
		pg.cache = core.NewStmtCache()
	}
	return pg
}

var _ isql.Engine = Postgres{}

func (pg Postgres) BeginTx(ctx context.Context) (isql.Engine, error) {
	if pg.tx != nil {
		return nil, dberr.ErrTransactionAlreadyStarted
	}
	tx, err := pg.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	pg.tx = tx
	return pg, nil
}

func (pg Postgres) Commit() error {
	if pg.tx == nil {
		return dberr.ErrTransactionNotStarted
	}
	err := pg.tx.Commit()
	pg.tx = nil
	return err
}

func (pg Postgres) Rollback() error {
	if pg.tx == nil {
		return dberr.ErrTransactionNotStarted
	}
	err := pg.tx.Rollback()
	pg.tx = nil
	return err
}

func (pg Postgres) Table(name string) isql.Engine {
	pg.statement.Table(name)
	return pg
}

func (pg Postgres) ID(id any) isql.Engine {
	pg.statement.ID(id)
	return pg
}

func (pg Postgres) In(col string, values ...any) isql.Engine {
	pg.statement.In(col, values...)
	return pg
}

func (pg Postgres) Where(cond string, args ...any) isql.Engine {
	pg.statement.Where(cond, args...)
	return pg
}

func (pg Postgres) Columns(cols ...string) isql.Engine {
	pg.statement.Columns(cols...)
	return pg
}

func (pg Postgres) AllCols() isql.Engine {
	pg.statement.AllCols()
	return pg
}

func (pg Postgres) MustCols(cols ...string) isql.Engine {
	pg.statement.MustCols(cols...)
	return pg
}

func (pg Postgres) MustFilterCols(cols ...string) isql.Engine {
	pg.statement.MustFilterCols(cols...)
	return pg
}

func (pg Postgres) ShowSQL(showSQL bool) isql.Engine {
	pg.statement.ShowSQL(showSQL)
	return pg
}

func (pg Postgres) OrderBy(col string, direction ...string) isql.Engine {
	pg.statement.OrderBy(col, direction...)
	return pg
}

func (pg Postgres) Limit(n int64) isql.Engine {
	pg.statement.Limit(n)
	return pg
}

func (pg Postgres) Offset(n int64) isql.Engine {
	pg.statement.Offset(n)
	return pg
}

func (pg Postgres) Distinct() isql.Engine {
	pg.statement.Distinct()
	return pg
}

func (pg Postgres) GroupBy(cols ...string) isql.Engine {
	pg.statement.GroupBy(cols...)
	return pg
}

func (pg Postgres) Having(cond string, args ...any) isql.Engine {
	pg.statement.Having(cond, args...)
	return pg
}

func (pg Postgres) Or(cond string, args ...any) isql.Engine {
	pg.statement.Or(cond, args...)
	return pg
}

func (pg Postgres) Like(col string, pattern string) isql.Engine {
	pg.statement.Like(col, pattern)
	return pg
}

func (pg Postgres) NotLike(col string, pattern string) isql.Engine {
	pg.statement.NotLike(col, pattern)
	return pg
}

func (pg Postgres) Exists(subquery string, args ...any) isql.Engine {
	pg.statement.Exists(subquery, args...)
	return pg
}

func (pg Postgres) NotExists(subquery string, args ...any) isql.Engine {
	pg.statement.NotExists(subquery, args...)
	return pg
}

func (pg Postgres) Select(aggs ...isql.Aggregate) isql.Engine {
	exprs := make([]string, len(aggs))
	for i, a := range aggs {
		exprs[i] = a.Expr()
	}
	pg.statement.Select(exprs...)
	return pg
}

func (pg Postgres) Preload(assoc string) isql.Engine {
	pg.statement.Preload(assoc)
	return pg
}

// preload eager-loads any registered associations onto docs using a clean
// engine (same connection/transaction, fresh statement) for batched queries.
func (pg Postgres) preload(ctx context.Context, docs any) error {
	preloads := pg.statement.Preloads()
	if len(preloads) == 0 {
		return nil
	}
	base := pg
	base.statement = lib.NewStatement(lib.Postgres)
	return core.PreloadRelations(ctx, base, docs, preloads)
}

func (pg Postgres) Paginate(page, perPage int64) isql.Engine {
	pg.statement.Paginate(page, perPage)
	return pg
}

// With registers sub as a named CTE. sub is compiled to a subquery in place;
// a non-Postgres Engine is ignored (returns the receiver unchanged).
func (pg Postgres) With(name string, sub isql.Engine) isql.Engine {
	s, ok := sub.(Postgres)
	if !ok {
		return pg
	}
	subSQL, subArgs := s.buildSubquery()
	pg.statement.With(name, subSQL, subArgs)
	return pg
}

// buildSubquery compiles the current statement into a SELECT string and its
// args without executing, for use as a CTE or subquery body.
func (pg Postgres) buildSubquery() (string, []any) {
	query := pg.statement.GenerateReadQuery(nil)
	return query, pg.statement.Args()
}

func (pg Postgres) Join(table, condition string) isql.Engine {
	pg.statement.Join(table, condition)
	return pg
}

func (pg Postgres) LeftJoin(table, condition string) isql.Engine {
	pg.statement.LeftJoin(table, condition)
	return pg
}

func (pg Postgres) RightJoin(table, condition string) isql.Engine {
	pg.statement.RightJoin(table, condition)
	return pg
}

func (pg Postgres) InnerJoin(table, condition string) isql.Engine {
	pg.statement.InnerJoin(table, condition)
	return pg
}

func (pg Postgres) EnableValidation(enable bool) isql.Engine {
	pg.statement.EnableValidation(enable)
	return pg
}

func (pg Postgres) WithDeleted() isql.Engine {
	pg.statement.WithDeleted()
	return pg
}

// detectSoftDelete sets soft delete column from struct tags if present.
func (pg Postgres) detectSoftDelete(doc any) Postgres {
	if col := core.ExtractSoftDeleteColumn(doc); col != "" {
		pg.statement.SoftDeleteCol(col)
	}
	return pg
}

func (pg Postgres) ForceDelete(ctx context.Context, filter ...any) error {
	pg.statement.SetForceDelete()
	return pg.DeleteOne(ctx, filter...)
}

func (pg Postgres) Restore(ctx context.Context, filter ...any) error {
	if len(filter) > 0 {
		pg = pg.detectSoftDelete(filter[0])
	}
	pg.statement.GenerateWhereClause(filter...)
	if err := pg.statement.CheckWhereClauseNotEmpty(); err != nil {
		return err
	}

	query := pg.statement.GenerateRestoreQuery()
	result, err := pg.statement.ExecuteWriteQuery(ctx, pg.conn, pg.tx, query, pg.observer, pg.cache)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return dberr.ErrNotFound
	}
	return nil
}

func (pg Postgres) FindOne(ctx context.Context, document any, filter ...any) (bool, error) {
	pg = pg.detectSoftDelete(document)
	pg.statement.GenerateWhereClause(filter...)

	if err := pg.statement.CheckWhereClauseNotEmpty(); err != nil {
		return false, err
	}

	query := pg.statement.GenerateReadQuery(document)
	err := pg.statement.ExecuteReadQuery(ctx, pg.conn, pg.tx, query, document, pg.observer, pg.cache)
	if err == nil {
		if err = core.RunAfterFind(ctx, document); err != nil {
			return false, err
		}
		if err = pg.preload(ctx, document); err != nil {
			return false, err
		}
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}

	return false, err
}

func (pg Postgres) FindMany(ctx context.Context, documents any, filter ...any) error {
	pg = pg.detectSoftDelete(documents)
	pg.statement.GenerateWhereClause(filter...)

	query := pg.statement.GenerateReadQuery(documents)
	if err := pg.statement.ExecuteReadQuery(ctx, pg.conn, pg.tx, query, documents, pg.observer, pg.cache); err != nil {
		return err
	}
	if err := core.RunAfterFindResults(ctx, documents); err != nil {
		return err
	}
	return pg.preload(ctx, documents)
}

// Count returns the number of rows in the table set via Table, matching any
// chained conditions. Soft-deleted rows are excluded unless WithDeleted was set.
func (pg Postgres) Count(ctx context.Context) (int64, error) {
	pg.statement.GenerateWhereClause()

	query := pg.statement.GenerateCountQuery()
	return pg.statement.ExecuteCountQuery(ctx, pg.conn, pg.tx, query, pg.observer, pg.cache)
}

func (pg Postgres) InsertOne(ctx context.Context, document any) (id any, err error) {
	if err := core.RunBeforeCreate(ctx, document); err != nil {
		return nil, err
	}
	if pg.statement.ShouldValidate() {
		if err := validation.Validate(document); err != nil {
			return nil, err
		}
	}
	pkCol := core.GetPKColumn(document)
	pg.statement.PKColumn(pkCol)
	query := pg.statement.GenerateInsertQuery(document)
	id, err = pg.statement.ExecuteInsertQuery(ctx, pg.conn, pg.tx, query, pg.observer, pg.cache)
	if err != nil {
		return nil, err
	}
	if err = core.AssignID(document, id); err != nil {
		return nil, err
	}
	if err = core.RunAfterCreate(ctx, document); err != nil {
		return nil, err
	}
	return id, nil
}

func (pg Postgres) InsertMany(ctx context.Context, documents []any) ([]any, error) {
	if len(documents) == 0 {
		return nil, nil
	}
	for _, doc := range documents {
		if err := core.RunBeforeCreate(ctx, doc); err != nil {
			return nil, err
		}
		if pg.statement.ShouldValidate() {
			if err := validation.Validate(doc); err != nil {
				return nil, err
			}
		}
	}

	pkCol := core.GetPKColumn(documents[0])
	pg.statement.PKColumn(pkCol)
	query := pg.statement.GenerateBulkInsertQuery(documents)
	ids, err := pg.statement.ExecuteBulkInsertQuery(ctx, pg.conn, pg.tx, query, pg.observer, pg.cache)
	if err != nil {
		return nil, err
	}

	for i, doc := range documents {
		if i < len(ids) {
			if err := core.AssignID(doc, ids[i]); err != nil {
				return nil, err
			}
		}
		if err := core.RunAfterCreate(ctx, doc); err != nil {
			return nil, err
		}
	}

	return ids, nil
}

func (pg Postgres) UpdateOne(ctx context.Context, document any) error {
	if err := core.RunBeforeUpdate(ctx, document); err != nil {
		return err
	}
	if pg.statement.ShouldValidate() {
		if err := validation.Validate(document); err != nil {
			return err
		}
	}
	pg.statement.GenerateWhereClause()
	if err := pg.statement.CheckWhereClauseNotEmpty(); err != nil {
		return err
	}

	query := pg.statement.GenerateUpdateQuery(document)
	result, err := pg.statement.ExecuteWriteQuery(ctx, pg.conn, pg.tx, query, pg.observer, pg.cache)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return dberr.ErrNotFound
	}
	return core.RunAfterUpdate(ctx, document)
}

func (pg Postgres) DeleteOne(ctx context.Context, filter ...any) error {
	if len(filter) > 0 {
		pg = pg.detectSoftDelete(filter[0])
		if err := core.RunBeforeDelete(ctx, filter[0]); err != nil {
			return err
		}
	}
	pg.statement.GenerateWhereClause(filter...)
	if err := pg.statement.CheckWhereClauseNotEmpty(); err != nil {
		return err
	}

	var query string
	if pg.statement.IsSoftDelete() {
		query = pg.statement.GenerateSoftDeleteQuery()
	} else {
		query = pg.statement.GenerateDeleteQuery()
	}
	result, err := pg.statement.ExecuteWriteQuery(ctx, pg.conn, pg.tx, query, pg.observer, pg.cache)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return dberr.ErrNotFound
	}
	if len(filter) > 0 {
		return core.RunAfterDelete(ctx, filter[0])
	}
	return nil
}

func (pg Postgres) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if pg.tx != nil {
		return pg.tx.QueryContext(ctx, query, args...)
	}
	return pg.conn.QueryContext(ctx, query, args...)
}

func (pg Postgres) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if pg.tx != nil {
		return pg.tx.ExecContext(ctx, query, args...)
	}
	return pg.conn.ExecContext(ctx, query, args...)
}

func (pg Postgres) Sync(ctx context.Context, tables ...any) error {
	for _, table := range tables {
		if err := lib.SyncTable(ctx, lib.Postgres, pg.conn, table); err != nil {
			return err
		}
		core.RegisterSoftDeleteColumn(core.GetTableName(table), core.ExtractSoftDeleteColumn(table))
	}

	return nil
}

func (pg Postgres) DropTable(ctx context.Context, name string) error {
	return lib.DropTable(ctx, pg.conn, name)
}

// Stats returns live statistics for the underlying connection pool.
func (pg Postgres) Stats() sql.DBStats {
	return pg.conn.Stats()
}

func (pg Postgres) Close() error {
	return pg.conn.Close()
}

func (pg Postgres) cleanup() {
	pg.statement = lib.NewStatement(lib.Postgres)
}
