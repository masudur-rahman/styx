package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/masudur-rahman/styx/dberr"
	isql "github.com/masudur-rahman/styx/sql"
	core "github.com/masudur-rahman/styx/sql/internal/core"
	"github.com/masudur-rahman/styx/sql/sqlite/lib"
	"github.com/masudur-rahman/styx/validation"

	_ "modernc.org/sqlite"
)

type SQLite struct {
	conn      *sql.DB
	tx        *sql.Tx
	statement lib.Statement
	observer  isql.Observer
	cache     *core.StmtCache
}

// NewSQLite returns a SQLite engine over conn. Options such as WithObserver and
// WithStmtCache configure cross-cutting behaviour.
func NewSQLite(conn *sql.DB, opts ...isql.Option) SQLite {
	cfg := isql.BuildConfig(opts...)
	sq := SQLite{conn: conn, observer: cfg.Observer}
	if cfg.StmtCache {
		sq.cache = core.NewStmtCache()
	}
	return sq
}

var _ isql.Engine = SQLite{}

func (sq SQLite) BeginTx(ctx context.Context) (isql.Engine, error) {
	if sq.tx != nil {
		return nil, dberr.ErrTransactionAlreadyStarted
	}
	tx, err := sq.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	sq.tx = tx
	return sq, nil
}

func (sq SQLite) Commit() error {
	if sq.tx == nil {
		return dberr.ErrTransactionNotStarted
	}
	err := sq.tx.Commit()
	sq.tx = nil
	return err
}

func (sq SQLite) Rollback() error {
	if sq.tx == nil {
		return dberr.ErrTransactionNotStarted
	}
	err := sq.tx.Rollback()
	sq.tx = nil
	return err
}

func (sq SQLite) Table(name string) isql.Engine {
	sq.statement.Table(name)
	return sq
}

func (sq SQLite) ID(id any) isql.Engine {
	sq.statement.ID(id)
	return sq
}

func (sq SQLite) In(col string, values ...any) isql.Engine {
	sq.statement.In(col, values...)
	return sq
}

func (sq SQLite) Where(cond string, args ...any) isql.Engine {
	sq.statement.Where(cond, args...)
	return sq
}

func (sq SQLite) Columns(cols ...string) isql.Engine {
	sq.statement.Columns(cols...)
	return sq
}

func (sq SQLite) AllCols() isql.Engine {
	sq.statement.AllCols()
	return sq
}

func (sq SQLite) MustCols(cols ...string) isql.Engine {
	sq.statement.MustCols(cols...)
	return sq
}

func (sq SQLite) MustFilterCols(cols ...string) isql.Engine {
	sq.statement.MustFilterCols(cols...)
	return sq
}

func (sq SQLite) ShowSQL(showSQL bool) isql.Engine {
	sq.statement.ShowSQL(showSQL)
	return sq
}

func (sq SQLite) OrderBy(col string, direction ...string) isql.Engine {
	sq.statement.OrderBy(col, direction...)
	return sq
}

func (sq SQLite) Limit(n int64) isql.Engine {
	sq.statement.Limit(n)
	return sq
}

func (sq SQLite) Offset(n int64) isql.Engine {
	sq.statement.Offset(n)
	return sq
}

func (sq SQLite) Distinct() isql.Engine {
	sq.statement.Distinct()
	return sq
}

func (sq SQLite) GroupBy(cols ...string) isql.Engine {
	sq.statement.GroupBy(cols...)
	return sq
}

func (sq SQLite) Having(cond string, args ...any) isql.Engine {
	sq.statement.Having(cond, args...)
	return sq
}

func (sq SQLite) Or(cond string, args ...any) isql.Engine {
	sq.statement.Or(cond, args...)
	return sq
}

func (sq SQLite) Like(col string, pattern string) isql.Engine {
	sq.statement.Like(col, pattern)
	return sq
}

func (sq SQLite) NotLike(col string, pattern string) isql.Engine {
	sq.statement.NotLike(col, pattern)
	return sq
}

func (sq SQLite) Exists(subquery string, args ...any) isql.Engine {
	sq.statement.Exists(subquery, args...)
	return sq
}

func (sq SQLite) NotExists(subquery string, args ...any) isql.Engine {
	sq.statement.NotExists(subquery, args...)
	return sq
}

func (sq SQLite) Select(aggs ...isql.Aggregate) isql.Engine {
	exprs := make([]string, len(aggs))
	for i, a := range aggs {
		exprs[i] = a.Expr()
	}
	sq.statement.Select(exprs...)
	return sq
}

func (sq SQLite) Preload(assoc string) isql.Engine {
	sq.statement.Preload(assoc)
	return sq
}

// preload eager-loads any registered associations onto docs using a clean
// engine (same connection/transaction, fresh statement) for batched queries.
func (sq SQLite) preload(ctx context.Context, docs any) error {
	preloads := sq.statement.Preloads()
	if len(preloads) == 0 {
		return nil
	}
	base := sq
	base.statement = lib.Statement{}
	return core.PreloadRelations(ctx, base, docs, preloads)
}

func (sq SQLite) Paginate(page, perPage int64) isql.Engine {
	sq.statement.Paginate(page, perPage)
	return sq
}

// With registers sub as a named CTE. sub is compiled to a subquery in place;
// a non-SQLite Engine is ignored (returns the receiver unchanged).
func (sq SQLite) With(name string, sub isql.Engine) isql.Engine {
	s, ok := sub.(SQLite)
	if !ok {
		return sq
	}
	subSQL, subArgs := s.buildSubquery()
	sq.statement.With(name, subSQL, subArgs)
	return sq
}

// buildSubquery compiles the current statement into a SELECT string and its
// args without executing, for use as a CTE or subquery body.
func (sq SQLite) buildSubquery() (string, []any) {
	query := sq.statement.GenerateReadQuery(nil)
	return query, sq.statement.Args()
}

func (sq SQLite) Join(table, condition string) isql.Engine {
	sq.statement.Join(table, condition)
	return sq
}

func (sq SQLite) LeftJoin(table, condition string) isql.Engine {
	sq.statement.LeftJoin(table, condition)
	return sq
}

func (sq SQLite) RightJoin(table, condition string) isql.Engine {
	sq.statement.RightJoin(table, condition)
	return sq
}

func (sq SQLite) InnerJoin(table, condition string) isql.Engine {
	sq.statement.InnerJoin(table, condition)
	return sq
}

func (sq SQLite) EnableValidation(enable bool) isql.Engine {
	sq.statement.EnableValidation(enable)
	return sq
}

func (sq SQLite) WithDeleted() isql.Engine {
	sq.statement.WithDeleted()
	return sq
}

// detectSoftDelete sets soft delete column from struct tags if present.
func (s SQLite) detectSoftDelete(doc any) SQLite {
	if col := core.ExtractSoftDeleteColumn(doc); col != "" {
		s.statement.SoftDeleteCol(col)
	}
	return s
}

func (sq SQLite) ForceDelete(ctx context.Context, filter ...any) error {
	sq.statement.SetForceDelete()
	return sq.DeleteOne(ctx, filter...)
}

func (sq SQLite) Restore(ctx context.Context, filter ...any) error {
	if len(filter) > 0 {
		sq = sq.detectSoftDelete(filter[0])
	}
	sq.statement.GenerateWhereClause(filter...)
	if err := sq.statement.CheckWhereClauseNotEmpty(); err != nil {
		return err
	}

	query := sq.statement.GenerateRestoreQuery()
	result, err := sq.statement.ExecuteWriteQuery(ctx, sq.conn, sq.tx, query, sq.observer, sq.cache)
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

func (sq SQLite) FindOne(ctx context.Context, document any, filter ...any) (bool, error) {
	sq = sq.detectSoftDelete(document)
	sq.statement.GenerateWhereClause(filter...)

	if err := sq.statement.CheckWhereClauseNotEmpty(); err != nil {
		return false, err
	}

	query := sq.statement.GenerateReadQuery(document)
	err := sq.statement.ExecuteReadQuery(ctx, sq.conn, sq.tx, query, document, sq.observer, sq.cache)
	if err == nil {
		if err = core.RunAfterFind(ctx, document); err != nil {
			return false, err
		}
		if err = sq.preload(ctx, document); err != nil {
			return false, err
		}
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}

	return false, err
}

func (sq SQLite) FindMany(ctx context.Context, documents any, filter ...any) error {
	sq = sq.detectSoftDelete(documents)
	sq.statement.GenerateWhereClause(filter...)

	query := sq.statement.GenerateReadQuery(documents)
	if err := sq.statement.ExecuteReadQuery(ctx, sq.conn, sq.tx, query, documents, sq.observer, sq.cache); err != nil {
		return err
	}
	if err := core.RunAfterFindResults(ctx, documents); err != nil {
		return err
	}
	return sq.preload(ctx, documents)
}

// Count returns the number of rows in the table set via Table, matching any
// chained conditions. Soft-deleted rows are excluded unless WithDeleted was set.
func (sq SQLite) Count(ctx context.Context) (int64, error) {
	sq.statement.GenerateWhereClause()

	query := sq.statement.GenerateCountQuery()
	return sq.statement.ExecuteCountQuery(ctx, sq.conn, sq.tx, query, sq.observer, sq.cache)
}

func (sq SQLite) InsertOne(ctx context.Context, document any) (id any, err error) {
	if err := core.RunBeforeCreate(ctx, document); err != nil {
		return nil, err
	}
	if sq.statement.ShouldValidate() {
		if err := validation.Validate(document); err != nil {
			return nil, err
		}
	}
	pkCol := core.GetPKColumn(document)
	sq.statement.PKColumn(pkCol)
	query := sq.statement.GenerateInsertQuery(document)
	id, err = sq.statement.ExecuteInsertQuery(ctx, sq.conn, sq.tx, query, sq.observer, sq.cache)
	if err != nil {
		return nil, err
	}
	if _, err = assignID(document, id); err != nil {
		return nil, err
	}
	if err = core.RunAfterCreate(ctx, document); err != nil {
		return nil, err
	}
	return id, nil
}

func (sq SQLite) InsertMany(ctx context.Context, documents []any) ([]any, error) {
	if len(documents) == 0 {
		return nil, nil
	}
	for _, doc := range documents {
		if err := core.RunBeforeCreate(ctx, doc); err != nil {
			return nil, err
		}
		if sq.statement.ShouldValidate() {
			if err := validation.Validate(doc); err != nil {
				return nil, err
			}
		}
	}

	pkCol := core.GetPKColumn(documents[0])
	sq.statement.PKColumn(pkCol)
	query := sq.statement.GenerateBulkInsertQuery(documents)
	ids, err := sq.statement.ExecuteBulkInsertQuery(ctx, sq.conn, sq.tx, query, sq.observer, sq.cache)
	if err != nil {
		return nil, err
	}

	for i, doc := range documents {
		if i < len(ids) {
			if _, err := assignID(doc, ids[i]); err != nil {
				return nil, err
			}
		}
		if err := core.RunAfterCreate(ctx, doc); err != nil {
			return nil, err
		}
	}

	return ids, nil
}

func assignID(document any, id any) (any, error) {
	val := reflect.ValueOf(document)
	if val.Kind() != reflect.Ptr {
		return document, nil
		// first make it backward compatible
		// return id, fmt.Errorf("document must be a pointer to a struct")
	}

	valElem := val.Elem()
	if valElem.Kind() != reflect.Struct {
		return id, fmt.Errorf("document must be a pointer to a struct")
	}

	var idField = fetchIDField(valElem)
	if !idField.CanSet() {
		return id, fmt.Errorf("ID field is not settable")
	}

	idVal := reflect.ValueOf(id)
	if idField.Kind() == reflect.Ptr {
		elemType := idField.Type().Elem()
		if !idVal.Type().AssignableTo(elemType) && !idVal.Type().ConvertibleTo(elemType) {
			return id, fmt.Errorf("ID type %s cannot be assigned to pointer element type %s", idVal.Type(), elemType)
		}
		idValPtr := reflect.New(elemType)
		if idVal.Type().AssignableTo(elemType) {
			idValPtr.Elem().Set(idVal)
		} else {
			idValPtr.Elem().Set(idVal.Convert(elemType))
		}
		idField.Set(idValPtr)
	} else {
		if !idVal.Type().AssignableTo(idField.Type()) {
			if idVal.Type().ConvertibleTo(idField.Type()) {
				idVal = idVal.Convert(idField.Type())
			} else {
				return id, fmt.Errorf("ID type %s cannot be assigned or converted to field type %s", idVal.Type(), idField.Type())
			}
		}
		idField.Set(idVal)
	}

	return id, nil
}

func fetchIDField(valElem reflect.Value) (idField reflect.Value) {
	for i := 0; i < valElem.NumField(); i++ {
		field := valElem.Type().Field(i)
		dbTag := field.Tag.Get("db")
		if dbTag != "" {
			dbTag = strings.Split(dbTag, ",")[0]
		}
		jsonTag := field.Tag.Get("json")
		if dbTag == "id" || jsonTag == "id" {
			idField = valElem.Field(i)
			return idField
		}
	}

	idFieldNames := []string{"ID", "Id"}
	for _, name := range idFieldNames {
		idField = valElem.FieldByName(name)
		if idField.IsValid() {
			return idField
		}
	}
	return
}

func (sq SQLite) UpdateOne(ctx context.Context, document any) error {
	if err := core.RunBeforeUpdate(ctx, document); err != nil {
		return err
	}
	if sq.statement.ShouldValidate() {
		if err := validation.Validate(document); err != nil {
			return err
		}
	}
	sq.statement.GenerateWhereClause()
	if err := sq.statement.CheckWhereClauseNotEmpty(); err != nil {
		return err
	}

	query := sq.statement.GenerateUpdateQuery(document)
	result, err := sq.statement.ExecuteWriteQuery(ctx, sq.conn, sq.tx, query, sq.observer, sq.cache)
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

func (sq SQLite) DeleteOne(ctx context.Context, filter ...any) error {
	if len(filter) > 0 {
		sq = sq.detectSoftDelete(filter[0])
		if err := core.RunBeforeDelete(ctx, filter[0]); err != nil {
			return err
		}
	}
	sq.statement.GenerateWhereClause(filter...)
	if err := sq.statement.CheckWhereClauseNotEmpty(); err != nil {
		return err
	}

	var query string
	if sq.statement.IsSoftDelete() {
		query = sq.statement.GenerateSoftDeleteQuery()
	} else {
		query = sq.statement.GenerateDeleteQuery()
	}
	result, err := sq.statement.ExecuteWriteQuery(ctx, sq.conn, sq.tx, query, sq.observer, sq.cache)
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

func (sq SQLite) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if sq.tx != nil {
		return sq.tx.QueryContext(ctx, query, args...)
	}
	return sq.conn.QueryContext(ctx, query, args...)
}

func (sq SQLite) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if sq.tx != nil {
		return sq.tx.ExecContext(ctx, query, args...)
	}
	return sq.conn.ExecContext(ctx, query, args...)
}

func (sq SQLite) Sync(ctx context.Context, tables ...any) error {
	for _, table := range tables {
		if err := lib.SyncTable(ctx, sq.conn, table); err != nil {
			return err
		}
		core.RegisterSoftDeleteColumn(core.GetTableName(table), core.ExtractSoftDeleteColumn(table))
	}

	return nil
}

func (sq SQLite) DropTable(ctx context.Context, name string) error {
	return lib.DropTable(ctx, sq.conn, name)
}

func (sq SQLite) Close() error {
	return sq.conn.Close()
}

// Stats returns live statistics for the underlying connection pool.
func (sq SQLite) Stats() sql.DBStats {
	return sq.conn.Stats()
}

func (sq SQLite) cleanup() {
	sq.statement = lib.Statement{}
}
