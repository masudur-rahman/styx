package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/masudur-rahman/styx/dberr"
	isql "github.com/masudur-rahman/styx/sql"
	"github.com/masudur-rahman/styx/sql/sqlite/lib"

	_ "modernc.org/sqlite"
)

type SQLite struct {
	conn      *sql.DB
	tx        *sql.Tx
	statement lib.Statement
}

func NewSQLite(conn *sql.DB) SQLite {
	return SQLite{conn: conn}
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

func (sq SQLite) Count(col string, alias ...string) isql.Engine {
	sq.statement.Count(col, alias...)
	return sq
}

func (sq SQLite) Sum(col string, alias ...string) isql.Engine {
	sq.statement.Sum(col, alias...)
	return sq
}

func (sq SQLite) Avg(col string, alias ...string) isql.Engine {
	sq.statement.Avg(col, alias...)
	return sq
}

func (sq SQLite) Min(col string, alias ...string) isql.Engine {
	sq.statement.Min(col, alias...)
	return sq
}

func (sq SQLite) Max(col string, alias ...string) isql.Engine {
	sq.statement.Max(col, alias...)
	return sq
}

func (sq SQLite) FindOne(ctx context.Context, document any, filter ...any) (bool, error) {
	sq.statement.GenerateWhereClause(filter...)

	if err := sq.statement.CheckWhereClauseNotEmpty(); err != nil {
		return false, err
	}

	query := sq.statement.GenerateReadQuery(document)
	err := sq.statement.ExecuteReadQuery(ctx, sq.conn, sq.tx, query, document)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}

	return false, err
}

func (sq SQLite) FindMany(ctx context.Context, documents any, filter ...any) error {
	sq.statement.GenerateWhereClause(filter...)

	query := sq.statement.GenerateReadQuery(documents)
	return sq.statement.ExecuteReadQuery(ctx, sq.conn, sq.tx, query, documents)
}

func (sq SQLite) InsertOne(ctx context.Context, document any) (id any, err error) {
	pkCol := lib.ExtractPKColumn(document)
	sq.statement.PKColumn(pkCol)
	query := sq.statement.GenerateInsertQuery(document)
	id, err = sq.statement.ExecuteInsertQuery(ctx, sq.conn, sq.tx, query)
	if err != nil {
		return nil, err
	}
	return assignID(document, id)
}

func (sq SQLite) InsertMany(ctx context.Context, documents []any) ([]any, error) {
	var ids []any
	for _, doc := range documents {
		pkCol := lib.ExtractPKColumn(doc)
		sq.statement.PKColumn(pkCol)
		query := sq.statement.GenerateInsertQuery(doc)
		id, err := sq.statement.ExecuteInsertQuery(ctx, sq.conn, sq.tx, query)
		if err != nil {
			return nil, err
		}

		_, err = assignID(doc, id)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
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
	sq.statement.GenerateWhereClause()
	if err := sq.statement.CheckWhereClauseNotEmpty(); err != nil {
		return err
	}

	query := sq.statement.GenerateUpdateQuery(document)
	result, err := sq.statement.ExecuteWriteQuery(ctx, sq.conn, sq.tx, query)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return dberr.DataNotFound
	}
	return nil
}

func (sq SQLite) DeleteOne(ctx context.Context, filter ...any) error {
	sq.statement.GenerateWhereClause(filter...)
	if err := sq.statement.CheckWhereClauseNotEmpty(); err != nil {
		return err
	}

	query := sq.statement.GenerateDeleteQuery()
	result, err := sq.statement.ExecuteWriteQuery(ctx, sq.conn, sq.tx, query)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return dberr.DataNotFound
	}
	return nil
}

func (sq SQLite) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return sq.conn.QueryContext(ctx, query, args...)
}

func (sq SQLite) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return sq.conn.ExecContext(ctx, query, args...)
}

func (sq SQLite) Sync(ctx context.Context, tables ...any) error {
	for _, table := range tables {
		if err := lib.SyncTable(ctx, sq.conn, table); err != nil {
			return err
		}
	}

	return nil
}

func (sq SQLite) Close() error {
	return sq.conn.Close()
}

func (sq SQLite) cleanup() {
	sq.statement = lib.Statement{}
}
