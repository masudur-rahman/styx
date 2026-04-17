package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/masudur-rahman/styx/dberr"
	isql "github.com/masudur-rahman/styx/sql"
	"github.com/masudur-rahman/styx/sql/postgres/lib"
)

type Postgres struct {
	conn      *sql.DB
	tx        *sql.Tx
	statement lib.Statement
}

func NewPostgres(conn *sql.DB) Postgres {
	return Postgres{conn: conn}
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

func (pg Postgres) FindOne(ctx context.Context, document any, filter ...any) (bool, error) {
	pg.statement.GenerateWhereClause(filter...)

	if err := pg.statement.CheckWhereClauseNotEmpty(); err != nil {
		return false, err
	}

	query := pg.statement.GenerateReadQuery(document)
	err := pg.statement.ExecuteReadQuery(ctx, pg.conn, pg.tx, query, document)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}

	return false, err
}

func (pg Postgres) FindMany(ctx context.Context, documents any, filter ...any) error {
	pg.statement.GenerateWhereClause(filter...)

	query := pg.statement.GenerateReadQuery(documents)
	return pg.statement.ExecuteReadQuery(ctx, pg.conn, pg.tx, query, documents)
}

func (pg Postgres) InsertOne(ctx context.Context, document any) (id any, err error) {
	pkCol := lib.ExtractPKColumn(document)
	pg.statement.PKColumn(pkCol)
	query := pg.statement.GenerateInsertQuery(document)
	id, err = pg.statement.ExecuteInsertQuery(ctx, pg.conn, pg.tx, query)
	if err != nil {
		return nil, err
	}
	return assignID(document, id)
}

func (pg Postgres) InsertMany(ctx context.Context, documents []any) ([]any, error) {
	var ids []any
	for _, doc := range documents {
		pkCol := lib.ExtractPKColumn(doc)
		pg.statement.PKColumn(pkCol)
		query := pg.statement.GenerateInsertQuery(doc)
		id, err := pg.statement.ExecuteInsertQuery(ctx, pg.conn, pg.tx, query)
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

func (pg Postgres) UpdateOne(ctx context.Context, document any) error {
	pg.statement.GenerateWhereClause()
	if err := pg.statement.CheckWhereClauseNotEmpty(); err != nil {
		return err
	}

	query := pg.statement.GenerateUpdateQuery(document)
	result, err := pg.statement.ExecuteWriteQuery(ctx, pg.conn, pg.tx, query)
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

func (pg Postgres) DeleteOne(ctx context.Context, filter ...any) error {
	pg.statement.GenerateWhereClause(filter...)
	if err := pg.statement.CheckWhereClauseNotEmpty(); err != nil {
		return err
	}

	query := pg.statement.GenerateDeleteQuery()
	result, err := pg.statement.ExecuteWriteQuery(ctx, pg.conn, pg.tx, query)
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

func (pg Postgres) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return pg.conn.QueryContext(ctx, query, args...)
}

func (pg Postgres) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return pg.conn.ExecContext(ctx, query, args...)
}

func (pg Postgres) Sync(ctx context.Context, tables ...any) error {
	for _, table := range tables {
		if err := lib.SyncTable(ctx, pg.conn, table); err != nil {
			return err
		}
	}

	return nil
}

func (pg Postgres) Close() error {
	return pg.conn.Close()
}

func (pg Postgres) cleanup() {
	pg.statement = lib.Statement{}
}
