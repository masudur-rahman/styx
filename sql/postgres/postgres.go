package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"

	isql "github.com/masudur-rahman/database/sql"
	"github.com/masudur-rahman/database/sql/postgres/lib"
)

type Postgres struct {
	ctx     context.Context
	table   string
	id      any
	columns []string
	allCols bool
	where   string
	args    []any
	conn    *sql.Conn
}

func NewPostgres(ctx context.Context, conn *sql.Conn) Postgres {
	return Postgres{ctx: ctx, conn: conn}
}

func (pg Postgres) Table(name string) isql.Database {
	pg.table = name
	return pg
}

func (pg Postgres) ID(id any) isql.Database {
	if pg.where != "" {
		pg.where += " AND "
	}

	pg.id = id
	return pg
}

func (pg Postgres) In(col string, values ...any) isql.Database {
	if pg.where != "" {
		pg.where += " AND "
	}

	pg.where += fmt.Sprintf("%s IN %s", col, lib.HandleSliceAny(values))
	return pg
}

func (pg Postgres) Where(cond string, args ...any) isql.Database {
	pg.where = pg.addWhereClause(cond)
	if len(args) > 0 {
		pg.args = append(pg.args, args)
	}
	return pg
}

func (pg Postgres) addWhereClause(cond string) string {
	if pg.where != "" && cond != "" {
		pg.where += " AND "
	}

	pg.where += cond
	return pg.where
}

func (pg Postgres) Columns(cols ...string) isql.Database {
	pg.columns = cols
	return pg
}

func (pg Postgres) AllCols() isql.Database {
	pg.allCols = true
	return pg
}

func (pg Postgres) generateReadQuery() string {
	var cols string
	if pg.allCols || len(pg.columns) == 0 {
		cols = "*"
	} else {
		cols = strings.Join(pg.columns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM \"%s\"", cols, pg.table)

	if pg.where != "" {
		query = fmt.Sprintf("%s WHERE %s;", query, pg.where)
	}

	return query
}

func (pg Postgres) executeReadQuery(query string, doc any) error {
	//defer pg.cleanup()

	log.Printf("Read Query: query: %v, args: %v\n", query, pg.args)
	rows, err := pg.conn.QueryContext(pg.ctx, query, pg.args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	elem := reflect.ValueOf(doc).Elem()
	switch elem.Kind() {
	case reflect.Struct:
		if rows.Next() {
			fieldMap := lib.GenerateDBFieldMap(doc)
			if err = lib.ScanSingleRow(rows, fieldMap); err != nil {
				return err
			}

			return rows.Err()
		}
	case reflect.Slice:
		for rows.Next() {
			rowELem := reflect.New(elem.Type().Elem()).Interface()
			fieldMap := lib.GenerateDBFieldMap(rowELem)
			if err = lib.ScanSingleRow(rows, fieldMap); err != nil {
				return err
			}
			elem.Set(reflect.Append(elem, reflect.ValueOf(rowELem).Elem()))
		}

		return rows.Err()
	}

	return sql.ErrNoRows
}

func (pg Postgres) FindOne(document any, filter ...any) (bool, error) {
	pg.where = pg.addWhereClause(lib.GenerateWhereClauseFromID(pg.id))
	if len(filter) > 0 {
		pg.where = pg.addWhereClause(lib.GenerateWhereClauseFromFilter(filter[0]))
	}

	if pg.where == "" {
		return false, fmt.Errorf("no filter parameter passed")
	}

	query := pg.generateReadQuery()
	err := pg.executeReadQuery(query, document)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}

	return false, err
}

func (pg Postgres) FindMany(documents any, filter ...any) error {
	if len(filter) > 0 {
		pg.where = pg.addWhereClause(lib.GenerateWhereClauseFromFilter(filter[0]))
	}

	query := pg.generateReadQuery()
	return pg.executeReadQuery(query, documents)
}

func (pg Postgres) executeInsertQuery(query string) (int64, error) {
	query += " RETURNING id;"
	log.Printf("Insert Query: query: %v, args: %v\n", query, pg.args)
	var id int64
	err := pg.conn.QueryRowContext(pg.ctx, query, pg.args...).Scan(&id)
	return id, err
}

func (pg Postgres) executeWriteQuery(query string) (sql.Result, error) {
	log.Printf("Write Query: query: %v, args: %v\n", query, pg.args)
	result, err := pg.conn.ExecContext(pg.ctx, query, pg.args...)

	return result, err
}

func (pg Postgres) InsertOne(document any) (id int64, err error) {
	query := lib.GenerateInsertQueries(pg.table, document)
	return pg.executeInsertQuery(query)
}

func (pg Postgres) InsertMany(documents []any) ([]int64, error) {
	var ids []int64
	for _, doc := range documents {
		query := lib.GenerateInsertQueries(pg.table, doc)
		id, err := pg.executeInsertQuery(query)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func (pg Postgres) UpdateOne(document any) error {
	pg.where = pg.addWhereClause(lib.GenerateWhereClauseFromID(pg.id))
	if pg.where == "" {
		return fmt.Errorf("no filter parameter passed")
	}

	query := lib.GenerateUpdateQueries(pg.table, pg.where, document)
	_, err := pg.executeWriteQuery(query)
	return err
}

func (pg Postgres) DeleteOne(filter ...any) error {
	pg.where = pg.addWhereClause(lib.GenerateWhereClauseFromID(pg.id))
	if len(filter) > 0 {
		pg.where = pg.addWhereClause(lib.GenerateWhereClauseFromFilter(filter[0]))
	}

	if pg.where == "" {
		return fmt.Errorf("no filter parameter passed")
	}
	query := lib.GenerateDeleteQueries(pg.table, pg.where)
	_, err := pg.executeWriteQuery(query)
	return err
}

func (pg Postgres) Query(query string, args ...any) (*sql.Rows, error) {
	return pg.conn.QueryContext(pg.ctx, query, args...)
}

func (pg Postgres) Exec(query string, args ...any) (sql.Result, error) {
	return pg.conn.ExecContext(pg.ctx, query, args...)
}

func (p Postgres) Sync(tables ...any) error {
	ctx := context.Background()
	for _, table := range tables {
		if err := lib.SyncTable(ctx, p.conn, table); err != nil {
			return err
		}
	}

	return nil
}

func (pg Postgres) cleanup() {
	fmt.Println(pg.table, pg.id, pg.columns, pg.allCols, pg.where, pg.args)
	pg.id = nil
	pg.columns = []string{}
	pg.allCols = false
	pg.where = ""
	pg.args = nil
}
