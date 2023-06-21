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

	log.Println(query, pg.args)
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

func (pg Postgres) FindMany(documents interface{}, filter ...interface{}) error {
	if len(filter) > 0 {
		pg.where = pg.addWhereClause(lib.GenerateWhereClauseFromFilter(filter[0]))
	}

	//if pg.where == "" {
	//	return fmt.Errorf("no filter parameter passed")
	//}

	query := pg.generateReadQuery()
	return pg.executeReadQuery(query, documents)
}

func (pg Postgres) InsertOne(document interface{}) (id string, err error) {
	//TODO implement me
	panic("implement me")
}

func (pg Postgres) InsertMany(documents []interface{}) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (pg Postgres) UpdateOne(document interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (pg Postgres) DeleteOne(filter ...interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (pg Postgres) Query(query string, args ...interface{}) (*sql.Rows, error) {
	//TODO implement me
	panic("implement me")
}

func (pg Postgres) Exec(query string, args ...interface{}) (sql.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (pg Postgres) cleanup() {
	fmt.Println(pg.table, pg.id, pg.columns, pg.allCols, pg.where, pg.args)
	pg.id = nil
	pg.columns = []string{}
	pg.allCols = false
	pg.where = ""
	pg.args = nil
}
