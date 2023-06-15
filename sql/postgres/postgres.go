package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/masudur-rahman/database/sql/postgres/lib"
	"strings"

	"github.com/masudur-rahman/database/dberr"
	"github.com/masudur-rahman/database/pkg"
	isql "github.com/masudur-rahman/database/sql"

	arango "github.com/arangodb/go-driver"
)

type Postgres struct {
	ctx     context.Context
	table   string
	id      string
	columns []string
	allCols bool
	where   string
	args    []any
	conn    *sql.Conn
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
	pg.args = append(pg.args, args)
	return pg
}

func (pg Postgres) addWhereClause(cond string) string {
	if pg.where != "" {
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
		query = fmt.Sprintf("%s WHERE %s", query, pg.where)
	}

	return query
}

func (pg Postgres) executeReadQuery(query string) string {
	rows, err := pg.conn.QueryContext(pg.ctx, query, pg.args)
	if err != nil {
		return
	}
}

func (pg Postgres) FindOne(document interface{}, filter ...any) (bool, error) {
	pg.where = pg.addWhereClause(lib.GenerateWhereClauseFromID(pg.id))
	if len(filter) > 0 {
		pg.where = pg.addWhereClause(lib.GenerateWhereClauseFromFilter(filter[0]))
	}
	if err := dberr.CheckIdOrFilterNonEmpty(pg.id, filter); err != nil {
		return false, err
	}
	if pg.where == "" {
		return false, fmt.Errorf("no filter parameter passed")
	}

	query := pg.generateReadQuery()
	lib.GenerateReadQuery()
	collection, err := getDBCollection(pg.ctx, pg.db, pg.collectionName)
	if err != nil {
		return false, err
	}

	if filter == nil {
		meta, err := collection.ReadDocument(pg.ctx, pg.id, document)
		if arango.IsNotFoundGeneral(err) {
			return false, nil
		}
		return meta.ID != "", err
	}

	query := generateArangoQuery(pg.collectionName, filter[0], false)
	results, err := executeArangoQuery(pg.ctx, pg.db, query, 1)
	if arango.IsNotFoundGeneral(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if len(results) != 1 {
		return false, nil
	}

	//reflect.ValueOf(documents).Elem().Set(reflect.ValueOf(results))
	if err = pkg.ParseInto(results[0], document); err != nil {
		return false, err
	}
	return true, nil
}

func (pg Postgres) FindMany(documents interface{}, filter interface{}) error {
	//TODO implement me
	panic("implement me")
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
