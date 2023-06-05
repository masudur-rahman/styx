package postgres

import (
	"context"
	"database/sql"

	"github.com/masudur-rahman/database/dberr"
	"github.com/masudur-rahman/database/pkg"
	isql "github.com/masudur-rahman/database/sql"

	arango "github.com/arangodb/go-driver"
)

type Postgres struct {
	ctx   context.Context
	table string
	id    string
	conn  *sql.Conn
}

func (pg Postgres) Table(name string) isql.Database {
	pg.table = name
	return pg
}

func (pg Postgres) ID(id string) isql.Database {
	pg.id = id
	return pg
}

func (pg Postgres) FindOne(document interface{}, filter ...interface{}) (bool, error) {
	if err := dberr.CheckIdOrFilterNonEmpty(pg.id, filter); err != nil {
		return false, err
	}

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
