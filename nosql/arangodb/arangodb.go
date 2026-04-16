package arangodb

import (
	"context"

	"github.com/masudur-rahman/styx/dberr"
	"github.com/masudur-rahman/styx/nosql"
	"github.com/masudur-rahman/styx/pkg"

	arango "github.com/arangodb/go-driver"
)

type ArangoDB struct {
	db             arango.Database
	id             string
	collectionName string
}

func NewArangoDB(db arango.Database) ArangoDB {
	return ArangoDB{
		db: db,
	}
}

func (a ArangoDB) Collection(collection string) nosql.Engine {
	a.collectionName = collection
	return a
}

func (a ArangoDB) ID(id string) nosql.Engine {
	a.id = id
	return a
}

func (a ArangoDB) FindOne(ctx context.Context, document interface{}, filter ...interface{}) (bool, error) {
	if err := dberr.CheckIdOrFilterNonEmpty(a.id, filter); err != nil {
		return false, err
	}

	collection, err := getDBCollection(ctx, a.db, a.collectionName)
	if err != nil {
		return false, err
	}

	if filter == nil {
		meta, err := collection.ReadDocument(ctx, a.id, document)
		if arango.IsNotFoundGeneral(err) {
			return false, nil
		}
		return meta.ID != "", err
	}

	query := generateArangoQuery(a.collectionName, filter[0], false)
	results, err := executeArangoQuery(ctx, a.db, query, 1)
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

func (a ArangoDB) FindMany(ctx context.Context, documents interface{}, filter interface{}) error {
	_, err := getDBCollection(ctx, a.db, a.collectionName)
	if err != nil {
		return err
	}

	query := generateArangoQuery(a.collectionName, filter, false)
	results, err := executeArangoQuery(ctx, a.db, query, -1)
	if err != nil {
		return err
	}

	return pkg.ParseInto(results, documents)
}

func (a ArangoDB) InsertOne(ctx context.Context, document interface{}) (id string, err error) {
	collection, err := getDBCollection(ctx, a.db, a.collectionName)
	if err != nil {
		return "", err
	}

	meta, err := collection.CreateDocument(ctx, document)
	if err != nil {
		return "", err
	}

	return meta.Key, nil
}

func (a ArangoDB) InsertMany(ctx context.Context, documents []interface{}) ([]string, error) {
	collection, err := getDBCollection(ctx, a.db, a.collectionName)
	if err != nil {
		return nil, err
	}

	metas, _, err := collection.CreateDocuments(ctx, documents)
	if err != nil {
		return nil, err
	}

	// Extract IDs of inserted documents
	ids := make([]string, len(metas))
	for i, result := range metas {
		ids[i] = string(result.ID)
	}

	return ids, nil
}

func (a ArangoDB) UpdateOne(ctx context.Context, document interface{}) error {
	if err := dberr.CheckIDNonEmpty(a.id); err != nil {
		return err
	}

	collection, err := getDBCollection(ctx, a.db, a.collectionName)
	if err != nil {
		return err
	}

	_, err = collection.UpdateDocument(ctx, a.id, document)
	return err
}

func (a ArangoDB) DeleteOne(ctx context.Context, filter ...interface{}) error {
	if err := dberr.CheckIdOrFilterNonEmpty(a.id, filter); err != nil {
		return err
	}

	collection, err := getDBCollection(ctx, a.db, a.collectionName)
	if err != nil {
		return err
	}

	if filter == nil {
		_, err = collection.RemoveDocument(ctx, a.id)
		return err
	}

	query := generateArangoQuery(a.collectionName, filter[0], true)
	_, err = executeArangoQuery(ctx, a.db, query, 1)
	if err != nil {
		return err
	}

	return nil
}

func (a ArangoDB) Query(ctx context.Context, query string, bindParams map[string]interface{}) (interface{}, error) {
	_, err := getDBCollection(ctx, a.db, a.collectionName)
	if err != nil {
		return nil, err
	}

	return executeArangoQuery(ctx, a.db, &Query{queryString: query, bindVars: bindParams}, -1)
}
