package supabase

import (
	"context"
	"database/sql"

	"github.com/masudur-rahman/styx/dberr"
	isql "github.com/masudur-rahman/styx/sql"

	"github.com/nedpals/supabase-go"
)

type Supabase struct {
	ctx    context.Context
	table  string
	id     any
	client *supabase.Client
}

func NewSupabase(ctx context.Context, client *supabase.Client) Supabase {
	return Supabase{
		ctx:    ctx,
		client: client,
	}
}

func (s Supabase) Table(name string) isql.Engine {
	s.table = name
	return s
}

func (s Supabase) ID(id any) isql.Engine {
	s.id = id
	return s
}

func (s Supabase) In(col string, values ...any) isql.Engine {
	//TODO implement me
	panic("implement me")
}

func (s Supabase) Where(cond string, args ...any) isql.Engine {
	//TODO implement me
	panic("implement me")
}

func (s Supabase) Columns(cols ...string) isql.Engine {
	//TODO implement me
	panic("implement me")
}

func (s Supabase) AllCols() isql.Engine {
	//TODO implement me
	panic("implement me")
}

func (s Supabase) FindOne(document interface{}, filter ...interface{}) (bool, error) {
	if err := dberr.CheckIdOrFilterNonEmpty(s.id, filter); err != nil {
		return false, err
	}

	var kvs []keyValue
	if s.id != "" {
		kvs = []keyValue{{"id", toString(s.id)}}
	} else {
		kvs = generateFilters(filter[0])
	}

	cl := s.client.DB.From(s.table).Select("*").Single()
	for idx := range kvs {
		cl.Eq(kvs[idx].key, kvs[idx].value)
	}
	if err := cl.Execute(document); err != nil {
		return false, err
	}

	return true, nil
}

func (s Supabase) FindMany(documents interface{}, filter ...interface{}) error {
	kvs := generateFilters(filter)
	cl := s.client.DB.From(s.table).Select("*")

	for idx := range kvs {
		cl.Eq(kvs[idx].key, kvs[idx].value)
	}
	if err := cl.Execute(documents); err != nil {
		return err
	}

	return nil
}

func (s Supabase) InsertOne(document interface{}) (id int64, err error) {
	docs := []Doc{}
	err = s.client.DB.From(s.table).Insert(document).Execute(&docs)
	if err != nil {
		return 0, err
	}
	return docs[0].ID.(int64), nil
}

func (s Supabase) InsertMany(documents []interface{}) ([]int64, error) {
	var ids = make([]int64, 0, len(documents))
	for idx := range documents {
		id, err := s.InsertOne(documents[idx])
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func (s Supabase) UpdateOne(document interface{}) error {
	if err := dberr.CheckIDNonEmpty(s.id); err != nil {
		return err
	}

	return s.client.DB.From(s.table).Update(document).Eq("id", toString(s.id)).Execute(&document)
}

func (s Supabase) DeleteOne(filter ...interface{}) error {
	if err := dberr.CheckIdOrFilterNonEmpty(s.id, filter); err != nil {
		return err
	}

	var kvs []keyValue
	if s.id != "" {
		kvs = []keyValue{{"id", toString(s.id)}}
	} else {
		kvs = generateFilters(filter[0])
	}

	cl := s.client.DB.From(s.table).Delete()
	for idx := range kvs {
		cl.Eq(kvs[idx].key, kvs[idx].value)
	}

	rs := map[string]interface{}{}
	return cl.Execute(&rs)
}

func (s Supabase) Query(query string, args ...interface{}) (*sql.Rows, error) {
	//TODO implement me
	panic("implement me")
}

func (s Supabase) Exec(query string, args ...interface{}) (sql.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (s Supabase) Sync(a ...any) error {
	//TODO implement me
	panic("implement me")
}

func (s Supabase) Close() error { return nil }
