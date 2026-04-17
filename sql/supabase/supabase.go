package supabase

import (
	"context"
	"database/sql"

	"github.com/masudur-rahman/styx/dberr"
	isql "github.com/masudur-rahman/styx/sql"

	"github.com/nedpals/supabase-go"
)

type Supabase struct {
	table  string
	id     any
	client *supabase.Client
}

func NewSupabase(client *supabase.Client) Supabase {
	return Supabase{
		client: client,
	}
}

func (s Supabase) BeginTx(ctx context.Context) (isql.Engine, error) {
	return nil, dberr.ErrTransactionNotStarted
}

func (s Supabase) Commit() error {
	return dberr.ErrTransactionNotStarted
}

func (s Supabase) Rollback() error {
	return dberr.ErrTransactionNotStarted
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

func (s Supabase) MustCols(cols ...string) isql.Engine {
	//TODO implement me
	panic("implement me")
}

func (s Supabase) MustFilterCols(cols ...string) isql.Engine {
	//TODO implement me
	panic("implement me")
}

func (s Supabase) ShowSQL(showSQL bool) isql.Engine {
	panic("implement me")
}

func (s Supabase) OrderBy(col string, direction ...string) isql.Engine {
	panic("implement me")
}

func (s Supabase) Limit(n int64) isql.Engine {
	panic("implement me")
}

func (s Supabase) Offset(n int64) isql.Engine {
	panic("implement me")
}

func (s Supabase) Distinct() isql.Engine {
	panic("implement me")
}

func (s Supabase) GroupBy(cols ...string) isql.Engine {
	panic("implement me")
}

func (s Supabase) Having(cond string, args ...any) isql.Engine {
	panic("implement me")
}

func (s Supabase) Or(cond string, args ...any) isql.Engine {
	panic("implement me")
}

func (s Supabase) Like(col string, pattern string) isql.Engine {
	panic("implement me")
}

func (s Supabase) NotLike(col string, pattern string) isql.Engine {
	panic("implement me")
}

func (s Supabase) Exists(subquery string, args ...any) isql.Engine {
	panic("implement me")
}

func (s Supabase) NotExists(subquery string, args ...any) isql.Engine {
	panic("implement me")
}

func (s Supabase) Count(col string, alias ...string) isql.Engine {
	panic("implement me")
}

func (s Supabase) Sum(col string, alias ...string) isql.Engine {
	panic("implement me")
}

func (s Supabase) Avg(col string, alias ...string) isql.Engine {
	panic("implement me")
}

func (s Supabase) Min(col string, alias ...string) isql.Engine {
	panic("implement me")
}

func (s Supabase) Max(col string, alias ...string) isql.Engine {
	panic("implement me")
}

func (s Supabase) Paginate(page, perPage int64) isql.Engine {
	panic("implement me")
}

func (s Supabase) EnableValidation(enable bool) isql.Engine {
	panic("implement me")
}

func (s Supabase) WithDeleted() isql.Engine {
	panic("implement me")
}

func (s Supabase) ForceDelete(ctx context.Context, filter ...any) error {
	panic("implement me")
}

func (s Supabase) Restore(ctx context.Context, filter ...any) error {
	panic("implement me")
}

func (s Supabase) FindOne(ctx context.Context, document any, filter ...any) (bool, error) {
	if err := dberr.CheckIdOrFilterNonEmpty(s.id, filter); err != nil {
		return false, err
	}

	var kvs []keyValue
	if s.id != nil && !dberr.IsZeroValue(s.id) {
		kvs = []keyValue{{"id", toString(s.id)}}
	} else if len(filter) > 0 {
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

func (s Supabase) FindMany(ctx context.Context, documents any, filter ...any) error {
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

func (s Supabase) InsertOne(ctx context.Context, document any) (id any, err error) {
	docs := []Doc{}
	err = s.client.DB.From(s.table).Insert(document).Execute(&docs)
	if err != nil {
		return int64(0), err
	}
	return docs[0].ID.(int64), nil
}

func (s Supabase) InsertMany(ctx context.Context, documents []any) ([]any, error) {
	var ids = make([]any, 0, len(documents))
	for idx := range documents {
		id, err := s.InsertOne(ctx, documents[idx])
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func (s Supabase) UpdateOne(ctx context.Context, document any) error {
	if err := dberr.CheckIDNonEmpty(s.id); err != nil {
		return err
	}

	return s.client.DB.From(s.table).Update(document).Eq("id", toString(s.id)).Execute(&document)
}

func (s Supabase) DeleteOne(ctx context.Context, filter ...any) error {
	if err := dberr.CheckIdOrFilterNonEmpty(s.id, filter); err != nil {
		return err
	}

	var kvs []keyValue
	if s.id != nil && !dberr.IsZeroValue(s.id) {
		kvs = []keyValue{{"id", toString(s.id)}}
	} else if len(filter) > 0 {
		kvs = generateFilters(filter[0])
	}

	cl := s.client.DB.From(s.table).Delete()
	for idx := range kvs {
		cl.Eq(kvs[idx].key, kvs[idx].value)
	}

	rs := map[string]interface{}{}
	return cl.Execute(&rs)
}

func (s Supabase) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	panic("implement me")
}

func (s Supabase) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	panic("implement me")
}

func (s Supabase) Sync(ctx context.Context, tables ...any) error {
	//TODO implement me
	panic("implement me")
}

func (s Supabase) Close() error { return nil }
