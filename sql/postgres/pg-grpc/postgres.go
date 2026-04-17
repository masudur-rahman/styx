package pg_grpc

import (
	"context"
	"database/sql"
	"strings"

	"github.com/masudur-rahman/styx/dberr"
	"github.com/masudur-rahman/styx/pkg"
	isql "github.com/masudur-rahman/styx/sql"
	"github.com/masudur-rahman/styx/sql/postgres/pg-grpc/pb"

	"google.golang.org/protobuf/types/known/anypb"
)

type Database struct {
	table  string
	id     any
	client pb.PostgresClient
}

func NewDatabase(client pb.PostgresClient) Database {
	return Database{
		client: client,
	}
}

func (d Database) BeginTx(ctx context.Context) (isql.Engine, error) {
	return nil, dberr.ErrTransactionNotStarted
}

func (d Database) Commit() error {
	return dberr.ErrTransactionNotStarted
}

func (d Database) Rollback() error {
	return dberr.ErrTransactionNotStarted
}

func (d Database) Table(name string) isql.Engine {
	d.table = name
	return d
}

func (d Database) ID(id any) isql.Engine {
	d.id = id
	return d
}

func (d Database) In(s string, a ...any) isql.Engine {
	panic("implement me")
}

func (d Database) Where(s string, a ...any) isql.Engine {
	panic("implement me")
}

func (d Database) Columns(s ...string) isql.Engine {
	panic("implement me")
}

func (d Database) AllCols() isql.Engine {
	panic("implement me")
}

func (d Database) MustCols(cols ...string) isql.Engine {
	panic("implement me")
}

func (d Database) MustFilterCols(cols ...string) isql.Engine {
	panic("implement me")
}

func (d Database) ShowSQL(showSQL bool) isql.Engine {
	panic("implement me")
}

func (d Database) OrderBy(col string, direction ...string) isql.Engine {
	panic("implement me")
}

func (d Database) Limit(n int64) isql.Engine {
	panic("implement me")
}

func (d Database) Offset(n int64) isql.Engine {
	panic("implement me")
}

func (d Database) Distinct() isql.Engine {
	panic("implement me")
}

func (d Database) GroupBy(cols ...string) isql.Engine {
	panic("implement me")
}

func (d Database) Having(cond string, args ...any) isql.Engine {
	panic("implement me")
}

func (d Database) Or(cond string, args ...any) isql.Engine {
	panic("implement me")
}

func (d Database) Like(col string, pattern string) isql.Engine {
	panic("implement me")
}

func (d Database) NotLike(col string, pattern string) isql.Engine {
	panic("implement me")
}

func (d Database) Exists(subquery string, args ...any) isql.Engine {
	panic("implement me")
}

func (d Database) NotExists(subquery string, args ...any) isql.Engine {
	panic("implement me")
}

func (d Database) Count(col string, alias ...string) isql.Engine {
	panic("implement me")
}

func (d Database) Sum(col string, alias ...string) isql.Engine {
	panic("implement me")
}

func (d Database) Avg(col string, alias ...string) isql.Engine {
	panic("implement me")
}

func (d Database) Min(col string, alias ...string) isql.Engine {
	panic("implement me")
}

func (d Database) Max(col string, alias ...string) isql.Engine {
	panic("implement me")
}

func (d Database) Paginate(page, perPage int64) isql.Engine {
	panic("implement me")
}

func (d Database) Join(table, condition string) isql.Engine {
	panic("implement me")
}

func (d Database) LeftJoin(table, condition string) isql.Engine {
	panic("implement me")
}

func (d Database) RightJoin(table, condition string) isql.Engine {
	panic("implement me")
}

func (d Database) InnerJoin(table, condition string) isql.Engine {
	panic("implement me")
}

func (d Database) EnableValidation(enable bool) isql.Engine {
	panic("implement me")
}

func (d Database) WithDeleted() isql.Engine {
	panic("implement me")
}

func (d Database) ForceDelete(ctx context.Context, filter ...any) error {
	panic("implement me")
}

func (d Database) Restore(ctx context.Context, filter ...any) error {
	panic("implement me")
}

func (d Database) FindOne(ctx context.Context, document any, filter ...any) (bool, error) {
	var err error
	if err = dberr.CheckIdOrFilterNonEmpty(d.id, filter); err != nil {
		return false, err
	}

	record := new(pb.RecordResponse)

	if filter == nil {
		idStr, _ := d.id.(string)
		record, err = d.client.GetById(ctx, &pb.IdParams{
			Table: d.table,
			Id:    idStr,
		})
	} else {
		var af *anypb.Any
		af, err = pkg.ToProtoAny(filter[0])
		if err != nil {
			return false, err
		}

		record, err = d.client.Get(ctx, &pb.FilterParams{
			Table:  d.table,
			Filter: af,
		})
	}
	if err != nil {
		if strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
			return false, nil
		}
		return false, err
	}

	if err = pkg.ParseProtoAnyInto(record.Record, document); err != nil {
		return false, err
	}

	return true, nil
}

func (d Database) FindMany(ctx context.Context, documents any, filter ...any) error {
	af, err := pkg.ToProtoAny(filter)
	if err != nil {
		return err
	}

	records, err := d.client.Find(ctx, &pb.FilterParams{
		Table:  d.table,
		Filter: af,
	})
	if err != nil {
		return err
	}

	rmaps := make([]map[string]interface{}, 0)
	for _, record := range records.Records {
		rmap, err := pkg.ProtoAnyToMap(record.Record)
		if err != nil {
			return err
		}
		rmaps = append(rmaps, rmap)
	}

	return pkg.ParseInto(rmaps, documents)
}

func (d Database) InsertOne(ctx context.Context, document any) (id any, err error) {
	df, err := pkg.ToProtoAny(document)
	if err != nil {
		return nil, err
	}

	record, err := d.client.Create(ctx, &pb.CreateParams{
		Table:  d.table,
		Record: df,
	})
	if err != nil {
		return nil, err
	}

	rmap, err := pkg.ProtoAnyToMap(record.Record)
	if err != nil {
		return nil, err
	}

	if err = pkg.ParseInto(rmap, document); err != nil {
		return nil, err
	}

	return rmap["id"], nil
}

func (d Database) InsertMany(ctx context.Context, documents []any) ([]any, error) {
	var ids []any

	for idx := range documents {
		id, err := d.InsertOne(ctx, documents[idx])
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func (d Database) UpdateOne(ctx context.Context, document any) error {
	if err := dberr.CheckIDNonEmpty(d.id); err != nil {
		return err
	}

	df, err := pkg.ToProtoAny(document)
	if err != nil {
		return err
	}

	record, err := d.client.Update(ctx, &pb.UpdateParams{
		Table:  d.table,
		Id:     d.id.(string),
		Record: df,
	})
	if err != nil {
		return err
	}

	return pkg.ParseProtoAnyInto(record.Record, document)
}

func (d Database) DeleteOne(ctx context.Context, filter ...any) error {
	if err := dberr.CheckIdOrFilterNonEmpty(d.id, filter); err != nil {
		return err
	}

	if filter != nil {
		doc := struct {
			ID string `json:"id"`
		}{}
		found, err := d.FindOne(ctx, &doc, filter)
		if err != nil {
			return err
		} else if !found {
			return dberr.ErrNotFound
		}
		d.id = doc.ID
	}

	_, err := d.client.Delete(ctx, &pb.IdParams{
		Table: d.table,
		Id:    d.id.(string),
	})
	return err
}

func (d Database) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	panic("implement me")
}

func (d Database) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	panic("implement me")
}

func (d Database) Sync(ctx context.Context, tables ...any) error {
	return dberr.ErrTransactionNotStarted
}

func (d Database) DropTable(ctx context.Context, name string) error {
	panic("implement me")
}

func (d Database) Close() error {
	return nil
}
