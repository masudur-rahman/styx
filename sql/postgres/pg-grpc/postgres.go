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
	ctx    context.Context
	table  string
	id     any
	client pb.PostgresClient
}

func NewDatabase(ctx context.Context, client pb.PostgresClient) Database {
	return Database{
		ctx:    ctx,
		client: client,
	}
}

func (d Database) BeginTx() (isql.Engine, error) {
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

func (d Database) FindOne(document any, filter ...any) (bool, error) {
	var err error
	if err = dberr.CheckIdOrFilterNonEmpty(d.id, filter); err != nil {
		return false, err
	}

	record := new(pb.RecordResponse)

	if filter == nil {
		idStr, _ := d.id.(string)
		record, err = d.client.GetById(d.ctx, &pb.IdParams{
			Table: d.table,
			Id:    idStr,
		})
	} else {
		var af *anypb.Any
		af, err = pkg.ToProtoAny(filter[0])
		if err != nil {
			return false, err
		}

		record, err = d.client.Get(d.ctx, &pb.FilterParams{
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

func (d Database) FindMany(documents any, filter ...any) error {
	af, err := pkg.ToProtoAny(filter)
	if err != nil {
		return err
	}

	records, err := d.client.Find(d.ctx, &pb.FilterParams{
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

func (d Database) InsertOne(document any) (id any, err error) {
	df, err := pkg.ToProtoAny(document)
	if err != nil {
		return nil, err
	}

	record, err := d.client.Create(d.ctx, &pb.CreateParams{
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

func (d Database) InsertMany(documents []any) ([]any, error) {
	var ids []any

	for idx := range documents {
		id, err := d.InsertOne(documents[idx])
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func (d Database) UpdateOne(document any) error {
	if err := dberr.CheckIDNonEmpty(d.id); err != nil {
		return err
	}

	df, err := pkg.ToProtoAny(document)
	if err != nil {
		return err
	}

	record, err := d.client.Update(d.ctx, &pb.UpdateParams{
		Table:  d.table,
		Id:     d.id.(string),
		Record: df,
	})
	if err != nil {
		return err
	}

	return pkg.ParseProtoAnyInto(record.Record, document)
}

func (d Database) DeleteOne(filter ...any) error {
	if err := dberr.CheckIdOrFilterNonEmpty(d.id, filter); err != nil {
		return err
	}

	if filter != nil {
		doc := struct {
			ID string `json:"id"`
		}{}
		found, err := d.FindOne(&doc, filter)
		if err != nil {
			return err
		} else if !found {
			return dberr.DataNotFound
		}
		d.id = doc.ID
	}

	_, err := d.client.Delete(d.ctx, &pb.IdParams{
		Table: d.table,
		Id:    d.id.(string),
	})
	return err
}

func (d Database) Query(query string, args ...any) (*sql.Rows, error) {
	panic("implement me")
}

func (d Database) Exec(query string, args ...any) (sql.Result, error) {
	panic("implement me")
}

func (d Database) Sync(...any) error {
	return dberr.ErrTransactionNotStarted
}

func (d Database) Close() error {
	return nil
}
