package server

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"

	"github.com/masudur-rahman/database/pkg"
	"github.com/masudur-rahman/database/sql/postgres/lib"
	"github.com/masudur-rahman/database/sql/postgres/pg-grpc/pb"

	"google.golang.org/grpc"
	health "google.golang.org/grpc/health/grpc_health_v1"
)

type PostgresDB struct {
	conn *sql.Conn
	pb.UnimplementedPostgresServer
}

func NewPostgresDB(conn *sql.Conn) *PostgresDB {
	return &PostgresDB{conn: conn}
}

func (p *PostgresDB) GetById(ctx context.Context, params *pb.IdParams) (*pb.RecordResponse, error) {
	filter := map[string]interface{}{
		"id": params.GetId(),
	}
	query := lib.GenerateReadQuery(params.GetTable(), filter)
	records, err := lib.ExecuteReadQuery(ctx, query, p.conn, 1)
	if err != nil {
		return nil, err
	}

	return lib.MapToRecord(records[0])
}

func (p *PostgresDB) Get(ctx context.Context, params *pb.FilterParams) (*pb.RecordResponse, error) {
	filter, err := pkg.ProtoAnyToMap(params.GetFilter())
	if err != nil {
		return nil, err
	}

	query := lib.GenerateReadQuery(params.GetTable(), filter)
	records, err := lib.ExecuteReadQuery(ctx, query, p.conn, 1)
	if err != nil {
		return nil, err
	}

	return lib.MapToRecord(records[0])
}

func (p *PostgresDB) Find(ctx context.Context, params *pb.FilterParams) (*pb.RecordsResponse, error) {
	filter, err := pkg.ProtoAnyToMap(params.GetFilter())

	query := lib.GenerateReadQuery(params.GetTable(), filter)
	records, err := lib.ExecuteReadQuery(ctx, query, p.conn, -1)
	if err != nil {
		return nil, err
	}

	return lib.MapsToRecords(records)
}

func (p *PostgresDB) Create(ctx context.Context, params *pb.CreateParams) (*pb.RecordResponse, error) {
	record, err := pkg.ProtoAnyToMap(params.GetRecord())
	if err != nil {
		return nil, err
	}

	query := lib.GenerateInsertQuery(params.GetTable(), record)
	_, err = lib.ExecuteWriteQuery(ctx, query, p.conn)
	if err != nil {
		return nil, err
	}

	lid, ok := record["id"].(string)
	if !ok {
		return nil, nil
	}

	return p.GetById(ctx, &pb.IdParams{
		Table: params.GetTable(),
		Id:    lid,
	})
}

func (p *PostgresDB) Update(ctx context.Context, params *pb.UpdateParams) (*pb.RecordResponse, error) {
	record, err := pkg.ProtoAnyToMap(params.GetRecord())
	if err != nil {
		return nil, err
	}

	query := lib.GenerateUpdateQuery(params.GetTable(), params.GetId(), record)
	_, err = lib.ExecuteWriteQuery(ctx, query, p.conn)
	if err != nil {
		return nil, err
	}

	return p.GetById(ctx, &pb.IdParams{
		Table: params.GetTable(),
		Id:    params.GetId(),
	})
}

func (p *PostgresDB) Delete(ctx context.Context, params *pb.IdParams) (*pb.DeleteResponse, error) {
	query := lib.GenerateDeleteQuery(params.GetTable(), params.GetId())
	_, err := lib.ExecuteWriteQuery(ctx, query, p.conn)
	if err != nil {
		return nil, err
	}

	return &pb.DeleteResponse{}, nil
}

func (p *PostgresDB) Query(ctx context.Context, params *pb.QueryParams) (*pb.QueryResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (p *PostgresDB) Exec(ctx context.Context, params *pb.ExecParams) (*pb.ExecResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (p *PostgresDB) Sync(tables ...interface{}) error {
	ctx := context.Background()
	for _, table := range tables {
		if err := lib.SyncTable(ctx, p.conn, table); err != nil {
			return err
		}
	}

	return nil
}

func StartPostgresServer(connConfig lib.PostgresConfig, host string, port int, tables ...interface{}) error {
	server := grpc.NewServer()

	hs := NewHealthChecker()
	health.RegisterHealthServer(server, hs)

	pgConn, err := lib.GetPostgresConnection(connConfig)
	if err != nil {
		return err
	}

	postgres := NewPostgresDB(pgConn)

	if err = postgres.Sync(tables); err != nil {
		return err
	}

	pb.RegisterPostgresServer(server, postgres)

	address := fmt.Sprintf("%s:%v", host, port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	hs.setDatabaseReady()
	log.Printf("gRPC for Postgres server started: %v\n", address)
	return server.Serve(listener)
}
