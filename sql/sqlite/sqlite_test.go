package sqlite

import (
	"context"
	"database/sql"
	"github.com/golang/mock/gomock"
	"github.com/masudur-rahman/database/sql/sqlite/lib"
	"github.com/masudur-rahman/pawsitively-purrfect/models"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

type User struct {
	ID    string `db:"id,pk"`
	Name  string
	Email string `db:"email,uq"`
}

func initialize() error {
	conn, err := lib.GetSQLiteConnection("test.db")
	if err != nil {
		return err
	}

	db := NewSqlite(context.Background(), conn)
	return db.Sync(User{})
}

func TestSqlite_InsertOne(t *testing.T) {
	db := initialize()

	t.Run("should create pet", func(t *testing.T) {
		id := "abc-xyz"
		pet := models.Pet{
			ID:     id,
			Name:   "Cathy",
			Gender: "Male",
		}

		gomock.InOrder(
			db.EXPECT().InsertOne(gomock.Any()).Return(id, nil),
		)

		err := pr.Save(&pet)
		assert.NoError(t, err)
		assert.Equal(t, id, pet.XKey)
	})
}

func TestSqlite_InsertOne2(t *testing.T) {
	type fields struct {
		ctx       context.Context
		conn      *sql.Conn
		statement lib.Statement
	}
	type args struct {
		document any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantId  any
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg := Sqlite{
				ctx:       tt.fields.ctx,
				conn:      tt.fields.conn,
				statement: tt.fields.statement,
			}
			gotId, err := pg.InsertOne(tt.args.document)
			if (err != nil) != tt.wantErr {
				t.Errorf("InsertOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotId, tt.wantId) {
				t.Errorf("InsertOne() gotId = %v, want %v", gotId, tt.wantId)
			}
		})
	}
}
