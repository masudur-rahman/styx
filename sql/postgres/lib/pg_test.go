package lib

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_generateReadQuery(t *testing.T) {
	t.Run("Generate Read Query", func(t *testing.T) {
		tableName := "user"
		params := map[string]interface{}{
			"id":   "abcd",
			"name": "masud",
		}
		query := GenerateReadQuery(tableName, params)
		assert.NotEmpty(t, query)
	})
}

type insertTestDoc struct {
	ID    int64  `db:"id,pk autoincr"`
	Name  string `db:"name"`
	Email string `db:"email"`
	Score int    `db:"score"`
}

func TestGenerateInsertQuery_skipsZeroValues(t *testing.T) {
	stmt := new(Statement).Table("test_doc")
	doc := insertTestDoc{Name: "alice", Email: "alice@test.com"}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "name")
	assert.Contains(t, query, "email")
	assert.NotContains(t, query, "score")
	assert.NotContains(t, query, "id")
	assert.Contains(t, query, "$1")
	assert.Equal(t, []any{"alice", "alice@test.com"}, stmt.args)
}

func TestGenerateInsertQuery_mustColsIncludesZeroValues(t *testing.T) {
	stmt := new(Statement).Table("test_doc").MustCols("score")
	doc := insertTestDoc{Name: "alice"}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "name")
	assert.Contains(t, query, "score")
	assert.Contains(t, query, "$1")
}

func TestGenerateInsertQuery_allColsIncludesAllFields(t *testing.T) {
	stmt := new(Statement).Table("test_doc").AllCols()
	doc := insertTestDoc{Name: "alice"}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "id")
	assert.Contains(t, query, "name")
	assert.Contains(t, query, "email")
	assert.Contains(t, query, "score")
}

type jsonAddress struct {
	Street string `json:"street"`
	City   string `json:"city"`
}

type jsonTestDoc struct {
	ID      int64           `db:"id,pk autoincr"`
	Name    string          `db:"name"`
	Payload json.RawMessage `db:"payload"`
	Address jsonAddress     `db:"address,json"`
	Blob    []byte          `db:"blob"`
}

func TestCreateTableQuery_jsonColumns(t *testing.T) {
	fields, err := getTableInfo(jsonTestDoc{})
	assert.NoError(t, err)

	query := createTableQuery("json_test_doc", fields)

	assert.Contains(t, query, "payload JSONB")
	assert.Contains(t, query, "address JSONB")
	assert.Contains(t, query, "blob BYTEA")
	assert.NotContains(t, query, ", ,", "no field may end up without a SQL type")
}

func TestGenerateInsertQuery_jsonArgsAsText(t *testing.T) {
	stmt := new(Statement).Table("json_test_doc")
	doc := jsonTestDoc{
		Name:    "alice",
		Payload: json.RawMessage(`{"a":1}`),
		Address: jsonAddress{Street: "Road 1", City: "Dhaka"},
	}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "payload")
	assert.Contains(t, query, "address")
	assert.Equal(t, []any{"alice", `{"a":1}`, `{"street":"Road 1","city":"Dhaka"}`}, stmt.args)
}

func TestGenerateUpdateQuery_jsonArgsAsText(t *testing.T) {
	stmt := new(Statement).Table("json_test_doc").Where("id = ?", 7)
	doc := jsonTestDoc{Payload: json.RawMessage(`{"b":2}`)}

	query := stmt.GenerateUpdateQuery(doc)

	assert.Contains(t, query, "payload = $1")
	assert.Equal(t, []any{`{"b":2}`, 7}, stmt.args)
}
