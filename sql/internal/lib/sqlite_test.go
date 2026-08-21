package lib

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateInsertQuery_skipsZeroValues_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).Table("test_doc")
	doc := insertTestDoc{Name: "alice", Email: "alice@test.com"}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "name")
	assert.Contains(t, query, "email")
	assert.NotContains(t, query, "score")
	assert.NotContains(t, query, "id")
	assert.Contains(t, query, "?")
	assert.Equal(t, []any{"alice", "alice@test.com"}, stmt.args)
}

func TestGenerateInsertQuery_mustColsIncludesZeroValues_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).Table("test_doc").MustCols("score")
	doc := insertTestDoc{Name: "alice"}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "name")
	assert.Contains(t, query, "score")
	assert.Contains(t, query, "?")
}

type whereTestDoc struct {
	UserID     int64  `db:"user_id"`
	CategoryID string `db:"category_id"`
	Score      int    `db:"score"`
}

func TestGenerateWhereClauseFromFilter_skipsZeroValues_sqlite(t *testing.T) {
	stmt := newStatement(SQLite)
	filter := whereTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id")
	assert.NotContains(t, clause, "category_id")
	assert.NotContains(t, clause, "score")
	assert.Equal(t, []any{int64(99)}, stmt.args)
}

func TestGenerateWhereClauseFromFilter_mustFilterColsIncludesZeroString_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).MustFilterCols("category_id")
	filter := whereTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id = ?")
	assert.Contains(t, clause, "category_id = ?")
	assert.NotContains(t, clause, "score")
	assert.Equal(t, []any{int64(99), ""}, stmt.args)
}

func TestGenerateWhereClauseFromFilter_mustFilterColsIncludesZeroInt_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).MustFilterCols("score")
	filter := whereTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id = ?")
	assert.Contains(t, clause, "score = ?")
	assert.NotContains(t, clause, "category_id")
	assert.Equal(t, []any{int64(99), 0}, stmt.args)
}

type reqTestDoc struct {
	UserID     int64  `db:"user_id"`
	CategoryID string `db:"category_id,uqs req"`
	AlertAt    int64  `db:"alert_at,req"`
	Score      int    `db:"score"`
}

func TestGenerateWhereClauseFromFilter_reqTagIncludesZeroValues_sqlite(t *testing.T) {
	stmt := newStatement(SQLite)
	filter := reqTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id = ?")
	assert.Contains(t, clause, "category_id = ?")
	assert.Contains(t, clause, "alert_at = ?")
	assert.NotContains(t, clause, "score")
	assert.Equal(t, []any{int64(99), "", int64(0)}, stmt.args)
}

func TestGenerateInsertQuery_reqTagIncludesZeroValues_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).Table("req_doc")
	doc := reqTestDoc{UserID: 1}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "user_id")
	assert.Contains(t, query, "category_id")
	assert.Contains(t, query, "alert_at")
	assert.NotContains(t, query, "score")
	assert.Contains(t, query, "?")
}

func TestGenerateUpdateQuery_reqTagIncludesZeroValues_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).Table("req_doc").Where("user_id = ?", 1)
	doc := reqTestDoc{UserID: 1}

	query := stmt.GenerateUpdateQuery(doc)

	assert.Contains(t, query, "category_id = ?")
	assert.Contains(t, query, "alert_at = ?")
	assert.NotContains(t, query, "score")
	// SET args come before WHERE args in driver call
	assert.Equal(t, int64(1), stmt.args[0])         // user_id SET value
	assert.Equal(t, "", stmt.args[1])               // category_id SET value
	assert.Equal(t, int64(0), stmt.args[2])         // alert_at SET value
	assert.Equal(t, 1, stmt.args[len(stmt.args)-1]) // WHERE arg last
}

func TestGenerateWhereClauseFromFilter_noReqTag_skipsZero_sqlite(t *testing.T) {
	stmt := newStatement(SQLite)
	filter := whereTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id = ?")
	assert.NotContains(t, clause, "category_id")
	assert.NotContains(t, clause, "score")
}

func TestGenerateInsertQuery_allColsIncludesAllFields_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).Table("test_doc").AllCols()
	doc := insertTestDoc{Name: "alice"}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "id")
	assert.Contains(t, query, "name")
	assert.Contains(t, query, "email")
	assert.Contains(t, query, "score")
}

func TestGenerateBulkInsertQuery_singleStatement_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).Table("test_doc")
	docs := []any{
		insertTestDoc{Name: "alice", Email: "alice@test.com"},
		insertTestDoc{Name: "bob", Email: "bob@test.com"},
	}

	query := stmt.GenerateBulkInsertQuery(docs)

	assert.Contains(t, query, "INSERT INTO \"test_doc\"")
	assert.Contains(t, query, "name")
	assert.Contains(t, query, "email")
	// one statement, two value groups
	assert.Equal(t, 1, strings.Count(query, "INSERT INTO"))
	assert.Contains(t, query, "(?, ?), (?, ?)")
	assert.Equal(t, []any{"alice", "alice@test.com", "bob", "bob@test.com"}, stmt.args)
}

func TestGenerateBulkInsertQuery_columnUnionAcrossDocs_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).Table("test_doc")
	// score is zero in first doc but set in second → must be included for both rows
	docs := []any{
		insertTestDoc{Name: "alice"},
		insertTestDoc{Name: "bob", Score: 7},
	}

	query := stmt.GenerateBulkInsertQuery(docs)

	assert.Contains(t, query, "score")
	assert.Equal(t, []any{"alice", 0, "bob", 7}, stmt.args)
}

func TestCreateTableQuery_jsonColumns_sqlite(t *testing.T) {
	fields, err := getTableInfo(SQLite, jsonTestDoc{})
	assert.NoError(t, err)

	query := createTableQuery(SQLite, "json_test_doc", fields)

	assert.Contains(t, query, "payload TEXT")
	assert.Contains(t, query, "address TEXT")
	assert.Contains(t, query, "blob BLOB")
	assert.NotContains(t, query, ", ,", "no field may end up without a SQL type")
}

func TestGenerateInsertQuery_jsonArgsAsText_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).Table("json_test_doc")
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

func TestWith_sqlitePrependsCTEArgs_sqlite(t *testing.T) {
	sub := newStatement(SQLite).Table("orders").Columns("user_id")
	sub.Where("total > ?", 1000)
	subSQL := sub.GenerateReadQuery(nil)
	subArgs := sub.Args()

	outer := newStatement(SQLite).Table("users")
	outer.With("big", subSQL, subArgs)
	outer.Where("active = ?", true)
	q := outer.GenerateReadQuery(nil)

	assert.Contains(t, q, `WITH big AS (SELECT user_id FROM "orders" WHERE total > ?)`)
	assert.Contains(t, q, `SELECT * FROM "users" WHERE active = ?`)
	assert.Equal(t, []any{1000, true}, outer.Args())
}

func TestGenerateCountQuery_sqliteWhere_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).Table("account").Where("age > ?", 18)

	q := stmt.GenerateCountQuery()

	assert.Contains(t, q, `SELECT COUNT(*) FROM "account"`)
	assert.Contains(t, q, "WHERE")
	assert.Contains(t, q, "age > ?")
	assert.Equal(t, 18, stmt.args[len(stmt.args)-1])
}

func TestGenerateCountQuery_sqliteExcludesSoftDeleted_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).Table("account")
	stmt.SoftDeleteCol("deleted_at")

	q := stmt.GenerateCountQuery()

	assert.Contains(t, q, `SELECT COUNT(*) FROM "account"`)
	assert.Contains(t, q, "deleted_at IS NULL")
}

func TestGenerateCountQuery_sqliteWithDeletedIncludesAll_sqlite(t *testing.T) {
	stmt := newStatement(SQLite).Table("account")
	stmt.SoftDeleteCol("deleted_at")
	stmt.WithDeleted()

	q := stmt.GenerateCountQuery()

	assert.NotContains(t, q, "deleted_at IS NULL")
}

func TestCreateTableQuery_notNull_sqlite(t *testing.T) {
	type notNullDoc struct {
		ID   int64  `db:"id,pk autoincr"`
		Name string `db:"name,notnull"`
		Note string `db:"note"`
	}
	fields, err := getTableInfo(SQLite, notNullDoc{})
	assert.NoError(t, err)

	query := createTableQuery(SQLite, "not_null_doc", fields)

	assert.Contains(t, query, "name TEXT NOT NULL")
	assert.NotContains(t, query, "note TEXT NOT NULL")
}
