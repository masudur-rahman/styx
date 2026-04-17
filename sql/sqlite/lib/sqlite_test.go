package lib

import (
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
	assert.Contains(t, query, "?")
	assert.Equal(t, []any{"alice", "alice@test.com"}, stmt.args)
}

func TestGenerateInsertQuery_mustColsIncludesZeroValues(t *testing.T) {
	stmt := new(Statement).Table("test_doc").MustCols("score")
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

func TestGenerateWhereClauseFromFilter_skipsZeroValues(t *testing.T) {
	stmt := new(Statement)
	filter := whereTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id")
	assert.NotContains(t, clause, "category_id")
	assert.NotContains(t, clause, "score")
	assert.Equal(t, []any{int64(99)}, stmt.args)
}

func TestGenerateWhereClauseFromFilter_mustFilterColsIncludesZeroString(t *testing.T) {
	stmt := new(Statement).MustFilterCols("category_id")
	filter := whereTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id = ?")
	assert.Contains(t, clause, "category_id = ?")
	assert.NotContains(t, clause, "score")
	assert.Equal(t, []any{int64(99), ""}, stmt.args)
}

func TestGenerateWhereClauseFromFilter_mustFilterColsIncludesZeroInt(t *testing.T) {
	stmt := new(Statement).MustFilterCols("score")
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

func TestGenerateWhereClauseFromFilter_reqTagIncludesZeroValues(t *testing.T) {
	stmt := new(Statement)
	filter := reqTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id = ?")
	assert.Contains(t, clause, "category_id = ?")
	assert.Contains(t, clause, "alert_at = ?")
	assert.NotContains(t, clause, "score")
	assert.Equal(t, []any{int64(99), "", int64(0)}, stmt.args)
}

func TestGenerateInsertQuery_reqTagIncludesZeroValues(t *testing.T) {
	stmt := new(Statement).Table("req_doc")
	doc := reqTestDoc{UserID: 1}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "user_id")
	assert.Contains(t, query, "category_id")
	assert.Contains(t, query, "alert_at")
	assert.NotContains(t, query, "score")
	assert.Contains(t, query, "?")
}

func TestGenerateUpdateQuery_reqTagIncludesZeroValues(t *testing.T) {
	stmt := new(Statement).Table("req_doc").Where("user_id = ?", 1)
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

func TestGenerateWhereClauseFromFilter_noReqTag_skipsZero(t *testing.T) {
	stmt := new(Statement)
	filter := whereTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id = ?")
	assert.NotContains(t, clause, "category_id")
	assert.NotContains(t, clause, "score")
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
