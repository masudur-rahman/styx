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
	stmt := Statement{}.Table("test_doc")
	doc := insertTestDoc{Name: "alice", Email: "alice@test.com"}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "name")
	assert.Contains(t, query, "email")
	assert.NotContains(t, query, "score")
	assert.NotContains(t, query, "id")
}

func TestGenerateInsertQuery_mustColsIncludesZeroValues(t *testing.T) {
	stmt := Statement{}.Table("test_doc").MustCols("score")
	doc := insertTestDoc{Name: "alice"}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "name")
	assert.Contains(t, query, "score")
}

type whereTestDoc struct {
	UserID     int64  `db:"user_id"`
	CategoryID string `db:"category_id"`
	Score      int    `db:"score"`
}

func TestGenerateWhereClauseFromFilter_skipsZeroValues(t *testing.T) {
	stmt := Statement{}
	filter := whereTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id")
	assert.NotContains(t, clause, "category_id")
	assert.NotContains(t, clause, "score")
}

func TestGenerateWhereClauseFromFilter_mustFilterColsIncludesZeroString(t *testing.T) {
	stmt := Statement{}.MustFilterCols("category_id")
	filter := whereTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id=99")
	assert.Contains(t, clause, "category_id=''")
	assert.NotContains(t, clause, "score")
}

func TestGenerateWhereClauseFromFilter_mustFilterColsIncludesZeroInt(t *testing.T) {
	stmt := Statement{}.MustFilterCols("score")
	filter := whereTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id=99")
	assert.Contains(t, clause, "score=0")
	assert.NotContains(t, clause, "category_id")
}

type noskipTestDoc struct {
	UserID     int64  `db:"user_id"`
	CategoryID string `db:"category_id,uqs noskip"`
	AlertAt    int64  `db:"alert_at,noskip"`
	Score      int    `db:"score"`
}

func TestGenerateWhereClauseFromFilter_noskipTagIncludesZeroValues(t *testing.T) {
	stmt := Statement{}
	filter := noskipTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id=99")
	assert.Contains(t, clause, "category_id=''")
	assert.Contains(t, clause, "alert_at=0")
	assert.NotContains(t, clause, "score")
}

func TestGenerateInsertQuery_noskipTagIncludesZeroValues(t *testing.T) {
	stmt := Statement{}.Table("noskip_doc")
	doc := noskipTestDoc{UserID: 1}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "user_id")
	assert.Contains(t, query, "category_id")
	assert.Contains(t, query, "alert_at")
	assert.NotContains(t, query, "score")
}

func TestGenerateUpdateQuery_noskipTagIncludesZeroValues(t *testing.T) {
	stmt := Statement{}.Table("noskip_doc").Where("user_id = ?", 1)
	doc := noskipTestDoc{UserID: 1}

	query := stmt.GenerateUpdateQuery(doc)

	assert.Contains(t, query, "category_id = ''")
	assert.Contains(t, query, "alert_at = 0")
	assert.NotContains(t, query, "score")
}

func TestGenerateWhereClauseFromFilter_noNoskipTag_skipsZero(t *testing.T) {
	stmt := Statement{}
	filter := whereTestDoc{UserID: 99}

	clause := stmt.GenerateWhereClauseFromFilter(filter)

	assert.Contains(t, clause, "user_id=99")
	assert.NotContains(t, clause, "category_id")
	assert.NotContains(t, clause, "score")
}

func TestGenerateInsertQuery_allColsIncludesAllFields(t *testing.T) {
	stmt := Statement{}.Table("test_doc").AllCols()
	doc := insertTestDoc{Name: "alice"}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "id")
	assert.Contains(t, query, "name")
	assert.Contains(t, query, "email")
	assert.Contains(t, query, "score")
}
