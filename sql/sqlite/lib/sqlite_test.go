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

func TestGenerateInsertQuery_allColsIncludesAllFields(t *testing.T) {
	stmt := Statement{}.Table("test_doc").AllCols()
	doc := insertTestDoc{Name: "alice"}

	query := stmt.GenerateInsertQuery(doc)

	assert.Contains(t, query, "id")
	assert.Contains(t, query, "name")
	assert.Contains(t, query, "email")
	assert.Contains(t, query, "score")
}
