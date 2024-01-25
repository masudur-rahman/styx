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
