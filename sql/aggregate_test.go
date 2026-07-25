package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAggregate_Expr(t *testing.T) {
	tests := []struct {
		name string
		agg  Aggregate
		want string
	}{
		{"count no alias", Count("id"), "COUNT(id)"},
		{"count with alias", Count("id").As("total"), "COUNT(id) as total"},
		{"sum with alias", Sum("amount").As("revenue"), "SUM(amount) as revenue"},
		{"avg", Avg("age").As("avg_age"), "AVG(age) as avg_age"},
		{"min", Min("price"), "MIN(price)"},
		{"max", Max("price").As("top"), "MAX(price) as top"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.agg.Expr())
		})
	}
}

func TestAggregate_AsDoesNotMutateOriginal(t *testing.T) {
	base := Count("id")
	aliased := base.As("total")

	assert.Equal(t, "COUNT(id)", base.Expr())
	assert.Equal(t, "COUNT(id) as total", aliased.Expr())
}
