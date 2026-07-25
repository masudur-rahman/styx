package sql

import "fmt"

// Aggregate is a SQL aggregate expression (COUNT, SUM, AVG, MIN, MAX) used to
// build aggregate columns in a SELECT. Construct one with the package-level
// Count, Sum, Avg, Min or Max functions and set an optional column alias with
// As, then pass it to Engine.Select:
//
//	db.Table("orders").
//	    Select(Count("id").As("total"), Sum("amount").As("revenue")).
//	    GroupBy("status").
//	    FindMany(ctx, &rows)
//
// This differs from the terminal Engine.Count(ctx, ...), which executes and
// returns a plain row count.
type Aggregate struct {
	fn    string
	col   string
	alias string
}

// Count builds a COUNT(col) aggregate expression.
func Count(col string) Aggregate { return Aggregate{fn: "COUNT", col: col} }

// Sum builds a SUM(col) aggregate expression.
func Sum(col string) Aggregate { return Aggregate{fn: "SUM", col: col} }

// Avg builds an AVG(col) aggregate expression.
func Avg(col string) Aggregate { return Aggregate{fn: "AVG", col: col} }

// Min builds a MIN(col) aggregate expression.
func Min(col string) Aggregate { return Aggregate{fn: "MIN", col: col} }

// Max builds a MAX(col) aggregate expression.
func Max(col string) Aggregate { return Aggregate{fn: "MAX", col: col} }

// As sets the column alias for the aggregate expression and returns the updated
// value (Aggregate is used by value, so the original is left unchanged).
func (a Aggregate) As(alias string) Aggregate {
	a.alias = alias
	return a
}

// Expr renders the aggregate as a SQL select expression, e.g. "COUNT(id) as total".
func (a Aggregate) Expr() string {
	expr := fmt.Sprintf("%s(%s)", a.fn, a.col)
	if a.alias != "" {
		expr += " as " + a.alias
	}
	return expr
}
