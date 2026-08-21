package lib

import (
	"flag"
	"fmt"
	"reflect"
	"testing"

	"github.com/masudur-rahman/styx/v2/sql/internal/sqltest"

	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "rewrite golden files instead of comparing")

// TestGolden runs the whole corpus against every dialect. The golden files were
// copied verbatim from the per-dialect packages this one replaced, so a passing
// run is proof the merge changed no emitted SQL.
func TestGolden(t *testing.T) {
	for _, d := range []Dialect{Postgres, SQLite} {
		d := d
		t.Run(d.Name(), func(t *testing.T) {
			goldenDDL(t, d)
			goldenSQLTypes(t, d)
			goldenStatements(t, d)
		})
	}
}

// newStatement returns an addressable statement for the given dialect.
func newStatement(d Dialect) *Statement {
	s := NewStatement(d)
	return &s
}

// TestGolden_DDL pins every DDL artefact the library derives from a struct.
func goldenDDL(t *testing.T, d Dialect) {
	t.Helper()
	var out sqltest.Section

	for _, tbl := range sqltest.Tables() {
		fields, err := getTableInfo(d, tbl.Value)
		require.NoError(t, err, tbl.Name)

		out.Add(tbl.Name+"/create", createTableQuery(d, tbl.Name, fields))

		var cols []string
		for _, f := range fields {
			cols = append(cols, fmt.Sprintf("%s %s", f.Name, f.Type))
		}
		out.Add(tbl.Name+"/add-columns", generateAddColumnQuery(tbl.Name, cols))

		out.Addf(tbl.Name+"/pk", "%s", ExtractPKColumn(tbl.Value))
		out.Addf(tbl.Name+"/soft-delete", "%q", ExtractSoftDeleteColumn(tbl.Value))
		out.Addf(tbl.Name+"/unique-groups", "%v", getUniqueColumnGroups(reflect.TypeOf(tbl.Value)))
		out.Addf(tbl.Name+"/indexes", "%+v", extractIndexes(tbl.Value))

		groups := getUniqueColumnGroups(reflect.TypeOf(tbl.Value))
		if len(groups) > 0 {
			out.Add(tbl.Name+"/add-constraint", generateAddConstraintStatement(tbl.Name, groups))
			out.Add(tbl.Name+"/drop-constraint", generateDropConstraintStatement(tbl.Name, groups))
		}
	}

	require.NoError(t, sqltest.Compare(d.Name()+"_ddl", out.String(), *update))
}

// TestGolden_SQLTypes pins the Go-kind to column-type mapping.
func goldenSQLTypes(t *testing.T, d Dialect) {
	t.Helper()
	var out sqltest.Section

	st := reflect.TypeOf(sqltest.Scalars{})
	for i := 0; i < st.NumField(); i++ {
		f := st.Field(i)
		out.Addf(f.Name, "plain=%q autoincr=%q",
			d.SQLType(f.Type, false), d.SQLType(f.Type, true))
	}

	require.NoError(t, sqltest.Compare(d.Name()+"_sqltypes", out.String(), *update))
}

// TestGolden_Statements pins the generated SQL and bound args for every
// builder combination, including placeholder numbering and CTE arg ordering.
func goldenStatements(t *testing.T, d Dialect) {
	t.Helper()
	doc := sqltest.InsertDoc{Name: "alice", Email: "alice@test.com", Score: 7}

	cases := []struct {
		name   string
		render func(*Statement) string
	}{
		{"read/plain", func(s *Statement) string {
			return s.Table("users").GenerateReadQuery(doc)
		}},
		{"read/by-id", func(s *Statement) string {
			return s.Table("users").ID("abc").GenerateReadQuery(doc)
		}},
		{"read/where", func(s *Statement) string {
			return s.Table("users").Where("age > ?", 21).Where("city = ?", "dhaka").GenerateReadQuery(doc)
		}},
		{"read/or", func(s *Statement) string {
			return s.Table("users").Where("age > ?", 21).Or("vip = ?", true).GenerateReadQuery(doc)
		}},
		{"read/in", func(s *Statement) string {
			return s.Table("users").In("status", "new", "active", "banned").GenerateReadQuery(doc)
		}},
		{"read/like", func(s *Statement) string {
			return s.Table("users").Like("name", "a%").NotLike("email", "%spam%").GenerateReadQuery(doc)
		}},
		{"read/columns", func(s *Statement) string {
			return s.Table("users").Columns("id", "name").GenerateReadQuery(doc)
		}},
		{"read/select-distinct", func(s *Statement) string {
			return s.Table("users").Select("count(*) AS n", "city").Distinct().GenerateReadQuery(doc)
		}},
		{"read/order-limit-offset", func(s *Statement) string {
			return s.Table("users").OrderBy("name").OrderBy("age", "DESC").Limit(10).Offset(20).GenerateReadQuery(doc)
		}},
		{"read/paginate", func(s *Statement) string {
			return s.Table("users").Paginate(3, 25).GenerateReadQuery(doc)
		}},
		{"read/group-having", func(s *Statement) string {
			return s.Table("users").GroupBy("city", "role").Having("count(*) > ?", 5).GenerateReadQuery(doc)
		}},
		{"read/joins", func(s *Statement) string {
			return s.Table("users").
				Join("orders", "orders.user_id = users.id").
				LeftJoin("carts", "carts.user_id = users.id AND carts.active = ?", true).
				GenerateReadQuery(doc)
		}},
		{"read/inner-right-join", func(s *Statement) string {
			return s.Table("users").
				InnerJoin("a", "a.id = users.id").
				RightJoin("b", "b.id = users.id").
				GenerateReadQuery(doc)
		}},
		{"read/exists", func(s *Statement) string {
			return s.Table("users").Exists("SELECT 1 FROM orders WHERE user_id = users.id AND total > ?", 100).GenerateReadQuery(doc)
		}},
		{"read/not-exists", func(s *Statement) string {
			return s.Table("users").NotExists("SELECT 1 FROM bans WHERE user_id = users.id AND reason = ?", "spam").GenerateReadQuery(doc)
		}},
		{"read/cte", func(s *Statement) string {
			return s.Table("users").
				With("recent", "SELECT user_id FROM orders WHERE total > ?", []any{100}).
				Where("id IN (SELECT user_id FROM recent) AND city = ?", "dhaka").
				GenerateReadQuery(doc)
		}},
		{"read/two-ctes", func(s *Statement) string {
			return s.Table("users").
				With("a", "SELECT id FROM x WHERE v > ?", []any{1}).
				With("b", "SELECT id FROM y WHERE v < ?", []any{2}).
				Where("city = ?", "dhaka").
				GenerateReadQuery(doc)
		}},
		{"read/soft-delete-filter", func(s *Statement) string {
			return s.Table("users").SoftDeleteCol("deleted_at").GenerateReadQuery(doc)
		}},
		{"read/with-deleted", func(s *Statement) string {
			return s.Table("users").SoftDeleteCol("deleted_at").WithDeleted().GenerateReadQuery(doc)
		}},
		{"count/plain", func(s *Statement) string {
			return s.Table("users").Where("age > ?", 21).GenerateCountQuery()
		}},
		{"insert/partial", func(s *Statement) string {
			return s.Table("users").GenerateInsertQuery(doc)
		}},
		{"insert/all-cols", func(s *Statement) string {
			return s.Table("users").AllCols().GenerateInsertQuery(doc)
		}},
		{"insert/must-cols", func(s *Statement) string {
			return s.Table("users").MustCols("score").GenerateInsertQuery(sqltest.InsertDoc{Name: "alice"})
		}},
		{"insert/bulk", func(s *Statement) string {
			return s.Table("users").GenerateBulkInsertQuery(sqltest.InsertDocs())
		}},
		{"update/partial", func(s *Statement) string {
			return s.Table("users").ID(int64(3)).GenerateUpdateQuery(doc)
		}},
		{"update/where", func(s *Statement) string {
			return s.Table("users").Where("city = ?", "dhaka").GenerateUpdateQuery(doc)
		}},
		{"update/all-cols", func(s *Statement) string {
			return s.Table("users").ID(int64(3)).AllCols().GenerateUpdateQuery(doc)
		}},
		{"delete/by-id", func(s *Statement) string {
			return s.Table("users").ID(int64(3)).GenerateDeleteQuery()
		}},
		{"delete/where", func(s *Statement) string {
			return s.Table("users").Where("city = ?", "dhaka").GenerateDeleteQuery()
		}},
		{"delete/soft", func(s *Statement) string {
			return s.Table("users").ID(int64(3)).SoftDeleteCol("deleted_at").GenerateSoftDeleteQuery()
		}},
		{"delete/restore", func(s *Statement) string {
			return s.Table("users").ID(int64(3)).SoftDeleteCol("deleted_at").GenerateRestoreQuery()
		}},
	}

	var out sqltest.Section
	for _, tc := range cases {
		stmt := newStatement(d)
		query := tc.render(stmt)
		out.Addf(tc.name, "sql:  %s\nargs: %#v", query, stmt.Args())
	}

	require.NoError(t, sqltest.Compare(d.Name()+"_statements", out.String(), *update))
}

// TestGetUniqueColumnGroups_deterministic guards the ordering that constraint
// names are derived from. The groups were once accumulated in a map, so the
// same struct yielded differently named constraints on different runs.
func TestGetUniqueColumnGroups_deterministic(t *testing.T) {
	typ := reflect.TypeOf(sqltest.CompositeUnique{})
	want := getUniqueColumnGroups(typ)
	require.Equal(t, [][]string{{"tenant"}, {"code"}}, want)

	for i := 0; i < 100; i++ {
		require.Equal(t, want, getUniqueColumnGroups(typ), "run %d", i)
	}
}
