package lib

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/masudur-rahman/styx/v2/dberr"
	isql "github.com/masudur-rahman/styx/v2/sql"
	core "github.com/masudur-rahman/styx/v2/sql/internal/core"
)

// observeQuery reports a completed statement to obs when one is configured.
func observeQuery(ctx context.Context, obs isql.Observer, query string, args []any, start time.Time, err error) {
	if obs != nil {
		obs.OnQuery(ctx, query, args, time.Since(start), err)
	}
}

// Statement accumulates the pieces of one SQL statement. It is dialect-aware:
// every placeholder it emits comes from the Dialect it was created with.
type Statement struct {
	dialect          Dialect
	table            string
	id               any
	columns          []string
	allCols          bool
	mustCols         []string
	mustColMap       map[string]bool
	mustFilterCols   []string
	mustFilterColMap map[string]bool
	where            string
	args             []any
	argCounter       int
	showSQL          bool
	pkColumn         string
	orderBy          []string
	limit            int64
	offset           int64
	groupBy          []string
	having           string
	distinct         bool
	aggregates       []string
	softDeleteCol    string
	withDeleted      bool
	forceDelete      bool
	validate         bool
	joins            []string
	preloads         []string
	ctes             []cteClause
}

// cteClause is a single named Common Table Expression (WITH name AS (sql)).
//
// Under numbered placeholders the args are merged into the statement at
// registration time and args stays nil, because the body was renumbered to
// match. Under positional placeholders they are held here and spliced ahead of
// the main body args when the query is assembled.
type cteClause struct {
	name string
	sql  string
	args []any
}

// placeholderRe matches Postgres positional placeholders ($1, $2, ...).
var placeholderRe = regexp.MustCompile(`\$(\d+)`)

// NewStatement returns a statement that emits SQL for the given dialect.
func NewStatement(d Dialect) Statement {
	return Statement{dialect: d}
}

// Dialect returns the dialect this statement emits SQL for.
func (stmt *Statement) Dialect() Dialect {
	if stmt.dialect == nil {
		return Postgres
	}
	return stmt.dialect
}

// nextPlaceholder consumes one bound-argument slot and returns its marker.
func (stmt *Statement) nextPlaceholder() string {
	stmt.argCounter++
	return stmt.Dialect().Placeholder(stmt.argCounter)
}

// bindCond rewrites the leading n "?" markers in cond into the dialect's
// placeholder form. Dialects whose placeholders are positional already use "?"
// and get the condition back untouched.
func (stmt *Statement) bindCond(cond string, n int) string {
	if !stmt.Dialect().NumberedArgs() {
		return cond
	}
	for i := 0; i < n; i++ {
		cond = strings.Replace(cond, "?", stmt.nextPlaceholder(), 1)
	}
	return cond
}

// placeholders returns n markers, consuming a slot for each.
func (stmt *Statement) placeholders(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = stmt.nextPlaceholder()
	}
	return out
}

// Preload registers an association name to be eager-loaded after the read.
func (stmt *Statement) Preload(assoc string) *Statement {
	stmt.preloads = append(stmt.preloads, assoc)
	return stmt
}

// Preloads returns the registered preload association names.
func (stmt *Statement) Preloads() []string {
	return stmt.preloads
}

// Args returns the accumulated positional arguments for the current statement.
// Used to compile a statement into a subquery for CTE composition.
func (stmt *Statement) Args() []any {
	return stmt.args
}

// With registers a named Common Table Expression whose body is the already
// compiled subSQL with its subArgs. Because Postgres placeholders reference args
// by number, the sub's $1..$k are shifted by the current arg count and its args
// are appended, keeping placeholder numbers aligned with arg positions.
func (stmt *Statement) With(name, subSQL string, subArgs []any) *Statement {
	if !stmt.Dialect().NumberedArgs() {
		stmt.ctes = append(stmt.ctes, cteClause{name: name, sql: subSQL, args: subArgs})
		return stmt
	}

	renumbered := renumberPlaceholders(subSQL, stmt.argCounter)
	stmt.argCounter += len(subArgs)
	stmt.args = append(stmt.args, subArgs...)
	stmt.ctes = append(stmt.ctes, cteClause{name: name, sql: renumbered})
	return stmt
}

// renumberPlaceholders shifts every $N placeholder in q up by offset.
func renumberPlaceholders(q string, offset int) string {
	if offset == 0 {
		return q
	}
	return placeholderRe.ReplaceAllStringFunc(q, func(m string) string {
		n, _ := strconv.Atoi(m[1:])
		return "$" + strconv.Itoa(n+offset)
	})
}

func (stmt *Statement) Table(name string) *Statement {
	stmt.table = name
	return stmt
}

func (stmt *Statement) ID(id any) *Statement {
	if stmt.where != "" {
		stmt.where += " AND "
	}

	stmt.id = id
	return stmt
}

func (stmt *Statement) In(col string, values ...any) *Statement {
	if stmt.where != "" {
		stmt.where += " AND "
	}

	placeholders := stmt.placeholders(len(values))
	stmt.args = append(stmt.args, values...)
	stmt.where += fmt.Sprintf("%s IN (%s)", col, strings.Join(placeholders, ", "))
	return stmt
}

func (stmt *Statement) Where(cond string, args ...any) *Statement {
	cond = stmt.bindCond(cond, len(args))
	stmt.where = stmt.AddWhereClause(cond)
	if len(args) > 0 {
		// Create a new slice to avoid sharing underlying array
		newArgs := make([]any, len(args))
		copy(newArgs, args)
		stmt.args = append(stmt.args, newArgs...)
	}
	return stmt
}

func (stmt *Statement) generateWhereClauseFromID() string {
	if core.IsZeroValue(stmt.id) {
		return ""
	}
	stmt.args = append(stmt.args, stmt.id)
	return "id = " + stmt.nextPlaceholder()
}

func (stmt *Statement) GenerateWhereClauseFromFilter(filter any) string {
	stmt.mustFilterColMap = stmt.generateMustFilterColMap()
	var conditions []string

	val := reflect.ValueOf(filter)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	for _, f := range core.WalkFields(val.Type()) {
		col := core.GetFieldName(f.StructField)
		fv := f.Value(val)

		if !(stmt.allCols || stmt.mustFilterColMap[col] || core.HasReqTag(f.StructField) || !fv.IsZero()) {
			continue
		}

		conditions = append(conditions, col+" = "+stmt.nextPlaceholder())
		stmt.args = append(stmt.args, core.SQLArgValue(f.StructField, fv))
	}

	return strings.Join(conditions, " AND ")
}

func (stmt *Statement) GenerateWhereClause(filter ...any) *Statement {
	stmt.where = stmt.AddWhereClause(stmt.generateWhereClauseFromID())
	if len(filter) > 0 {
		// Noted here so a single-row delete or restore can order by it; the
		// filter is the only place those statements see the document type.
		if stmt.pkColumn == "" {
			stmt.pkColumn, _ = core.DeclaredPKColumn(filter[0])
		}
		stmt.where = stmt.AddWhereClause(stmt.GenerateWhereClauseFromFilter(filter[0]))
	}
	return stmt
}

func (stmt *Statement) CheckWhereClauseNotEmpty() error {
	if stmt.where == "" {
		return dberr.ErrMissingWhereClause
	}
	return nil
}

func (stmt *Statement) AddWhereClause(cond string) string {
	if stmt.where != "" && cond != "" {
		stmt.where += " AND "
	}

	stmt.where += cond
	return stmt.where
}

func (stmt *Statement) Columns(cols ...string) *Statement {
	stmt.columns = cols
	return stmt
}

func (stmt *Statement) AllCols() *Statement {
	stmt.allCols = true
	return stmt
}

func (stmt *Statement) MustCols(cols ...string) *Statement {
	stmt.mustCols = cols
	return stmt
}

// MustFilterCols marks columns that must be included in WHERE clauses even when zero-valued.
func (stmt *Statement) MustFilterCols(cols ...string) *Statement {
	stmt.mustFilterCols = cols
	return stmt
}

func (stmt *Statement) ShowSQL(showSQL bool) *Statement {
	stmt.showSQL = showSQL
	return stmt
}

// PKColumn sets the primary key column name for RETURNING clause in INSERT queries.
func (stmt *Statement) PKColumn(col string) *Statement {
	stmt.pkColumn = col
	return stmt
}

// OrderBy adds an ORDER BY clause. Default direction is ASC.
func (stmt *Statement) OrderBy(col string, direction ...string) *Statement {
	dir := "ASC"
	if len(direction) > 0 && strings.ToUpper(direction[0]) == "DESC" {
		dir = "DESC"
	}
	stmt.orderBy = append(stmt.orderBy, fmt.Sprintf("%s %s", col, dir))
	return stmt
}

// Limit sets the maximum number of rows to return.
func (stmt *Statement) Limit(n int64) *Statement {
	stmt.limit = n
	return stmt
}

// Offset sets the number of rows to skip.
func (stmt *Statement) Offset(n int64) *Statement {
	stmt.offset = n
	return stmt
}

// Distinct enables SELECT DISTINCT.
func (stmt *Statement) Distinct() *Statement {
	stmt.distinct = true
	return stmt
}

// GroupBy adds GROUP BY columns.
func (stmt *Statement) GroupBy(cols ...string) *Statement {
	stmt.groupBy = append(stmt.groupBy, cols...)
	return stmt
}

// Having sets the HAVING clause for GROUP BY filtering.
func (stmt *Statement) Having(cond string, args ...any) *Statement {
	cond = stmt.bindCond(cond, len(args))
	stmt.having = cond
	if len(args) > 0 {
		newArgs := make([]any, len(args))
		copy(newArgs, args)
		stmt.args = append(stmt.args, newArgs...)
	}
	return stmt
}

// Or adds an OR condition to the WHERE clause.
func (stmt *Statement) Or(cond string, args ...any) *Statement {
	cond = stmt.bindCond(cond, len(args))
	if stmt.where != "" {
		stmt.where += " OR " + cond
	} else {
		stmt.where = cond
	}
	if len(args) > 0 {
		newArgs := make([]any, len(args))
		copy(newArgs, args)
		stmt.args = append(stmt.args, newArgs...)
	}
	return stmt
}

// Like adds a LIKE condition to the WHERE clause.
func (stmt *Statement) Like(col string, pattern string) *Statement {
	stmt.where = stmt.AddWhereClause(col + " LIKE " + stmt.nextPlaceholder())
	stmt.args = append(stmt.args, pattern)
	return stmt
}

// NotLike adds a NOT LIKE condition to the WHERE clause.
func (stmt *Statement) NotLike(col string, pattern string) *Statement {
	stmt.where = stmt.AddWhereClause(col + " NOT LIKE " + stmt.nextPlaceholder())
	stmt.args = append(stmt.args, pattern)
	return stmt
}

// Exists adds an EXISTS subquery condition to the WHERE clause.
func (stmt *Statement) Exists(subquery string, args ...any) *Statement {
	subquery = stmt.bindCond(subquery, len(args))
	cond := fmt.Sprintf("EXISTS (%s)", subquery)
	stmt.where = stmt.AddWhereClause(cond)
	if len(args) > 0 {
		newArgs := make([]any, len(args))
		copy(newArgs, args)
		stmt.args = append(stmt.args, newArgs...)
	}
	return stmt
}

// NotExists adds a NOT EXISTS subquery condition to the WHERE clause.
func (stmt *Statement) NotExists(subquery string, args ...any) *Statement {
	subquery = stmt.bindCond(subquery, len(args))
	cond := fmt.Sprintf("NOT EXISTS (%s)", subquery)
	stmt.where = stmt.AddWhereClause(cond)
	if len(args) > 0 {
		newArgs := make([]any, len(args))
		copy(newArgs, args)
		stmt.args = append(stmt.args, newArgs...)
	}
	return stmt
}

// Select appends aggregate expressions (already rendered as SQL) to the SELECT
// clause.
func (stmt *Statement) Select(exprs ...string) *Statement {
	stmt.aggregates = append(stmt.aggregates, exprs...)
	return stmt
}

// Paginate sets LIMIT and OFFSET for page-based pagination.
func (stmt *Statement) Paginate(page, perPage int64) *Statement {
	if perPage <= 0 {
		perPage = 20
	}
	if page <= 0 {
		page = 1
	}
	stmt.limit = perPage
	stmt.offset = (page - 1) * perPage
	return stmt
}

// EnableValidation enables or disables struct validation before writes.
func (stmt *Statement) EnableValidation(enable bool) *Statement {
	stmt.validate = enable
	return stmt
}

// ShouldValidate returns true if validation is enabled.
func (stmt *Statement) ShouldValidate() bool {
	return stmt.validate
}

// Join adds a JOIN clause.
func (stmt *Statement) Join(table string, on string, args ...any) *Statement {
	return stmt.addJoin("JOIN", table, on, args...)
}

// LeftJoin adds a LEFT JOIN clause.
func (stmt *Statement) LeftJoin(table string, on string, args ...any) *Statement {
	return stmt.addJoin("LEFT JOIN", table, on, args...)
}

// RightJoin adds a RIGHT JOIN clause.
func (stmt *Statement) RightJoin(table string, on string, args ...any) *Statement {
	return stmt.addJoin("RIGHT JOIN", table, on, args...)
}

// InnerJoin adds an INNER JOIN clause.
func (stmt *Statement) InnerJoin(table string, on string, args ...any) *Statement {
	return stmt.addJoin("INNER JOIN", table, on, args...)
}

func (stmt *Statement) addJoin(joinType, table, on string, args ...any) *Statement {
	on = stmt.bindCond(on, len(args))
	stmt.joins = append(stmt.joins, fmt.Sprintf("%s \"%s\" ON %s", joinType, table, on))
	if len(args) > 0 {
		newArgs := make([]any, len(args))
		copy(newArgs, args)
		stmt.args = append(stmt.args, newArgs...)
	}
	return stmt
}

// SoftDeleteCol sets the soft delete column name for the current query.
func (stmt *Statement) SoftDeleteCol(col string) *Statement {
	stmt.softDeleteCol = col
	return stmt
}

// WithDeleted disables the automatic soft delete filter.
func (stmt *Statement) WithDeleted() *Statement {
	stmt.withDeleted = true
	return stmt
}

// SetForceDelete marks the next delete as a hard delete even with soft delete enabled.
func (stmt *Statement) SetForceDelete() *Statement {
	stmt.forceDelete = true
	return stmt
}

// IsSoftDelete returns true if soft delete is enabled and not force-deleting.
// SoftDeleteColumn returns the column marking a row deleted: the one taken from
// a document, else the one Sync registered against the table name.
//
// The fallback matters for statements never handed a document — DeleteOne by
// ID(), Restore, Count — which would otherwise treat a soft-delete table as an
// ordinary one and delete the row outright.
func (stmt *Statement) SoftDeleteColumn() string {
	if stmt.softDeleteCol != "" {
		return stmt.softDeleteCol
	}
	return core.SoftDeleteColumnForTable(stmt.table)
}

func (stmt *Statement) IsSoftDelete() bool {
	return stmt.SoftDeleteColumn() != "" && !stmt.forceDelete
}

// The values the soft-delete column is moved between.
const (
	softDeletedValue = "CURRENT_TIMESTAMP"
	softLiveValue    = "NULL"
)

// GenerateSoftDeleteQuery generates an UPDATE that marks at most one row deleted.
func (stmt *Statement) GenerateSoftDeleteQuery() string {
	return stmt.softDeleteQuery(true, true)
}

// GenerateSoftDeleteManyQuery generates an UPDATE that marks every matching row
// deleted.
func (stmt *Statement) GenerateSoftDeleteManyQuery() string {
	return stmt.softDeleteQuery(true, false)
}

// GenerateRestoreQuery generates an UPDATE that clears the soft delete column
// on at most one row, the inverse of GenerateSoftDeleteQuery.
func (stmt *Statement) GenerateRestoreQuery() string {
	return stmt.softDeleteQuery(false, true)
}

// softDeleteQuery moves rows to the other side of the soft-delete marker.
//
// Rows already on the destination side are excluded, which matters most under a
// single-row cap: a filter matching one deleted row and two live ones would
// otherwise re-stamp the deleted one and report success, leaving both live rows
// in place. It also keeps the affected-row count honest, so a delete that
// changed nothing reports ErrNotFound rather than a silent no-op.
func (stmt *Statement) softDeleteQuery(markDeleted, limitOne bool) string {
	col := stmt.SoftDeleteColumn()

	value, guard := softDeletedValue, col+" IS NULL"
	if !markDeleted {
		value, guard = softLiveValue, col+" IS NOT NULL"
	}
	stmt.where = stmt.AddWhereClause(guard)

	return fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s",
		quoteIdent(stmt.table), col, value, stmt.rowLimit(limitOne))
}

// rowLimit returns the statement's WHERE condition, narrowed to a single row
// when limitOne is set.
//
// Neither Postgres nor the SQLite build styx links accepts UPDATE ... LIMIT, so
// the row is chosen by a subquery instead. It selects the primary key where the
// struct declares one: the key both identifies the row stably and orders the
// candidates, so the same call settles on the same row rather than whichever
// the planner reached first.
//
// Without a declared key the subquery falls back to the dialect's physical row
// identifier. That is a weaker choice on Postgres, where an UPDATE rewrites the
// ctid it just selected, so a row updated concurrently in between leaves the
// outer statement matching nothing at all.
//
// An ID() lookup already names a single row, so its condition is left alone.
func (stmt *Statement) rowLimit(limitOne bool) string {
	if !limitOne || stmt.id != nil || stmt.where == "" {
		return stmt.where
	}

	rowID, orderBy := stmt.pkColumn, ""
	if rowID == "" {
		rowID = stmt.Dialect().RowIDExpr()
	} else {
		orderBy = " ORDER BY " + rowID
	}

	return fmt.Sprintf("%s IN (SELECT %s FROM %s WHERE %s%s LIMIT 1)",
		rowID, rowID, quoteIdent(stmt.table), stmt.where, orderBy)
}

// buildCTEPrefix emits the "WITH n1 AS (sql1), n2 AS (sql2) " prefix.
//
// Under numbered placeholders the CTE args were already merged into stmt.args
// by With, so only the SQL text is assembled here. Under positional
// placeholders they are spliced ahead of the main body args, because the CTE
// bodies appear first in the final SQL. Returns "" when no CTEs are registered.
func (stmt *Statement) buildCTEPrefix() string {
	if len(stmt.ctes) == 0 {
		return ""
	}

	parts := make([]string, len(stmt.ctes))
	var cteArgs []any
	for i, c := range stmt.ctes {
		parts[i] = fmt.Sprintf("%s AS (%s)", c.name, c.sql)
		cteArgs = append(cteArgs, c.args...)
	}
	if len(cteArgs) > 0 {
		stmt.args = append(cteArgs, stmt.args...)
	}

	return "WITH " + strings.Join(parts, ", ") + " "
}

// GenerateReadQuery builds a SELECT query from the current statement state.

func (stmt *Statement) GenerateReadQuery(doc any) string {
	var colParts []string
	if len(stmt.aggregates) > 0 {
		colParts = append(colParts, stmt.aggregates...)
	}
	if len(stmt.columns) > 0 && !stmt.allCols {
		colParts = append(colParts, stmt.columns...)
	}
	if len(colParts) == 0 {
		colParts = []string{"*"}
	}

	if stmt.table == "" {
		stmt.table = core.GetTableName(doc)
	}

	selectKeyword := "SELECT"
	if stmt.distinct {
		selectKeyword = "SELECT DISTINCT"
	}

	var b strings.Builder
	if prefix := stmt.buildCTEPrefix(); prefix != "" {
		b.WriteString(prefix)
	}
	fmt.Fprintf(&b, "%s %s FROM \"%s\"", selectKeyword, strings.Join(colParts, ", "), stmt.table)

	for _, join := range stmt.joins {
		b.WriteString(" ")
		b.WriteString(join)
	}

	if softCol := stmt.SoftDeleteColumn(); softCol != "" && !stmt.withDeleted {
		stmt.where = stmt.AddWhereClause(softCol + " IS NULL")
	}
	if stmt.where != "" {
		b.WriteString(" WHERE ")
		b.WriteString(stmt.where)
	}
	if len(stmt.groupBy) > 0 {
		b.WriteString(" GROUP BY ")
		b.WriteString(strings.Join(stmt.groupBy, ", "))
	}
	if stmt.having != "" {
		b.WriteString(" HAVING ")
		b.WriteString(stmt.having)
	}
	if len(stmt.orderBy) > 0 {
		b.WriteString(" ORDER BY ")
		b.WriteString(strings.Join(stmt.orderBy, ", "))
	}
	if stmt.limit > 0 {
		fmt.Fprintf(&b, " LIMIT %d", stmt.limit)
	}
	if stmt.offset > 0 {
		fmt.Fprintf(&b, " OFFSET %d", stmt.offset)
	}

	return b.String()
}

func (stmt *Statement) ExecuteReadQuery(ctx context.Context, conn *sql.DB, tx *sql.Tx, query string, doc any, obs isql.Observer, cache *core.StmtCache) (err error) {
	if stmt.showSQL {
		log.Printf("Read Query: query: %v, args: %v\n", query, stmt.args)
	}
	start := time.Now()
	defer func() { observeQuery(ctx, obs, query, stmt.args, start, err) }()

	var rows *sql.Rows

	switch {
	case tx != nil:
		rows, err = tx.QueryContext(ctx, query, stmt.args...)
	case cache != nil:
		rows, err = cache.QueryContext(ctx, conn, query, stmt.args...)
	default:
		rows, err = conn.QueryContext(ctx, query, stmt.args...)
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	elem := reflect.ValueOf(doc).Elem()
	switch elem.Kind() {
	case reflect.Struct:
		if rows.Next() {
			if err = core.ScanRow(rows, doc); err != nil {
				return err
			}

			return rows.Err()
		}
	case reflect.Slice:
		for rows.Next() {
			rowElem := reflect.New(elem.Type().Elem()).Interface()
			if err = core.ScanRow(rows, rowElem); err != nil {
				return err
			}
			elem.Set(reflect.Append(elem, reflect.ValueOf(rowElem).Elem()))
		}

		return rows.Err()
	}

	return sql.ErrNoRows
}

// GenerateCountQuery builds a SELECT COUNT(*) query honoring the current table,
// JOINs, soft-delete filter, and WHERE clause. Columns, aggregates, ORDER BY and
// LIMIT/OFFSET are ignored; it is intended for ungrouped row counts. The
// soft-delete column is taken from the statement, or the Sync-time registry keyed
// by table name, so soft-deleted rows are excluded unless WithDeleted was set.
func (stmt *Statement) GenerateCountQuery() string {
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT COUNT(*) FROM \"%s\"", stmt.table)

	for _, join := range stmt.joins {
		b.WriteString(" ")
		b.WriteString(join)
	}

	if softCol := stmt.SoftDeleteColumn(); softCol != "" && !stmt.withDeleted {
		stmt.where = stmt.AddWhereClause(softCol + " IS NULL")
	}
	if stmt.where != "" {
		b.WriteString(" WHERE ")
		b.WriteString(stmt.where)
	}
	return b.String()
}

// ExecuteCountQuery runs a COUNT query and returns the scalar result.
func (stmt *Statement) ExecuteCountQuery(ctx context.Context, conn *sql.DB, tx *sql.Tx, query string, obs isql.Observer, cache *core.StmtCache) (count int64, err error) {
	if stmt.showSQL {
		log.Printf("Count Query: query: %v, args: %v\n", query, stmt.args)
	}
	start := time.Now()
	defer func() { observeQuery(ctx, obs, query, stmt.args, start, err) }()

	row, err := stmt.queryRow(ctx, conn, tx, cache, query)
	if err != nil {
		return 0, err
	}
	err = row.Scan(&count)
	return count, err
}

func (stmt *Statement) GenerateInsertQuery(doc any) string {
	stmt.mustColMap = stmt.generateMustColMap()
	rvalue := reflect.ValueOf(doc)
	if reflect.TypeOf(doc).Kind() == reflect.Pointer {
		rvalue = rvalue.Elem()
	}
	var cols []string
	for _, f := range core.WalkFields(rvalue.Type()) {
		col := core.GetFieldName(f.StructField)
		fv := f.Value(rvalue)

		if !(stmt.allCols || stmt.mustColMap[col] || core.HasReqTag(f.StructField) || !fv.IsZero()) {
			continue
		}

		cols = append(cols, col)
		stmt.args = append(stmt.args, core.SQLArgValue(f.StructField, fv))
	}

	if stmt.table == "" {
		stmt.table = core.GetTableName(doc)
	}

	placeholders := stmt.placeholders(len(cols))

	return fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)",
		stmt.table, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
}

func (stmt *Statement) ExecuteInsertQuery(ctx context.Context, conn *sql.DB, tx *sql.Tx, query string, obs isql.Observer, cache *core.StmtCache) (id any, err error) {
	pkCol := stmt.pkColumn
	if pkCol == "" {
		pkCol = "id"
	}
	query += fmt.Sprintf(" RETURNING %s;", pkCol)
	if stmt.showSQL {
		log.Printf("Insert Query: query: %v, args: %v\n", query, stmt.args)
	}
	start := time.Now()
	defer func() { observeQuery(ctx, obs, query, stmt.args, start, err) }()

	row, err := stmt.queryRow(ctx, conn, tx, cache, query)
	if err != nil {
		return nil, err
	}
	err = row.Scan(&id)
	return id, err
}

// queryRow runs a single-row query over the transaction, the statement cache, or
// the raw connection, in that order of preference.
func (stmt *Statement) queryRow(ctx context.Context, conn *sql.DB, tx *sql.Tx, cache *core.StmtCache, query string) (*sql.Row, error) {
	switch {
	case tx != nil:
		return tx.QueryRowContext(ctx, query, stmt.args...), nil
	case cache != nil:
		return cache.QueryRowContext(ctx, conn, query, stmt.args...)
	default:
		return conn.QueryRowContext(ctx, query, stmt.args...), nil
	}
}

// GenerateBulkInsertQuery builds a single multi-row INSERT for docs that share
// the same struct type. A column is included if it is forced, required, or
// non-zero in any of the docs; every row then supplies a value for each such
// column. Docs must all be of the same type.
func (stmt *Statement) GenerateBulkInsertQuery(docs []any) string {
	stmt.mustColMap = stmt.generateMustColMap()

	first := reflect.ValueOf(docs[0])
	if first.Kind() == reflect.Pointer {
		first = first.Elem()
	}

	walked := core.WalkFields(first.Type())
	include := make([]bool, len(walked))
	for _, doc := range docs {
		rv := reflect.ValueOf(doc)
		if rv.Kind() == reflect.Pointer {
			rv = rv.Elem()
		}
		for idx, f := range walked {
			if include[idx] {
				continue
			}
			col := core.GetFieldName(f.StructField)
			if stmt.allCols || stmt.mustColMap[col] || core.HasReqTag(f.StructField) || !f.Value(rv).IsZero() {
				include[idx] = true
			}
		}
	}

	var cols []string
	var included []core.FieldRef
	for idx := range walked {
		if !include[idx] {
			continue
		}
		cols = append(cols, core.GetFieldName(walked[idx].StructField))
		included = append(included, walked[idx])
	}

	if stmt.table == "" {
		stmt.table = core.GetTableName(docs[0])
	}

	rows := make([]string, 0, len(docs))
	for _, doc := range docs {
		rv := reflect.ValueOf(doc)
		if rv.Kind() == reflect.Pointer {
			rv = rv.Elem()
		}
		placeholders := make([]string, len(included))
		for i, f := range included {
			placeholders[i] = stmt.nextPlaceholder()
			stmt.args = append(stmt.args, core.SQLArgValue(f.StructField, f.Value(rv)))
		}
		rows = append(rows, "("+strings.Join(placeholders, ", ")+")")
	}

	return fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES %s",
		stmt.table, strings.Join(cols, ", "), strings.Join(rows, ", "))
}

// ExecuteBulkInsertQuery runs a multi-row INSERT and returns the generated
// primary keys in row order.
func (stmt *Statement) ExecuteBulkInsertQuery(ctx context.Context, conn *sql.DB, tx *sql.Tx, query string, obs isql.Observer, cache *core.StmtCache) (ids []any, err error) {
	pkCol := stmt.pkColumn
	if pkCol == "" {
		pkCol = "id"
	}
	query += fmt.Sprintf(" RETURNING %s;", pkCol)
	if stmt.showSQL {
		log.Printf("Bulk Insert Query: query: %v, args: %v\n", query, stmt.args)
	}
	start := time.Now()
	defer func() { observeQuery(ctx, obs, query, stmt.args, start, err) }()

	var rows *sql.Rows
	switch {
	case tx != nil:
		rows, err = tx.QueryContext(ctx, query, stmt.args...)
	case cache != nil:
		rows, err = cache.QueryContext(ctx, conn, query, stmt.args...)
	default:
		rows, err = conn.QueryContext(ctx, query, stmt.args...)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id any
		if scanErr := rows.Scan(&id); scanErr != nil {
			return nil, scanErr
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (stmt *Statement) ExecuteWriteQuery(ctx context.Context, conn *sql.DB, tx *sql.Tx, query string, obs isql.Observer, cache *core.StmtCache) (res sql.Result, err error) {
	if stmt.showSQL {
		log.Printf("Write Query: query: %v, args: %v\n", query, stmt.args)
	}
	start := time.Now()
	defer func() { observeQuery(ctx, obs, query, stmt.args, start, err) }()

	switch {
	case tx != nil:
		res, err = tx.ExecContext(ctx, query, stmt.args...)
	case cache != nil:
		res, err = cache.ExecContext(ctx, conn, query, stmt.args...)
	default:
		res, err = conn.ExecContext(ctx, query, stmt.args...)
	}
	return res, err
}

func (stmt *Statement) generateMustColMap() map[string]bool {
	stmt.mustColMap = map[string]bool{}
	for _, col := range stmt.mustCols {
		stmt.mustColMap[col] = true
	}
	return stmt.mustColMap
}

func (stmt *Statement) generateMustFilterColMap() map[string]bool {
	stmt.mustFilterColMap = map[string]bool{}
	for _, col := range stmt.mustFilterCols {
		stmt.mustFilterColMap[col] = true
	}
	return stmt.mustFilterColMap
}

// GenerateUpdateQuery builds an UPDATE that changes at most one row.
func (stmt *Statement) GenerateUpdateQuery(doc any) string {
	return stmt.generateUpdateQuery(doc, true)
}

// GenerateUpdateManyQuery builds an UPDATE that changes every matching row.
func (stmt *Statement) GenerateUpdateManyQuery(doc any) string {
	return stmt.generateUpdateQuery(doc, false)
}

func (stmt *Statement) generateUpdateQuery(doc any, limitOne bool) string {
	setCols, setArgs := stmt.updateSetClause(doc)

	if stmt.table == "" {
		stmt.table = core.GetTableName(doc)
	}
	if stmt.pkColumn == "" {
		stmt.pkColumn, _ = core.DeclaredPKColumn(doc)
	}

	// SET placeholders are numbered afresh from 1, so any existing WHERE
	// placeholders have to shift up by however many SET columns there are.
	if stmt.Dialect().NumberedArgs() {
		stmt.where = renumberPlaceholders(stmt.where, len(setCols))
		stmt.argCounter += len(setCols)
	}

	// SET args before WHERE args so SQL argument order matches
	stmt.args = append(setArgs, stmt.args...)

	return fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		quoteIdent(stmt.table), strings.Join(setCols, ", "), stmt.rowLimit(limitOne))
}

// updateSetClause builds the "col = placeholder" list and its arguments from
// the fields of doc that the statement is set to write.
func (stmt *Statement) updateSetClause(doc any) ([]string, []any) {
	stmt.mustColMap = stmt.generateMustColMap()

	rvalue := reflect.ValueOf(doc)
	if rvalue.Kind() == reflect.Pointer {
		rvalue = rvalue.Elem()
	}

	var setCols []string
	var setArgs []any
	for _, f := range core.WalkFields(rvalue.Type()) {
		col := core.GetFieldName(f.StructField)
		fv := f.Value(rvalue)

		if !(stmt.allCols || stmt.mustColMap[col] || core.HasReqTag(f.StructField) || !fv.IsZero()) {
			continue
		}

		setCols = append(setCols, col+" = "+stmt.Dialect().Placeholder(len(setCols)+1))
		setArgs = append(setArgs, core.SQLArgValue(f.StructField, fv))
	}

	return setCols, setArgs
}

// GenerateDeleteQuery builds a DELETE that removes at most one row.
func (stmt *Statement) GenerateDeleteQuery() string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(stmt.table), stmt.rowLimit(true))
}

// GenerateDeleteManyQuery builds a DELETE that removes every matching row.
func (stmt *Statement) GenerateDeleteManyQuery() string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(stmt.table), stmt.rowLimit(false))
}

// UpdateQuery picks the UPDATE an engine needs: one row or every matching row.
func (stmt *Statement) UpdateQuery(doc any, one bool) string {
	if one {
		return stmt.GenerateUpdateQuery(doc)
	}
	return stmt.GenerateUpdateManyQuery(doc)
}

// DeleteQuery picks the statement that removes rows: a soft-delete UPDATE when
// the schema marks rows deleted, a DELETE otherwise, over one row or all of
// them.
func (stmt *Statement) DeleteQuery(one bool) string {
	switch {
	case stmt.IsSoftDelete() && one:
		return stmt.GenerateSoftDeleteQuery()
	case stmt.IsSoftDelete():
		return stmt.GenerateSoftDeleteManyQuery()
	case one:
		return stmt.GenerateDeleteQuery()
	default:
		return stmt.GenerateDeleteManyQuery()
	}
}
