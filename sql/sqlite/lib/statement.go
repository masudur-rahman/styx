package lib

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/masudur-rahman/styx/dberr"
	isql "github.com/masudur-rahman/styx/sql"
	core "github.com/masudur-rahman/styx/sql/internal/core"
)

// observeQuery reports a completed statement to obs when one is configured.
func observeQuery(ctx context.Context, obs isql.Observer, query string, args []any, start time.Time, err error) {
	if obs != nil {
		obs.OnQuery(ctx, query, args, time.Since(start), err)
	}
}

type Statement struct {
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
type cteClause struct {
	name string
	sql  string
	args []any
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
// compiled subSQL with its subArgs. The CTE is emitted as a WITH prefix and,
// because SQLite uses positional "?" placeholders, its args are spliced ahead
// of the main body args at query-generation time.
func (stmt *Statement) With(name, subSQL string, subArgs []any) *Statement {
	stmt.ctes = append(stmt.ctes, cteClause{name: name, sql: subSQL, args: subArgs})
	return stmt
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

	placeholders := make([]string, len(values))
	for i := range values {
		placeholders[i] = "?"
	}
	stmt.args = append(stmt.args, values...)
	stmt.where += fmt.Sprintf("%s IN (%s)", col, strings.Join(placeholders, ", "))
	return stmt
}

func (stmt *Statement) Where(cond string, args ...any) *Statement {
	stmt.where = stmt.AddWhereClause(cond)
	if len(args) > 0 {
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
	return "id = ?"
}

func (stmt *Statement) GenerateWhereClauseFromFilter(filter any) string {
	stmt.mustFilterColMap = stmt.generateMustFilterColMap()
	var conditions []string

	val := reflect.ValueOf(filter)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	for idx := 0; idx < val.NumField(); idx++ {
		field := val.Type().Field(idx)
		col := core.GetFieldName(field)

		if !(stmt.allCols || stmt.mustFilterColMap[col] || core.HasReqTag(field) || !val.Field(idx).IsZero()) {
			continue
		}

		conditions = append(conditions, col+" = ?")
		stmt.args = append(stmt.args, core.SQLArgValue(field, val.Field(idx)))
	}

	return strings.Join(conditions, " AND ")
}

func (stmt *Statement) GenerateWhereClause(filter ...any) *Statement {
	stmt.where = stmt.AddWhereClause(stmt.generateWhereClauseFromID())
	if len(filter) > 0 {
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
	stmt.where = stmt.AddWhereClause(fmt.Sprintf("%s LIKE ?", col))
	stmt.args = append(stmt.args, pattern)
	return stmt
}

// NotLike adds a NOT LIKE condition to the WHERE clause.
func (stmt *Statement) NotLike(col string, pattern string) *Statement {
	stmt.where = stmt.AddWhereClause(fmt.Sprintf("%s NOT LIKE ?", col))
	stmt.args = append(stmt.args, pattern)
	return stmt
}

// Exists adds an EXISTS subquery condition to the WHERE clause.
func (stmt *Statement) Exists(subquery string, args ...any) *Statement {
	stmt.where = stmt.AddWhereClause(fmt.Sprintf("EXISTS (%s)", subquery))
	if len(args) > 0 {
		newArgs := make([]any, len(args))
		copy(newArgs, args)
		stmt.args = append(stmt.args, newArgs...)
	}
	return stmt
}

// NotExists adds a NOT EXISTS subquery condition to the WHERE clause.
func (stmt *Statement) NotExists(subquery string, args ...any) *Statement {
	stmt.where = stmt.AddWhereClause(fmt.Sprintf("NOT EXISTS (%s)", subquery))
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
func (stmt *Statement) IsSoftDelete() bool {
	return stmt.softDeleteCol != "" && !stmt.forceDelete
}

// GenerateSoftDeleteQuery generates an UPDATE query that sets the soft delete column.
func (stmt *Statement) GenerateSoftDeleteQuery() string {
	return fmt.Sprintf("UPDATE \"%s\" SET %s = CURRENT_TIMESTAMP WHERE %s", stmt.table, stmt.softDeleteCol, stmt.where)
}

// GenerateRestoreQuery generates an UPDATE that clears the soft delete column.
func (stmt *Statement) GenerateRestoreQuery() string {
	return fmt.Sprintf("UPDATE \"%s\" SET %s = NULL WHERE %s", stmt.table, stmt.softDeleteCol, stmt.where)
}

// buildCTEPrefix emits the "WITH n1 AS (sql1), n2 AS (sql2) " prefix and splices
// the CTE args ahead of the main body args, since SQLite placeholders are
// positional and the CTE bodies appear first in the final SQL. Returns "" when
// no CTEs are registered.
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
	stmt.args = append(cteArgs, stmt.args...)
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

	if stmt.softDeleteCol != "" && !stmt.withDeleted {
		stmt.where = stmt.AddWhereClause(stmt.softDeleteCol + " IS NULL")
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

	softCol := stmt.softDeleteCol
	if softCol == "" {
		softCol = core.SoftDeleteColumnForTable(stmt.table)
	}
	if softCol != "" && !stmt.withDeleted {
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
	for idx := 0; idx < rvalue.NumField(); idx++ {
		field := rvalue.Type().Field(idx)
		if core.IsRelationField(field) {
			continue
		}
		col := core.GetFieldName(field)

		if !(stmt.allCols || stmt.mustColMap[col] || core.HasReqTag(field) || !rvalue.Field(idx).IsZero()) {
			continue
		}

		cols = append(cols, col)
		stmt.args = append(stmt.args, core.SQLArgValue(field, rvalue.Field(idx)))
	}

	if stmt.table == "" {
		stmt.table = core.GetTableName(doc)
	}

	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = "?"
	}

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

	include := make([]bool, first.NumField())
	for _, doc := range docs {
		rv := reflect.ValueOf(doc)
		if rv.Kind() == reflect.Pointer {
			rv = rv.Elem()
		}
		for idx := 0; idx < rv.NumField(); idx++ {
			if include[idx] {
				continue
			}
			field := rv.Type().Field(idx)
			if core.IsRelationField(field) {
				continue
			}
			col := core.GetFieldName(field)
			if stmt.allCols || stmt.mustColMap[col] || core.HasReqTag(field) || !rv.Field(idx).IsZero() {
				include[idx] = true
			}
		}
	}

	var cols []string
	var fieldIdxs []int
	for idx := 0; idx < first.NumField(); idx++ {
		if !include[idx] {
			continue
		}
		cols = append(cols, core.GetFieldName(first.Type().Field(idx)))
		fieldIdxs = append(fieldIdxs, idx)
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
		placeholders := make([]string, len(fieldIdxs))
		for i, fi := range fieldIdxs {
			placeholders[i] = "?"
			stmt.args = append(stmt.args, core.SQLArgValue(rv.Type().Field(fi), rv.Field(fi)))
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

func (stmt *Statement) GenerateUpdateQuery(doc any) string {
	stmt.mustColMap = stmt.generateMustColMap()
	var setCols []string
	var setArgs []any
	rvalue := reflect.ValueOf(doc)
	if reflect.TypeOf(doc).Kind() == reflect.Pointer {
		rvalue = rvalue.Elem()
	}
	for idx := 0; idx < rvalue.NumField(); idx++ {
		field := rvalue.Type().Field(idx)
		if core.IsRelationField(field) {
			continue
		}
		col := core.GetFieldName(field)

		if !(stmt.allCols || stmt.mustColMap[col] || core.HasReqTag(field) || !rvalue.Field(idx).IsZero()) {
			continue
		}

		setCols = append(setCols, col+" = ?")
		setArgs = append(setArgs, core.SQLArgValue(field, rvalue.Field(idx)))
	}

	if stmt.table == "" {
		stmt.table = core.GetTableName(doc)
	}

	// SET args go before WHERE args in the driver call
	stmt.args = append(setArgs, stmt.args...)

	return fmt.Sprintf("UPDATE \"%s\" SET %s WHERE %s",
		stmt.table, strings.Join(setCols, ", "), stmt.where)
}

func (stmt *Statement) GenerateDeleteQuery() string {
	query := fmt.Sprintf("DELETE FROM \"%s\" WHERE %s", stmt.table, stmt.where)
	return query
}
