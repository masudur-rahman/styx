package lib

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/masudur-rahman/styx/dberr"
)

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
	argCounter       int
	showSQL          bool
	pkColumn         string
}

func (stmt Statement) Table(name string) Statement {
	stmt.table = name
	return stmt
}

func (stmt Statement) ID(id any) Statement {
	if stmt.where != "" {
		stmt.where += " AND "
	}

	stmt.id = id
	return stmt
}

func (stmt Statement) In(col string, values ...any) Statement {
	if stmt.where != "" {
		stmt.where += " AND "
	}

	// Use parameterized placeholders instead of direct string formatting
	placeholders := make([]string, len(values))
	for i := range values {
		stmt.argCounter++
		placeholders[i] = fmt.Sprintf("$%d", stmt.argCounter)
	}
	stmt.args = append(stmt.args, values...)
	stmt.where += fmt.Sprintf("%s IN (%s)", col, strings.Join(placeholders, ", "))
	return stmt
}

func (stmt Statement) Where(cond string, args ...any) Statement {
	for range args {
		stmt.argCounter++
		cond = strings.Replace(cond, "?", fmt.Sprintf("$%d", stmt.argCounter), 1)
	}
	stmt.where = stmt.AddWhereClause(cond)
	if len(args) > 0 {
		// Create a new slice to avoid sharing underlying array
		newArgs := make([]any, len(args))
		copy(newArgs, args)
		stmt.args = append(stmt.args, newArgs...)
	}
	return stmt
}

func (stmt Statement) GenerateWhereClause(filter ...any) Statement {
	stmt.where = stmt.AddWhereClause(GenerateWhereClauseFromID(stmt.id))
	if len(filter) > 0 {
		stmt.where = stmt.AddWhereClause(stmt.GenerateWhereClauseFromFilter(filter[0]))
	}
	return stmt
}

func (stmt Statement) CheckWhereClauseNotEmpty() error {
	if stmt.where == "" {
		return dberr.ErrMissingWhereClause
	}
	return nil
}

func (stmt Statement) AddWhereClause(cond string) string {
	if stmt.where != "" && cond != "" {
		stmt.where += " AND "
	}

	stmt.where += cond
	return stmt.where
}

func (stmt Statement) Columns(cols ...string) Statement {
	stmt.columns = cols
	return stmt
}

func (stmt Statement) AllCols() Statement {
	stmt.allCols = true
	return stmt
}

func (stmt Statement) MustCols(cols ...string) Statement {
	stmt.mustCols = cols
	return stmt
}

// MustFilterCols marks columns that must be included in WHERE clauses even when zero-valued.
func (stmt Statement) MustFilterCols(cols ...string) Statement {
	stmt.mustFilterCols = cols
	return stmt
}

func (stmt Statement) ShowSQL(showSQL bool) Statement {
	stmt.showSQL = showSQL
	return stmt
}

// PKColumn sets the primary key column name for RETURNING clause in INSERT queries.
func (stmt Statement) PKColumn(col string) Statement {
	stmt.pkColumn = col
	return stmt
}

func (stmt Statement) GenerateReadQuery(doc any) string {
	var cols string
	if stmt.allCols || len(stmt.columns) == 0 {
		cols = "*"
	} else {
		cols = strings.Join(stmt.columns, ", ")
	}

	if stmt.table == "" {
		val := reflect.ValueOf(doc)
		if val.Kind() == reflect.Slice {
			doc = val.Index(0).Interface()
		}

		stmt.table = GenerateTableName(doc)
	}

	query := fmt.Sprintf("SELECT %s FROM \"%s\"", cols, stmt.table)

	if stmt.where != "" {
		query = fmt.Sprintf("%s WHERE %s;", query, stmt.where)
	}

	return query
}

func (stmt Statement) ExecuteReadQuery(ctx context.Context, conn *sql.Conn, tx *sql.Tx, query string, doc any) error {
	//defer  stmt.cleanup()

	if stmt.showSQL {
		log.Printf("Read Query: query: %v, args: %v\n", query, stmt.args)
	}

	var (
		err  error
		rows *sql.Rows
	)

	if tx != nil {
		rows, err = tx.QueryContext(ctx, query, stmt.args...)
	} else {
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
			fieldMap := GenerateDBFieldMap(doc)
			if err = ScanSingleRow(rows, fieldMap); err != nil {
				return err
			}

			return rows.Err()
		}
	case reflect.Slice:
		for rows.Next() {
			rowELem := reflect.New(elem.Type().Elem()).Interface()
			fieldMap := GenerateDBFieldMap(rowELem)
			if err = ScanSingleRow(rows, fieldMap); err != nil {
				return err
			}
			elem.Set(reflect.Append(elem, reflect.ValueOf(rowELem).Elem()))
		}

		return rows.Err()
	}

	return sql.ErrNoRows
}

func (stmt Statement) GenerateInsertQuery(doc any) string {
	stmt.mustColMap = stmt.generateMustColMap()
	rvalue := reflect.ValueOf(doc)
	if reflect.TypeOf(doc).Kind() == reflect.Pointer {
		rvalue = rvalue.Elem()
	}
	var cols, values []string
	for idx := 0; idx < rvalue.NumField(); idx++ {
		field := rvalue.Type().Field(idx)
		col := getFieldName(field)

		if !(stmt.allCols || stmt.mustColMap[col] || hasReqTag(field) || !rvalue.Field(idx).IsZero()) {
			continue
		}

		value := formatValues(rvalue.Field(idx).Interface())
		cols = append(cols, col)
		values = append(values, value)
	}

	if stmt.table == "" {
		stmt.table = GenerateTableName(doc)
	}

	colClause := strings.Join(cols, ", ")
	valClause := strings.Join(values, ", ")
	query := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)", stmt.table, colClause, valClause)

	return query
}

func (stmt Statement) ExecuteInsertQuery(ctx context.Context, conn *sql.Conn, tx *sql.Tx, query string) (any, error) {
	pkCol := stmt.pkColumn
	if pkCol == "" {
		pkCol = "id"
	}
	query += fmt.Sprintf(" RETURNING %s;", pkCol)
	if stmt.showSQL {
		log.Printf("Insert Query: query: %v, args: %v\n", query, stmt.args)
	}

	var (
		id  any
		err error
	)
	if tx != nil {
		err = tx.QueryRowContext(ctx, query, stmt.args...).Scan(&id)
	} else {
		err = conn.QueryRowContext(ctx, query, stmt.args...).Scan(&id)
	}
	return id, err
}

func (stmt Statement) ExecuteWriteQuery(ctx context.Context, conn *sql.Conn, tx *sql.Tx, query string) (sql.Result, error) {
	if stmt.showSQL {
		log.Printf("Write Query: query: %v, args: %v\n", query, stmt.args)
	}

	if tx != nil {
		return tx.ExecContext(ctx, query, stmt.args...)
	}
	return conn.ExecContext(ctx, query, stmt.args...)
}

func (stmt Statement) generateMustColMap() map[string]bool {
	stmt.mustColMap = map[string]bool{}
	for _, col := range stmt.mustCols {
		stmt.mustColMap[col] = true
	}
	return stmt.mustColMap
}

func (stmt Statement) generateMustFilterColMap() map[string]bool {
	stmt.mustFilterColMap = map[string]bool{}
	for _, col := range stmt.mustFilterCols {
		stmt.mustFilterColMap[col] = true
	}
	return stmt.mustFilterColMap
}

func (stmt Statement) GenerateUpdateQuery(doc any) string {
	stmt.mustColMap = stmt.generateMustColMap()
	var setValues []string
	rvalue := reflect.ValueOf(doc)
	if reflect.TypeOf(doc).Kind() == reflect.Pointer {
		rvalue = rvalue.Elem()
	}
	for idx := 0; idx < rvalue.NumField(); idx++ {
		field := rvalue.Type().Field(idx)
		col := getFieldName(field)

		if !(stmt.allCols || stmt.mustColMap[col] || hasReqTag(field) || !rvalue.Field(idx).IsZero()) {
			continue
		}

		value := formatValues(rvalue.Field(idx).Interface())
		setValue := fmt.Sprintf("%s = %s", col, value)
		setValues = append(setValues, setValue)
	}

	if stmt.table == "" {
		stmt.table = GenerateTableName(doc)
	}

	setClause := strings.Join(setValues, ", ")
	query := fmt.Sprintf("UPDATE \"%s\" SET %s WHERE %s", stmt.table, setClause, stmt.where)
	return query
}

func (stmt Statement) GenerateDeleteQuery() string {
	query := fmt.Sprintf("DELETE FROM \"%s\" WHERE %s", stmt.table, stmt.where)
	return query
}
