package lib

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	core "github.com/masudur-rahman/styx/v2/sql/internal/core"
)

type fieldInfo struct {
	Name        string
	Type        string
	IsComposite bool
}

func GenerateTableName(table interface{}) string {
	return core.GetTableName(table)
}

func getTableInfo(d Dialect, table interface{}) ([]fieldInfo, error) {
	tableType := reflect.TypeOf(table)
	tableValue := reflect.ValueOf(table)

	if tableType.Kind() == reflect.Ptr {
		tableType = tableType.Elem()
		tableValue = tableValue.Elem()
	}

	if tableType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("getTableInfo: table is expected to be struct, got %v", tableType.Kind())
	}

	var fields []fieldInfo
	var tagErrs []error
	for _, f := range core.WalkFields(tableType) {
		// Every bad tag on the struct is reported at once, so one run tells
		// the caller about all of them rather than the first.
		if _, err := core.ParseDBTag(f.StructField); err != nil {
			tagErrs = append(tagErrs, err)
			continue
		}

		fields = append(fields, getFieldInfo(d, f.StructField, f.Value(tableValue)))
	}

	if len(tagErrs) > 0 {
		return nil, fmt.Errorf("table %s: %w", core.GetTableName(table), errors.Join(tagErrs...))
	}

	return fields, nil
}

func createTable(ctx context.Context, d Dialect, conn *sql.DB, tableName string, fields []fieldInfo) error {
	query := createTableQuery(d, tableName, fields)
	_, err := ExecuteWriteQuery(ctx, query, conn)
	return err
}

func addMissingColumns(ctx context.Context, d Dialect, conn *sql.DB, tableName string, fields []fieldInfo) error {
	columns, err := getExistingColumns(ctx, d, conn, tableName)
	if err != nil {
		return err
	}

	missingColumns := getMissingColumns(fields, columns)
	if len(missingColumns) > 0 {
		alterQuery := generateAddColumnQuery(tableName, missingColumns)
		_, err = ExecuteWriteQuery(ctx, alterQuery, conn)
		if err != nil {
			return fmt.Errorf("error adding columns to table %s: %v (query: %s)", tableName, err, alterQuery)
		}
	}
	return nil
}

func getFieldInfo(d Dialect, fieldType reflect.StructField, fieldValue reflect.Value) fieldInfo {
	fieldName := getFieldName(fieldType)
	columnConstraint, autoincr, isComposite := getFieldConstraint(d, fieldType)
	if columnConstraint != "" {
		columnConstraint = " " + columnConstraint
	}

	return fieldInfo{
		Name:        fieldName,
		Type:        columnType(d, fieldType, fieldValue.Type(), autoincr) + columnConstraint,
		IsComposite: isComposite,
	}
}

// columnType resolves a field's column type, most explicit source first:
//
//  1. a type= tag, which names the column type outright
//  2. the registry, covering styx.UUID, google/uuid.UUID, time.Time and
//     anything a caller registered, plus types implementing core.SQLTyper
//  3. the json tag, which forces the dialect's JSON column type
//  4. the dialect's Go-kind switch
//
// A genuinely auto-incrementing column skips 1 to 3: the dialect's
// auto-increment type is a complete column definition on SQLite, and
// overriding it would produce a column that does not auto-increment.
//
// "Genuinely" matters because pk implies autoincr on Postgres regardless of
// the field's type, and SQLType falls through to its ordinary mapping for a
// kind that has no auto-incrementing form. Comparing the two answers is what
// distinguishes a real SERIAL from a plain column that merely asked for one:
// without it, a styx.UUID primary key resolved to VARCHAR(255).
func columnType(d Dialect, field reflect.StructField, fieldType reflect.Type, autoincr bool) string {
	if autoincr {
		if col := d.SQLType(fieldType, true); col != "" && col != d.SQLType(fieldType, false) {
			return col
		}
	}

	if tag, err := core.ParseDBTag(field); err == nil {
		if named, ok := tag.Assignment(core.TokenType); ok {
			return core.LookupNamedSQLType(named, d.Name())
		}
	}

	if col, ok := core.LookupSQLType(fieldType, d.Name()); ok {
		return col
	}

	if core.IsJSONField(field) {
		return d.JSONColumnType()
	}

	return d.SQLType(fieldType, false)
}

func getFieldName(fieldType reflect.StructField) string {
	return core.GetFieldName(fieldType)
}

// getFieldConstraint renders the column constraints declared by a db tag.
//
// Tokens are scanned twice because two of the decisions are order-independent:
// autoincr can be requested either by the autoincr token or, on dialects that
// say so, by pk alone; and whether PRIMARY KEY is emitted as its own constraint
// depends on whether the resulting auto-increment column type already contains
// it. SQLite's does, which previously produced the keyword twice and needed a
// post-hoc string fixup to remove.
func getFieldConstraint(d Dialect, fieldType reflect.StructField) (fc string, autoincr bool, isComposite bool) {
	tokens := core.DBTagTokens(fieldType)

	var isPK bool
	for _, tok := range tokens {
		switch tok {
		case core.TokenPK:
			isPK = true
		case core.TokenAutoIncr:
			autoincr = true
		case core.TokenUniqueS:
			isComposite = true
		}
	}
	if isPK && d.AutoIncrOnPK() {
		autoincr = true
	}

	constraints := []string{}
	for _, tok := range tokens {
		switch tok {
		case core.TokenPK:
			if autoincr && d.AutoIncrIncludesPK() {
				continue
			}
			constraints = append(constraints, "PRIMARY KEY")
		case core.TokenUnique:
			constraints = append(constraints, "UNIQUE")
		case core.TokenNotNull:
			constraints = append(constraints, "NOT NULL")
		case core.TokenUniqueS, core.TokenAutoIncr:
			// handled above, no DDL effect of their own
		case core.TokenRequired:
			// handled at query generation time, no DDL effect
		case core.TokenJSON:
			// column type handled in getFieldInfo, no constraint
		}
	}

	return strings.Join(constraints, " "), autoincr, isComposite
}

func hasReqTag(field reflect.StructField) bool {
	return core.HasReqTag(field)
}

// ExtractPKColumn returns the primary key column name from a struct's pk tag.
// Returns "id" as default if no pk tag is found.
func ExtractPKColumn(table any) string {
	return core.GetPKColumn(table)
}

// ExtractSoftDeleteColumn returns the column name tagged with archive, or an
// empty string when the struct has none.
func ExtractSoftDeleteColumn(table any) string {
	return core.ExtractSoftDeleteColumn(table)
}

// getUniqueColumnGroups returns the unique-constraint column groups declared by
// uqs tags, in field declaration order.
//
// Groups are accumulated into a slice rather than a map because constraint
// names are derived from the group's position: map iteration order is
// randomised, so the same struct produced differently named constraints on
// different runs.
func getUniqueColumnGroups(t reflect.Type) [][]string {
	groups := [][]string{}
	for _, f := range core.WalkFields(t) {
		if core.HasDBToken(f.StructField, core.TokenUniqueS) {
			groups = append(groups, []string{getFieldName(f.StructField)})
		}
	}

	return groups
}

// tableExists reports whether tableName is present.
//
// The two dialects answer differently: Postgres selects EXISTS and always
// returns a row, while SQLite selects the name from sqlite_master and returns
// no rows when the table is absent.
func tableExists(ctx context.Context, d Dialect, conn *sql.DB, tableName string) (bool, error) {
	row := conn.QueryRowContext(ctx, d.TableExistsQuery(), tableName)

	if d.Name() == DialectPostgres {
		var exists bool
		if err := row.Scan(&exists); err != nil {
			return false, fmt.Errorf("checking if table %s exists: %w", tableName, err)
		}
		return exists, nil
	}

	var name string
	switch err := row.Scan(&name); {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("checking if table %s exists: %w", tableName, err)
	}
	return true, nil
}

func createTableQuery(d Dialect, tableName string, fields []fieldInfo) string {
	var columnDefs []string
	var compositeKeyGroup []string
	for _, field := range fields {
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", field.Name, field.Type))
		if field.IsComposite {
			compositeKeyGroup = append(compositeKeyGroup, field.Name)
		}
	}

	columnSQL := strings.Join(columnDefs, ", ")
	if len(compositeKeyGroup) > 0 {
		compositeKeySQL := fmt.Sprintf("UNIQUE(%s)", strings.Join(compositeKeyGroup, ", "))
		columnSQL += ", " + compositeKeySQL
	}

	return fmt.Sprintf("%s %s (%s);", d.CreateTablePrefix(), quoteIdent(tableName), columnSQL)
}

func generateAddColumnQuery(tableName string, missingColumns []string) string {
	alterQuery := fmt.Sprintf("ALTER TABLE \"%s\" ", tableName)
	var addColumns []string
	for _, col := range missingColumns {
		addColumns = append(addColumns, fmt.Sprintf("ADD COLUMN %s", col))
	}

	alterQuery += strings.Join(addColumns, ", ")
	return alterQuery
}

func getExistingColumns(ctx context.Context, d Dialect, conn *sql.DB, tableName string) ([]string, error) {
	var columns []string

	rows, err := conn.QueryContext(ctx, d.ExistingColumnsQuery(), tableName)
	if err != nil {
		return nil, fmt.Errorf("getting columns for table %s: %w", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		column, err := d.ScanColumnName(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scanning column for table %s: %w", tableName, err)
		}
		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error getting columns for table %s: %v", tableName, err)
	}

	return columns, nil
}

func getMissingColumns(fields []fieldInfo, columns []string) []string {
	var missingColumns []string

	for _, f := range fields {
		if !contains(columns, f.Name) {
			missingColumns = append(missingColumns, fmt.Sprintf("%s %s", f.Name, f.Type))
		}
	}

	return missingColumns
}

func getUniqueConstraints(ctx context.Context, conn *sql.DB, tableName string) ([][]string, error) {
	query := `
	SELECT kcu.column_name
	FROM information_schema.table_constraints tc
	JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
	WHERE tc.table_name = $1 AND tc.constraint_type = 'UNIQUE'
	ORDER BY kcu.ordinal_position;
	`

	rows, err := conn.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("error getting unique constraints for table %s: %v", tableName, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		err = rows.Scan(&column)
		if err != nil {
			return nil, fmt.Errorf("error scanning unique constraint for table %s: %v", tableName, err)
		}
		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error getting unique constraints for table %s: %v", tableName, err)
	}

	var result [][]string
	if len(columns) > 0 {
		result = append(result, columns)
	}

	return result, nil
}

func generateDropConstraintStatement(tableName string, uqConstraints [][]string) string {
	sql := fmt.Sprintf("ALTER TABLE \"%s\" ", tableName)

	var dropConstraints []string
	for i := range uqConstraints {
		dropConstraints = append(dropConstraints,
			fmt.Sprintf("DROP CONSTRAINT IF EXISTS %s_uq_%d", tableName,
				i))
	}

	sql += strings.Join(dropConstraints, ", ")

	return sql
}

func generateAddConstraintStatement(tableName string,
	uqGroups [][]string) string {

	sql := fmt.Sprintf("ALTER TABLE \"%s\" ", tableName)

	var addConstraints []string
	for i, group := range uqGroups {
		addConstraints = append(addConstraints,
			fmt.Sprintf("ADD CONSTRAINT %s_uq_%d UNIQUE(%s)",
				tableName,
				i,
				strings.Join(group,
					", ")))
	}

	sql += strings.Join(addConstraints,
		", ")

	return sql
}

func contains(slice []string, val string) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

// indexInfo holds parsed index metadata from struct tags.
type indexInfo struct {
	Name   string
	Cols   []string
	Unique bool
}

// extractIndexes parses idx and uidx tags from a struct type.
//
// namedOrder preserves first-declaration order. Ranging the map directly
// emitted CREATE INDEX statements in a different order on every run.
func extractIndexes(table any) []indexInfo {
	tableType := reflect.TypeOf(table)
	if tableType.Kind() == reflect.Ptr {
		tableType = tableType.Elem()
	}
	if tableType.Kind() != reflect.Struct {
		return nil
	}

	named := map[string]*indexInfo{}
	var namedOrder []string
	var unnamed []indexInfo

	for _, f := range core.WalkFields(tableType) {
		tag, _ := core.ParseDBTag(f.StructField)
		if len(tag.Tokens) == 0 {
			continue
		}
		colName := core.GetFieldName(f.StructField)

		for _, tok := range tag.Tokens {
			prefix, idxName, isNamed := strings.Cut(tok, ":")
			unique := prefix == core.TokenUIndex
			if prefix != core.TokenIndex && prefix != core.TokenUIndex {
				continue
			}

			if !isNamed {
				unnamed = append(unnamed, indexInfo{Cols: []string{colName}, Unique: unique})
				continue
			}

			idxName = strings.ToLower(idxName)
			if existing, ok := named[idxName]; ok {
				existing.Cols = append(existing.Cols, colName)
				continue
			}
			named[idxName] = &indexInfo{Name: idxName, Cols: []string{colName}, Unique: unique}
			namedOrder = append(namedOrder, idxName)
		}
	}

	var result []indexInfo
	for _, name := range namedOrder {
		result = append(result, *named[name])
	}
	return append(result, unnamed...)
}

func createIndexes(ctx context.Context, conn *sql.DB, tableName string, indexes []indexInfo) error {
	for i, idx := range indexes {
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		name := idx.Name
		if name == "" {
			name = fmt.Sprintf("idx_%s_%d", tableName, i)
		}
		query := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS \"%s\" ON \"%s\" (%s)",
			unique, name, tableName, strings.Join(idx.Cols, ", "))
		if _, err := ExecuteWriteQuery(ctx, query, conn); err != nil {
			return fmt.Errorf("error creating index %s: %w", name, err)
		}
	}
	return nil
}

// DropTable drops a table by name.
func DropTable(ctx context.Context, conn *sql.DB, tableName string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS \"%s\"", tableName)
	_, err := ExecuteWriteQuery(ctx, query, conn)
	return err
}

func SyncTable(ctx context.Context, d Dialect, conn *sql.DB, table any) error {
	tableName := GenerateTableName(table)
	fields, err := getTableInfo(d, table)
	if err != nil {
		return err
	}

	if exist, err := tableExists(ctx, d, conn, tableName); err != nil {
		return err
	} else if !exist {
		if err = createTable(ctx, d, conn, tableName, fields); err != nil {
			return err
		}
	} else {
		if err = addMissingColumns(ctx, d, conn, tableName, fields); err != nil {
			return err
		}
	}

	indexes := extractIndexes(table)
	if len(indexes) > 0 {
		if err = createIndexes(ctx, conn, tableName, indexes); err != nil {
			return err
		}
	}

	return nil
}

// ExecuteWriteQuery runs a statement that returns no rows.
func ExecuteWriteQuery(ctx context.Context, query string, conn *sql.DB) (sql.Result, error) {
	return conn.ExecContext(ctx, query)
}
