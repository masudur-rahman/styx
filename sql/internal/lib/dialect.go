// Package lib holds the SQL generation shared by every SQL engine styx
// supports. Everything that differs between databases is reached through a
// Dialect, so statement building and table synchronisation exist once rather
// than once per driver.
package lib

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Dialect names for type-registry lookups and error messages.
const (
	DialectPostgres = "postgres"
	DialectSQLite   = "sqlite"
)

// Dialect describes everything the shared SQL builders need to know about a
// particular database.
type Dialect interface {
	// Name identifies the dialect, e.g. "postgres".
	Name() string

	// NumberedArgs reports whether placeholders reference bound arguments by
	// number ($1, $2) rather than by order of appearance (?). It decides how
	// conditions are rewritten and how CTE arguments are ordered.
	NumberedArgs() bool

	// Placeholder returns the marker for the n-th bound argument, 1-based.
	Placeholder(n int) string

	// SQLType maps a Go type to a column type. autoincr requests the
	// auto-incrementing form for integer kinds. An empty result means the type
	// has no mapping.
	SQLType(fieldType reflect.Type, autoincr bool) string

	// AutoIncrOnPK reports whether a pk tag alone implies auto-increment.
	// Postgres emits SERIAL for an integer primary key; SQLite does not.
	AutoIncrOnPK() bool

	// AutoIncrIncludesPK reports whether the auto-increment column type already
	// carries PRIMARY KEY, in which case it must not also be emitted as a
	// separate constraint.
	AutoIncrIncludesPK() bool

	// JSONColumnType is the column type used for json-tagged fields.
	JSONColumnType() string

	// CreateTablePrefix is the leading clause of a CREATE TABLE statement,
	// including any IF NOT EXISTS.
	CreateTablePrefix() string

	// TableExistsQuery returns a query taking the table name as its only bound
	// argument. See TableExistsFromRows for how the result is interpreted.
	TableExistsQuery() string

	// ExistingColumnsQuery returns a query taking the table name as its only
	// bound argument and yielding one row per column.
	ExistingColumnsQuery() string

	// ScanColumnName reads a column name from one row of ExistingColumnsQuery.
	// The shapes differ: information_schema returns a single column, while
	// SQLite's pragma table_info returns six.
	ScanColumnName(scan func(...any) error) (string, error)
}

// postgres implements Dialect for PostgreSQL.
type postgres struct{}

// Postgres is the PostgreSQL dialect.
var Postgres Dialect = postgres{}

func (postgres) Name() string { return DialectPostgres }

func (postgres) NumberedArgs() bool { return true }

func (postgres) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }

func (postgres) AutoIncrOnPK() bool { return true }

func (postgres) AutoIncrIncludesPK() bool { return false }

func (postgres) JSONColumnType() string { return "JSONB" }

func (postgres) CreateTablePrefix() string { return "CREATE TABLE" }

func (postgres) TableExistsQuery() string {
	return "" +
		"SELECT EXISTS (" +
		"    SELECT FROM " +
		"        information_schema.tables " +
		"    WHERE " +
		"        table_schema LIKE 'public' AND " +
		"        table_name = $1" +
		");"
}

func (postgres) ExistingColumnsQuery() string {
	return "SELECT column_name FROM information_schema.columns WHERE table_name=$1"
}

func (postgres) ScanColumnName(scan func(...any) error) (string, error) {
	var column string
	err := scan(&column)
	return column, err
}

func (postgres) SQLType(fieldType reflect.Type, autoincr bool) string {
	fieldType = derefType(fieldType)

	if autoincr {
		switch fieldType.Kind() {
		case reflect.Int, reflect.Int32:
			return "SERIAL"
		case reflect.Int64, reflect.Uint64:
			return "BIGSERIAL"
		}
	}

	switch fieldType.Kind() {
	case reflect.Int, reflect.Int32:
		return "INTEGER"
	case reflect.Int64, reflect.Uint64:
		return "BIGINT"
	case reflect.Float32, reflect.Float64:
		return "FLOAT"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.String:
		return "VARCHAR(255)"
	case reflect.Slice:
		if fieldType.Elem().Kind() == reflect.Uint8 {
			return "BYTEA"
		}
	case reflect.Struct:
		if fieldType == timeType {
			return "TIMESTAMP WITH TIME ZONE"
		}
	}

	return ""
}

// sqlite implements Dialect for SQLite.
type sqlite struct{}

// SQLite is the SQLite dialect.
var SQLite Dialect = sqlite{}

func (sqlite) Name() string { return DialectSQLite }

func (sqlite) NumberedArgs() bool { return false }

func (sqlite) Placeholder(int) string { return "?" }

func (sqlite) JSONColumnType() string { return "TEXT" }

func (sqlite) AutoIncrOnPK() bool { return false }

// AutoIncrIncludesPK is true because SQLite's auto-increment column type is
// literally "INTEGER PRIMARY KEY AUTOINCREMENT".
func (sqlite) AutoIncrIncludesPK() bool { return true }

func (sqlite) CreateTablePrefix() string { return "CREATE TABLE IF NOT EXISTS" }

func (sqlite) TableExistsQuery() string {
	return "SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
}

func (sqlite) ExistingColumnsQuery() string {
	return "SELECT cid, name, type, \"notnull\", dflt_value, pk FROM pragma_table_info(?)"
}

func (sqlite) ScanColumnName(scan func(...any) error) (string, error) {
	var discard any
	var column string
	err := scan(&discard, &column, &discard, &discard, &discard, &discard)
	return column, err
}

func (sqlite) SQLType(fieldType reflect.Type, autoincr bool) string {
	fieldType = derefType(fieldType)

	if autoincr {
		switch fieldType.Kind() {
		case reflect.Int, reflect.Int32, reflect.Int64, reflect.Uint64:
			return "INTEGER PRIMARY KEY AUTOINCREMENT"
		}
	}

	switch fieldType.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64, reflect.Uint64:
		return "INTEGER"
	case reflect.Float32, reflect.Float64:
		return "REAL"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.String:
		return "TEXT"
	case reflect.Slice:
		if fieldType.Elem().Kind() == reflect.Uint8 {
			return "BLOB"
		}
	case reflect.Struct:
		if fieldType == timeType {
			return "DATETIME"
		}
	}

	return ""
}

var timeType = reflect.TypeOf(time.Time{})

func derefType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

// quoteIdent wraps an identifier in double quotes, which both dialects accept.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
