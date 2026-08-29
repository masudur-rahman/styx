// Package types holds styx's public column types and the registry that maps a
// Go type to the column type each dialect should use for it.
package types

import (
	"reflect"
	"strings"
	"sync"
	"time"
)

// Dialect names used as keys in the column-type registry.
const (
	DialectPostgres = "postgres"
	DialectSQLite   = "sqlite"
)

// UUID is a uuid-typed column.
//
// Its underlying type is string, so it assigns from a string literal, compares
// with ==, is sent to the driver as text, and marshals to JSON as a plain
// string. styx maps it to the native uuid type where the dialect has one.
type UUID string

// String returns the uuid in its usual 36-character form.
func (u UUID) String() string { return string(u) }

// SQLTyper lets a caller's own type declare the column type it wants. It is an
// escape hatch, not a requirement: styx registers the common types itself, and
// RegisterSQLType covers types whose package you do not control.
type SQLTyper interface {
	SQLType(dialect string) string
}

// sqlTypes maps a type key to its column type per dialect. Types are keyed by
// "pkgpath.Name" rather than by reflect.Type so that widely used types can be
// registered without styx importing the packages that define them.
var sqlTypes sync.Map // map[string]map[string]string

// namedTypes maps a bare, dialect-portable type name usable in a type= tag,
// e.g. type=uuid, onto the same per-dialect column types.
var namedTypes sync.Map // map[string]map[string]string

func init() {
	uuidColumns := map[string]string{
		DialectPostgres: "UUID",
		DialectSQLite:   "TEXT",
	}
	RegisterSQLType(reflect.TypeOf(UUID("")), uuidColumns)
	RegisterNamedSQLType("uuid", uuidColumns)

	// Registered by name so styx imports neither package. google/uuid is
	// already an indirect dependency and stays one; gofrs comes along free.
	registerTypeKey("github.com/google/uuid.UUID", uuidColumns)
	registerTypeKey("github.com/gofrs/uuid.UUID", uuidColumns)

	// time.Time was a hardcoded special case in each dialect's type switch.
	// As a registry entry it becomes overridable by a type= tag like any other.
	RegisterSQLType(reflect.TypeOf(time.Time{}), map[string]string{
		DialectPostgres: "TIMESTAMP WITH TIME ZONE",
		DialectSQLite:   "DATETIME",
	})
}

// RegisterSQLType records the column type a Go type maps to, per dialect.
// It is safe to call from an init function in another package.
func RegisterSQLType(t reflect.Type, byDialect map[string]string) {
	registerTypeKey(TypeKey(t), byDialect)
}

// RegisterNamedSQLType records a portable name usable in a type= db tag, so
// that `type=uuid` resolves to the right column type on each dialect rather
// than being passed through as literal SQL.
func RegisterNamedSQLType(name string, byDialect map[string]string) {
	namedTypes.Store(strings.ToLower(name), byDialect)
}

func registerTypeKey(key string, byDialect map[string]string) {
	sqlTypes.Store(key, byDialect)
}

// TypeKey is the registry key for a type: "pkgpath.Name" for a named type
// defined in a package, or just the name for a builtin.
func TypeKey(t reflect.Type) string {
	if t.PkgPath() == "" {
		return t.Name()
	}
	return t.PkgPath() + "." + t.Name()
}

// LookupSQLType returns the registered column type for t on the given dialect.
// Pointers are followed, so *UUID resolves the same as UUID.
func LookupSQLType(t reflect.Type, dialect string) (string, bool) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if v, ok := sqlTypes.Load(TypeKey(t)); ok {
		col, found := v.(map[string]string)[dialect]
		return col, found
	}

	// A type may instead declare its own column type.
	if st, ok := reflect.New(t).Interface().(SQLTyper); ok {
		return st.SQLType(dialect), true
	}

	return "", false
}

// LookupNamedSQLType resolves a type= tag value. A registered portable name
// maps per dialect; anything else is passed through as literal SQL, so
// `type=numeric(10,2)` reaches the database unchanged.
func LookupNamedSQLType(name, dialect string) string {
	if v, ok := namedTypes.Load(strings.ToLower(name)); ok {
		if col, found := v.(map[string]string)[dialect]; found {
			return col
		}
	}
	return name
}
