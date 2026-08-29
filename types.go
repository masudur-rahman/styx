package styx

import (
	"reflect"

	"github.com/masudur-rahman/styx/v2/types"
)

// Dialect names accepted by RegisterSQLType and passed to SQLTyper.
const (
	DialectPostgres = types.DialectPostgres
	DialectSQLite   = types.DialectSQLite
)

// UUID is a uuid-typed column.
//
// Its underlying type is string, so it assigns from a string literal, compares
// with ==, is sent to the driver as text, and marshals to JSON as a plain
// string. styx maps it to the native uuid type where the dialect has one, and
// to TEXT where it does not:
//
//	type Patient struct {
//	    ID   styx.UUID `json:"id" db:"id,pk"`
//	    Name string    `json:"name"`
//	}
//
// A field typed uuid.UUID from github.com/google/uuid or github.com/gofrs/uuid
// maps the same way, without either package being imported here. A plain
// string field can opt in per column with a tag instead: `db:"id,pk type=uuid"`.
//
// Note that a defined type over another uuid type, "type UUID uuid.UUID", is
// not the same thing: a defined type inherits none of its underlying type's
// methods, so it loses Scan, Value and MarshalText and will not round-trip.
// Use an alias, "type UUID = uuid.UUID", or a string-backed type like this one.
type UUID = types.UUID

// SQLTyper lets a type declare the column type it wants. It is an escape
// hatch: styx already maps the common types, and RegisterSQLType covers types
// whose package you do not control.
type SQLTyper = types.SQLTyper

// RegisterSQLType records the column type a Go type maps to, keyed by dialect:
//
//	styx.RegisterSQLType(reflect.TypeOf(money.Amount{}), map[string]string{
//	    styx.DialectPostgres: "NUMERIC(19,4)",
//	    styx.DialectSQLite:   "REAL",
//	})
//
// Call it from an init function, before the first Sync.
func RegisterSQLType(t reflect.Type, byDialect map[string]string) {
	types.RegisterSQLType(t, byDialect)
}

// RegisterNamedSQLType records a portable name usable in a `type=` db tag, so
// the same tag resolves to the right column type on every dialect.
func RegisterNamedSQLType(name string, byDialect map[string]string) {
	types.RegisterNamedSQLType(name, byDialect)
}
