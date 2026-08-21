package core

import (
	"reflect"

	"github.com/masudur-rahman/styx/v2/types"
)

// Dialect names, re-exported so lib code need not import types directly.
const (
	DialectPostgres = types.DialectPostgres
	DialectSQLite   = types.DialectSQLite
)

// LookupSQLType returns the registered column type for t on the given dialect.
func LookupSQLType(t reflect.Type, dialect string) (string, bool) {
	return types.LookupSQLType(t, dialect)
}

// LookupNamedSQLType resolves a type= tag value against the registry, passing
// anything unregistered through as literal SQL.
func LookupNamedSQLType(name, dialect string) string {
	return types.LookupNamedSQLType(name, dialect)
}
