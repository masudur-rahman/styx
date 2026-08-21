package types

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// declaringType maps itself, exercising the SQLTyper escape hatch.
type declaringType string

func (declaringType) SQLType(dialect string) string {
	if dialect == DialectPostgres {
		return "INET"
	}
	return "TEXT"
}

func TestLookupSQLType_builtinRegistrations(t *testing.T) {
	tests := []struct {
		name     string
		typ      reflect.Type
		dialect  string
		want     string
		wantOK   bool
		skipDesc string
	}{
		{name: "styx UUID on postgres", typ: reflect.TypeOf(UUID("")), dialect: DialectPostgres, want: "UUID", wantOK: true},
		{name: "styx UUID on sqlite", typ: reflect.TypeOf(UUID("")), dialect: DialectSQLite, want: "TEXT", wantOK: true},
		{name: "pointer to UUID follows through", typ: reflect.TypeOf(new(UUID)), dialect: DialectPostgres, want: "UUID", wantOK: true},
		{name: "time on postgres", typ: reflect.TypeOf(time.Time{}), dialect: DialectPostgres, want: "TIMESTAMP WITH TIME ZONE", wantOK: true},
		{name: "time on sqlite", typ: reflect.TypeOf(time.Time{}), dialect: DialectSQLite, want: "DATETIME", wantOK: true},
		{name: "unregistered type", typ: reflect.TypeOf(""), dialect: DialectPostgres, wantOK: false},
		{name: "SQLTyper on postgres", typ: reflect.TypeOf(declaringType("")), dialect: DialectPostgres, want: "INET", wantOK: true},
		{name: "SQLTyper on sqlite", typ: reflect.TypeOf(declaringType("")), dialect: DialectSQLite, want: "TEXT", wantOK: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := LookupSQLType(tc.typ, tc.dialect)
			assert.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

// TestLookupSQLType_thirdPartyUUIDsRegisteredByName checks the registrations
// made by type key rather than by reflect.Type, which is how styx maps
// google/uuid and gofrs/uuid without importing either.
func TestLookupSQLType_thirdPartyUUIDsRegisteredByName(t *testing.T) {
	for _, key := range []string{
		"github.com/google/uuid.UUID",
		"github.com/gofrs/uuid.UUID",
	} {
		v, ok := sqlTypes.Load(key)
		require.True(t, ok, "expected %s to be registered", key)
		assert.Equal(t, "UUID", v.(map[string]string)[DialectPostgres])
		assert.Equal(t, "TEXT", v.(map[string]string)[DialectSQLite])
	}
}

func TestLookupNamedSQLType(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		dialect string
		want    string
	}{
		{name: "registered name maps per dialect", value: "uuid", dialect: DialectPostgres, want: "UUID"},
		{name: "registered name on sqlite", value: "uuid", dialect: DialectSQLite, want: "TEXT"},
		{name: "name is case insensitive", value: "UUID", dialect: DialectPostgres, want: "UUID"},
		{name: "unregistered passes through verbatim", value: "numeric(10,2)", dialect: DialectPostgres, want: "numeric(10,2)"},
		{name: "case of a raw type is preserved", value: "JSONB", dialect: DialectPostgres, want: "JSONB"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, LookupNamedSQLType(tc.value, tc.dialect))
		})
	}
}

func TestRegisterSQLType_roundTrip(t *testing.T) {
	type money struct{ Cents int64 }

	RegisterSQLType(reflect.TypeOf(money{}), map[string]string{
		DialectPostgres: "NUMERIC(19,4)",
		DialectSQLite:   "REAL",
	})

	got, ok := LookupSQLType(reflect.TypeOf(money{}), DialectPostgres)
	require.True(t, ok)
	assert.Equal(t, "NUMERIC(19,4)", got)

	got, ok = LookupSQLType(reflect.TypeOf(money{}), DialectSQLite)
	require.True(t, ok)
	assert.Equal(t, "REAL", got)
}

func TestRegisterNamedSQLType_roundTrip(t *testing.T) {
	RegisterNamedSQLType("ipaddr", map[string]string{
		DialectPostgres: "INET",
		DialectSQLite:   "TEXT",
	})

	assert.Equal(t, "INET", LookupNamedSQLType("ipaddr", DialectPostgres))
	assert.Equal(t, "INET", LookupNamedSQLType("IPAddr", DialectPostgres))
	assert.Equal(t, "TEXT", LookupNamedSQLType("ipaddr", DialectSQLite))
}

func TestTypeKey(t *testing.T) {
	assert.Equal(t, "github.com/masudur-rahman/styx/v2/types.UUID", TypeKey(reflect.TypeOf(UUID(""))))
	assert.Equal(t, "time.Time", TypeKey(reflect.TypeOf(time.Time{})))
	assert.Equal(t, "string", TypeKey(reflect.TypeOf("")))
}

func TestUUID_String(t *testing.T) {
	const raw = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	assert.Equal(t, raw, UUID(raw).String())
}
