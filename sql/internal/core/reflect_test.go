package core

import (
	stdsql "database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

func TestAsBool_conversions(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want bool
	}{
		{"bool true", true, true},
		{"bool false", false, false},
		{"int64 one", int64(1), true},
		{"int64 zero", int64(0), false},
		{"int one", int(1), true},
		{"int zero", int(0), false},
		{"float nonzero", float64(1), true},
		{"float zero", float64(0), false},
		{"bytes one", []byte("1"), true},
		{"bytes zero", []byte("0"), false},
		{"bytes true", []byte("true"), true},
		{"bytes empty", []byte{}, false},
		{"bytes null byte", []byte{0x00}, false},
		{"string 1", "1", true},
		{"string true", "true", true},
		{"string TRUE", "TRUE", true},
		{"string True", "True", true},
		{"string t", "t", true},
		{"string 0", "0", false},
		{"string empty", "", false},
		{"unknown type", 3.14i, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, asBool(tt.in))
		})
	}
}

type jsonAddress struct {
	Street string `json:"street"`
	City   string `json:"city"`
}

type jsonTagDoc struct {
	ID      int64            `db:"id,pk autoincr"`
	Name    string           `db:"name"`
	Payload json.RawMessage  `db:"payload"`
	Address jsonAddress      `db:"address,json"`
	Extra   *jsonAddress     `db:"extra,json"`
	RawPtr  *json.RawMessage `db:"raw_ptr"`
	NotJSON []byte           `db:"not_json"`
}

func field(t *testing.T, name string) reflect.StructField {
	t.Helper()
	f, ok := reflect.TypeOf(jsonTagDoc{}).FieldByName(name)
	assert.True(t, ok)
	return f
}

func TestHasJSONTag(t *testing.T) {
	assert.True(t, HasJSONTag(field(t, "Address")))
	assert.True(t, HasJSONTag(field(t, "Extra")))
	assert.False(t, HasJSONTag(field(t, "Payload")))
	assert.False(t, HasJSONTag(field(t, "Name")))
	assert.False(t, HasJSONTag(field(t, "NotJSON")))
}

func TestIsJSONField(t *testing.T) {
	assert.True(t, IsJSONField(field(t, "Address")), "json tag")
	assert.True(t, IsJSONField(field(t, "Payload")), "json.RawMessage")
	assert.True(t, IsJSONField(field(t, "RawPtr")), "*json.RawMessage")
	assert.False(t, IsJSONField(field(t, "Name")))
	assert.False(t, IsJSONField(field(t, "NotJSON")), "plain []byte is not JSON")
}

func TestSQLArgValue(t *testing.T) {
	doc := jsonTagDoc{
		Name:    "alice",
		Payload: json.RawMessage(`{"a":1}`),
		Address: jsonAddress{Street: "Road 1", City: "Dhaka"},
	}
	v := reflect.ValueOf(doc)

	t.Run("non-JSON passes through", func(t *testing.T) {
		assert.Equal(t, "alice", SQLArgValue(field(t, "Name"), v.FieldByName("Name")))
	})
	t.Run("RawMessage becomes string", func(t *testing.T) {
		assert.Equal(t, `{"a":1}`, SQLArgValue(field(t, "Payload"), v.FieldByName("Payload")))
	})
	t.Run("json-tagged struct marshaled", func(t *testing.T) {
		assert.Equal(t, `{"street":"Road 1","city":"Dhaka"}`, SQLArgValue(field(t, "Address"), v.FieldByName("Address")))
	})
	t.Run("nil pointer becomes NULL", func(t *testing.T) {
		assert.Nil(t, SQLArgValue(field(t, "Extra"), v.FieldByName("Extra")))
	})
	t.Run("empty RawMessage becomes NULL", func(t *testing.T) {
		empty := jsonTagDoc{}
		assert.Nil(t, SQLArgValue(field(t, "Payload"), reflect.ValueOf(empty).FieldByName("Payload")))
	})
}

func TestSetJSONField(t *testing.T) {
	t.Run("unmarshal into struct", func(t *testing.T) {
		var doc jsonTagDoc
		f := reflect.ValueOf(&doc).Elem().FieldByName("Address")
		assert.NoError(t, setJSONField(f, []byte(`{"street":"Road 9","city":"Dhaka"}`)))
		assert.Equal(t, jsonAddress{Street: "Road 9", City: "Dhaka"}, doc.Address)
	})
	t.Run("unmarshal into pointer", func(t *testing.T) {
		var doc jsonTagDoc
		f := reflect.ValueOf(&doc).Elem().FieldByName("Extra")
		assert.NoError(t, setJSONField(f, `{"city":"Ctg"}`))
		assert.Equal(t, "Ctg", doc.Extra.City)
	})
	t.Run("RawMessage copies bytes", func(t *testing.T) {
		var doc jsonTagDoc
		buf := []byte(`{"a":1}`)
		f := reflect.ValueOf(&doc).Elem().FieldByName("Payload")
		assert.NoError(t, setJSONField(f, buf))
		buf[0] = 'X' // driver may reuse the buffer
		assert.Equal(t, json.RawMessage(`{"a":1}`), doc.Payload)
	})
	t.Run("invalid json errors", func(t *testing.T) {
		var doc jsonTagDoc
		f := reflect.ValueOf(&doc).Elem().FieldByName("Address")
		assert.Error(t, setJSONField(f, []byte(`{invalid`)))
	})
	t.Run("empty value is a no-op", func(t *testing.T) {
		var doc jsonTagDoc
		f := reflect.ValueOf(&doc).Elem().FieldByName("Address")
		assert.NoError(t, setJSONField(f, []byte{}))
		assert.Equal(t, jsonAddress{}, doc.Address)
	})
}

type joinAuthor struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

type joinBook struct {
	ID     int64      `db:"id"`
	Title  string     `db:"title"`
	Author joinAuthor `db:"author"`
}

func TestScanRow_nestedJoinHydration(t *testing.T) {
	db, err := stdsql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE author(id INTEGER, name TEXT)`)
	mustExec(t, db, `CREATE TABLE book(id INTEGER, title TEXT, author_id INTEGER)`)
	mustExec(t, db, `INSERT INTO author VALUES (1,'alice')`)
	mustExec(t, db, `INSERT INTO book VALUES (10,'go in action',1)`)

	rows, err := db.Query(`SELECT book.id AS id, book.title AS title,
		author.id AS "author.id", author.name AS "author.name"
		FROM book JOIN author ON author.id = book.author_id`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())

	var b joinBook
	require.NoError(t, ScanRow(rows, &b))

	assert.Equal(t, int64(10), b.ID)
	assert.Equal(t, "go in action", b.Title)
	assert.Equal(t, int64(1), b.Author.ID)
	assert.Equal(t, "alice", b.Author.Name)
}

func mustExec(t *testing.T, db *stdsql.DB, query string) {
	t.Helper()
	_, err := db.Exec(query)
	require.NoError(t, err)
}

// uuidLike mimics google/uuid.UUID: a [16]byte with pointer-receiver Scan.
type uuidLike [16]byte

func (u *uuidLike) Scan(src any) error {
	s, ok := src.(string)
	if !ok {
		return fmt.Errorf("uuidLike: cannot scan %T", src)
	}
	if len(s) != 32 {
		return fmt.Errorf("uuidLike: bad length %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	copy(u[:], b)
	return nil
}

// failingScanner always errors, to prove Scan errors propagate.
type failingScanner struct{}

func (failingScanner) Scan(any) error { return errors.New("boom") }

func TestSetFieldValue_scanner(t *testing.T) {
	const hexID = "6ba7b8109dad11d180b400c04fd430c8"
	var wantUUID uuidLike
	b, err := hex.DecodeString(hexID)
	require.NoError(t, err)
	copy(wantUUID[:], b)

	type row struct {
		ID    uuidLike
		IDPtr *uuidLike
		Plain string
		Num   int64
	}

	tests := []struct {
		name    string
		field   string
		raw     any
		wantErr string
		check   func(t *testing.T, r row)
	}{
		{
			name:  "value receiver field is hydrated",
			field: "ID",
			raw:   hexID,
			check: func(t *testing.T, r row) { assert.Equal(t, wantUUID, r.ID) },
		},
		{
			name:  "pointer field is allocated and hydrated",
			field: "IDPtr",
			raw:   hexID,
			check: func(t *testing.T, r row) {
				require.NotNil(t, r.IDPtr)
				assert.Equal(t, wantUUID, *r.IDPtr)
			},
		},
		{
			name:    "scan error propagates",
			field:   "ID",
			raw:     "not-hex",
			wantErr: "bad length",
		},
		{
			name:    "wrong source type propagates",
			field:   "ID",
			raw:     int64(42),
			wantErr: "cannot scan int64",
		},
		{
			name:  "non-scanner string field is unaffected",
			field: "Plain",
			raw:   "hello",
			check: func(t *testing.T, r row) { assert.Equal(t, "hello", r.Plain) },
		},
		{
			name:  "non-scanner int field is unaffected",
			field: "Num",
			raw:   int64(7),
			check: func(t *testing.T, r row) { assert.Equal(t, int64(7), r.Num) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var r row
			rv := reflect.ValueOf(&r).Elem()
			idx, ok := rv.Type().FieldByName(tc.field)
			require.True(t, ok)

			err := setFieldValue(rv.FieldByName(tc.field), idx, tc.raw)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			tc.check(t, r)
		})
	}
}

func TestSetFieldValue_scannerErrorIsReturned(t *testing.T) {
	type row struct{ F failingScanner }

	var r row
	rv := reflect.ValueOf(&r).Elem()
	sf, ok := rv.Type().FieldByName("F")
	require.True(t, ok)

	err := setFieldValue(rv.FieldByName("F"), sf, "anything")
	require.Error(t, err)
	assert.ErrorContains(t, err, "scanning into field F")
	assert.ErrorContains(t, err, "boom")
}

func TestSetFieldValue_jsonTagWinsOverScanner(t *testing.T) {
	type row struct {
		Data uuidLike `db:"data,json"`
	}

	var r row
	rv := reflect.ValueOf(&r).Elem()
	sf, ok := rv.Type().FieldByName("Data")
	require.True(t, ok)

	// A json-tagged field must route to setJSONField, not the Scanner branch;
	// the Scanner would reject this input as a bad length.
	err := setFieldValue(rv.FieldByName("Data"), sf, []byte(`"not a uuid"`))
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "bad length")
}

func TestSetFieldValue_unexportedFieldIsSkipped(t *testing.T) {
	type row struct {
		id uuidLike //nolint:unused // exercises the CanSet guard
	}

	var r row
	rv := reflect.ValueOf(&r).Elem()
	sf, ok := rv.Type().FieldByName("id")
	require.True(t, ok)

	require.NoError(t, setFieldValue(rv.FieldByName("id"), sf, "whatever"))
}
