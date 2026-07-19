package sql

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
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
