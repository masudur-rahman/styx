package core

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/iancoleman/strcase"
)

var rawMessageType = reflect.TypeOf(json.RawMessage{})

var timeType = reflect.TypeOf(time.Time{})

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

var (
	fieldMapCache  sync.Map // map[reflect.Type]map[string]int (index of field)
	tableNameCache sync.Map // map[reflect.Type]string
	pkColumnCache  sync.Map // map[reflect.Type]string
)

// GetTableName returns the database table name for a given struct, with caching.
func GetTableName(table interface{}) string {
	t := reflect.TypeOf(table)
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	if name, ok := tableNameCache.Load(t); ok {
		return name.(string)
	}

	// Default name from struct
	tableName := strcase.ToSnake(t.Name())
	if tableName == "" {
		// Try to get name from Type if t.Name() is empty (can happen with some reflect types)
		tableName = strcase.ToSnake(t.String())
		// t.String() might be "sqlite_test.User", we want "User"
		parts := strings.Split(tableName, ".")
		tableName = parts[len(parts)-1]
	}

	// Check for TableName() method
	// We need a value to call the method
	val := reflect.New(t)
	if method := val.MethodByName("TableName"); method.IsValid() {
		rs := method.Call([]reflect.Value{})
		tableName = rs[0].String()
	}

	tableNameCache.Store(t, tableName)
	return tableName
}

// GetDBFieldMap maps each column name to the field that backs it, with caching.
// Columns promoted out of an embedded struct carry a multi-element index path.
func GetDBFieldMap(doc any) map[string]FieldRef {
	t := reflect.TypeOf(doc)
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	if cache, ok := fieldMapCache.Load(t); ok {
		return cache.(map[string]FieldRef)
	}

	fieldMap := make(map[string]FieldRef)
	for _, f := range WalkFields(t) {
		fieldMap[GetFieldName(f.StructField)] = f
	}

	fieldMapCache.Store(t, fieldMap)
	return fieldMap
}

// GetPKColumn returns the primary key column name for a struct, with caching.
func GetPKColumn(table any) string {
	t := reflect.TypeOf(table)
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	if col, ok := pkColumnCache.Load(t); ok {
		return col.(string)
	}

	pkCol := "id" // default
	for _, f := range WalkFields(t) {
		tag, _ := ParseDBTag(f.StructField)
		if !tag.Has(TokenPK) {
			continue
		}
		pkCol = GetFieldName(f.StructField)
		break
	}

	pkColumnCache.Store(t, pkCol)
	return pkCol
}

var softDeleteCache sync.Map

// ExtractSoftDeleteColumn returns the column name tagged with archive, with caching.
func ExtractSoftDeleteColumn(table any) string {
	t := reflect.TypeOf(table)
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	if col, ok := softDeleteCache.Load(t); ok {
		return col.(string)
	}

	softDeleteCol := ""
	for _, f := range WalkFields(t) {
		tag, _ := ParseDBTag(f.StructField)
		if !tag.Has(TokenArchive) {
			continue
		}
		softDeleteCol = GetFieldName(f.StructField)
		break
	}

	softDeleteCache.Store(t, softDeleteCol)
	return softDeleteCol
}

// GetFieldName returns the database column name for a struct field.
func GetFieldName(field reflect.StructField) string {
	fieldName := field.Name
	if tag, _ := ParseDBTag(field); tag.Name != "" {
		fieldName = tag.Name
	}
	return strcase.ToSnake(fieldName)
}

// HasReqTag checks if a struct field has the "req" option in its db tag.
func HasReqTag(field reflect.StructField) bool {
	return HasDBToken(field, TokenRequired)
}

// HasJSONTag checks if a struct field has the "json" option in its db tag.
func HasJSONTag(field reflect.StructField) bool {
	return HasDBToken(field, TokenJSON)
}

// IsJSONField reports whether a struct field is stored as JSON in the
// database: either tagged with the "json" db option, or typed
// json.RawMessage (directly or behind a pointer).
func IsJSONField(field reflect.StructField) bool {
	if HasJSONTag(field) {
		return true
	}
	t := field.Type
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t == rawMessageType
}

// SQLArgValue returns the driver argument for a struct field value. JSON
// fields are passed as their textual form: drivers like lib/pq encode
// []byte as bytea, which JSON/JSONB columns reject, so raw bytes are
// converted to string and other values are marshaled.
func SQLArgValue(field reflect.StructField, value reflect.Value) any {
	if !IsJSONField(field) {
		return value.Interface()
	}

	v := value
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Type() == rawMessageType {
		raw := v.Bytes()
		if len(raw) == 0 {
			return nil
		}
		return string(raw)
	}

	data, err := json.Marshal(v.Interface())
	if err != nil {
		// Pass the original value through; the driver will surface the error.
		return value.Interface()
	}
	return string(data)
}

// setJSONField assigns a scanned JSON column value onto a struct field,
// unmarshaling into typed fields and copying raw bytes for json.RawMessage
// (drivers may reuse the scan buffer).
func setJSONField(field reflect.Value, rawVal any) error {
	var data []byte
	switch v := rawVal.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into JSON field", rawVal)
	}
	if len(data) == 0 {
		return nil
	}
	cp := append([]byte(nil), data...)

	t := field.Type()
	switch {
	case t == rawMessageType:
		field.Set(reflect.ValueOf(json.RawMessage(cp)))
		return nil
	case t.Kind() == reflect.Ptr && t.Elem() == rawMessageType:
		rm := json.RawMessage(cp)
		field.Set(reflect.ValueOf(&rm))
		return nil
	case t.Kind() == reflect.Ptr:
		nv := reflect.New(t.Elem())
		if err := json.Unmarshal(cp, nv.Interface()); err != nil {
			return err
		}
		field.Set(nv)
		return nil
	default:
		return json.Unmarshal(cp, field.Addr().Interface())
	}
}

// IsZeroValue checks if a value is its type's zero value.
func IsZeroValue(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	return v.IsZero()
}

// ScanRow scans a database row into a struct using cached field mapping.
func ScanRow(rows *sql.Rows, doc any) error {
	fields, err := rows.Columns()
	if err != nil {
		return err
	}

	fieldMap := GetDBFieldMap(doc)
	val := reflect.ValueOf(doc)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	scans := make([]any, len(fields))
	for i := range scans {
		scans[i] = &scans[i]
	}
	if err := rows.Scan(scans...); err != nil {
		return err
	}

	for idx, col := range fields {
		rawVal := scans[idx]
		if rawVal == nil {
			continue
		}

		// Columns aliased as "prefix.column" (e.g. from a JOIN) hydrate a
		// nested struct field named by the prefix.
		if prefix, inner, isNested := strings.Cut(col, "."); isNested {
			if err := setNestedField(val, prefix, inner, rawVal); err != nil {
				return err
			}
			continue
		}

		ref, ok := fieldMap[col]
		if !ok {
			continue
		}

		if err := setFieldValue(ref.Value(val), ref.StructField, rawVal); err != nil {
			return err
		}
	}
	return nil
}

// scanIntoField hydrates a field whose type implements sql.Scanner, allocating
// first when the field is a pointer. It reports whether it handled the field so
// callers can fall through to the ordinary conversion rules.
//
// Without this, a Scanner field is left at its zero value with no error
// reported, because the driver's raw value is neither assignable nor
// convertible to it: lib/pq returns a string for a uuid column, and uuid.UUID
// is [16]byte.
func scanIntoField(field reflect.Value, sf reflect.StructField, rawVal any) (bool, error) {
	var target reflect.Value
	switch {
	case field.Addr().Type().Implements(scannerType):
		target = field.Addr()
	case field.Kind() == reflect.Ptr && field.Type().Implements(scannerType):
		target = reflect.New(field.Type().Elem())
	default:
		return false, nil
	}

	if err := target.Interface().(sql.Scanner).Scan(rawVal); err != nil {
		return true, fmt.Errorf("scanning into field %s: %w", sf.Name, err)
	}

	if field.Kind() == reflect.Ptr {
		field.Set(target)
	}
	return true, nil
}

// setPointerField allocates and assigns into a pointer field, mirroring the
// assignable/convertible/time.Time rules used for non-pointer fields. Values it
// cannot map are left as a nil pointer.
func setPointerField(field, v reflect.Value, rawVal any) {
	elemType := field.Type().Elem()
	newVal := reflect.New(elemType)

	switch {
	case v.Type().AssignableTo(elemType):
		newVal.Elem().Set(v)
	case v.Type().ConvertibleTo(elemType):
		newVal.Elem().Set(v.Convert(elemType))
	case elemType == timeType:
		s, ok := rawVal.(string)
		if !ok {
			return
		}
		t, err := parseTime(s)
		if err != nil {
			return
		}
		newVal.Elem().Set(reflect.ValueOf(t))
	default:
		return
	}

	field.Set(newVal)
}

// setFieldValue assigns a scanned raw DB value onto a struct field, handling
// JSON columns, pointers, time.Time, bool coercion, and convertible types.
func setFieldValue(field reflect.Value, sf reflect.StructField, rawVal any) error {
	if !field.CanSet() {
		return nil
	}

	if IsJSONField(sf) {
		return setJSONField(field, rawVal)
	}

	if handled, err := scanIntoField(field, sf, rawVal); handled {
		return err
	}

	v := reflect.ValueOf(rawVal)
	switch {
	case v.Type().AssignableTo(field.Type()):
		field.Set(v)
	case field.Kind() == reflect.Ptr:
		setPointerField(field, v, rawVal)
	case field.Type() == timeType:
		if s, ok := rawVal.(string); ok {
			if t, err := parseTime(s); err == nil {
				field.Set(reflect.ValueOf(t))
			}
		}
	case field.Kind() == reflect.Bool:
		// SQLite stores BOOLEAN as INTEGER, so the driver returns int64;
		// int64 is not ConvertibleTo bool, so convert explicitly.
		field.SetBool(asBool(rawVal))
	case v.Type().ConvertibleTo(field.Type()):
		field.Set(v.Convert(field.Type()))
	}
	return nil
}

// setNestedField routes a "prefix.column" scanned value into the nested struct
// field named by prefix, allocating a pointer target if needed. Unknown
// prefixes are ignored so extra joined columns don't error.
func setNestedField(parent reflect.Value, prefix, inner string, rawVal any) error {
	fieldIdx, ok := nestedFieldIndex(parent.Type(), prefix)
	if !ok {
		return nil
	}

	nf := parent.Field(fieldIdx)
	if !nf.CanSet() {
		return nil
	}

	target := nf
	if target.Kind() == reflect.Ptr {
		if target.IsNil() {
			target.Set(reflect.New(target.Type().Elem()))
		}
		target = target.Elem()
	}
	if target.Kind() != reflect.Struct {
		return nil
	}

	innerMap := GetDBFieldMap(target.Addr().Interface())
	ref, ok := innerMap[inner]
	if !ok {
		return nil
	}
	return setFieldValue(ref.Value(target), ref.StructField, rawVal)
}

// nestedFieldIndex returns the index of a struct or pointer-to-struct field on t
// whose db column name matches prefix. JSON fields and time.Time are excluded,
// as they are scalar columns rather than joinable nested entities.
func nestedFieldIndex(t reflect.Type, prefix string) (int, bool) {
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() || IsJSONField(sf) {
			continue
		}
		ft := sf.Type
		for ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		if ft.Kind() != reflect.Struct || ft == timeType {
			continue
		}
		if GetFieldName(sf) == prefix {
			return i, true
		}
	}
	return 0, false
}

// asBool coerces a scanned SQL value into a Go bool. SQLite returns BOOLEAN
// columns as int64; other drivers may return bool, []byte or string.
func asBool(v any) bool {
	switch n := v.(type) {
	case bool:
		return n
	case int64:
		return n != 0
	case int:
		return n != 0
	case float64:
		return n != 0
	case []byte:
		return len(n) > 0 && (n[0] == '1' || n[0] == 't' || n[0] == 'T')
	case string:
		return n == "1" || n == "true" || n == "TRUE" || n == "True" || n == "t"
	default:
		return false
	}
}

func parseTime(s string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05",
		time.RFC3339,
		"2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse time: %s", s)
}
