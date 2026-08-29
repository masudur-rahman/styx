package core

import (
	"database/sql/driver"
	"reflect"
	"sync"
)

var (
	valuerType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
	walkCache  sync.Map // map[reflect.Type][]FieldRef
)

// FieldRef is one column-bearing field of a struct, flattened out of however
// many embedded structs it was nested inside.
type FieldRef struct {
	reflect.StructField

	// Index is the nested path to the field, for reflect.Value.FieldByIndex.
	// It has more than one element when the field came from an embedded struct.
	Index []int
}

// Value resolves the field on a struct value.
func (f FieldRef) Value(structVal reflect.Value) reflect.Value {
	if len(f.Index) == 1 {
		return structVal.Field(f.Index[0])
	}
	return structVal.FieldByIndex(f.Index)
}

// WalkFields returns every column-bearing field of a struct type, descending
// into embedded (anonymous) struct fields so their fields become columns of the
// outer table.
//
// Shadowing follows Go's own rule, by column name rather than by field name: a
// column declared at a shallower depth hides one of the same name declared
// deeper, so an outer CreatedBy overrides an embedded Auditable.CreatedBy.
//
// Embedded pointers to structs are skipped. Supporting them means deciding
// whether to allocate on scan, and reflect.Value.FieldByIndex panics rather
// than allocating when it meets a nil pointer along the way.
func WalkFields(t reflect.Type) []FieldRef {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	if cached, ok := walkCache.Load(t); ok {
		return cached.([]FieldRef)
	}

	fields := walkFields(t, nil, map[string]bool{})
	walkCache.Store(t, fields)
	return fields
}

// walkFields collects one level and recurses, breadth-first within the level so
// that shallower columns are seen before the deeper ones they shadow.
func walkFields(t reflect.Type, prefix []int, seen map[string]bool) []FieldRef {
	var fields []FieldRef
	var embedded []reflect.StructField

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if IsIgnoredField(sf) {
			continue
		}

		// Embedding is checked before exportedness: an embedded struct whose
		// *type* is unexported still promotes its exported fields, and reflect
		// permits both reading and setting through it.
		if isEmbeddedStruct(sf) {
			embedded = append(embedded, sf)
			continue
		}
		if !sf.IsExported() || IsRelationField(sf) {
			continue
		}

		col := GetFieldName(sf)
		if seen[col] {
			continue
		}
		seen[col] = true
		fields = append(fields, FieldRef{StructField: sf, Index: appendIndex(prefix, sf.Index[0])})
	}

	for _, sf := range embedded {
		fields = append(fields, walkFields(sf.Type, appendIndex(prefix, sf.Index[0]), seen)...)
	}

	return fields
}

// isEmbeddedStruct reports whether a field should be descended into rather than
// treated as a column of its own.
//
// A struct that knows how to be a single column is a leaf: one registered in
// the type registry (time.Time), one implementing sql.Scanner or driver.Valuer,
// and one carrying a db tag, which is an explicit statement that it is a
// column. A struct that is itself a table is a relation, not part of this one.
func isEmbeddedStruct(sf reflect.StructField) bool {
	if !sf.Anonymous || sf.Type.Kind() != reflect.Struct {
		return false
	}
	if _, tagged := sf.Tag.Lookup("db"); tagged {
		return false
	}
	if IsJSONField(sf) || isColumnStruct(sf.Type) {
		return false
	}
	return !hasTableName(sf.Type)
}

// isColumnStruct reports whether a struct type maps to a single column.
func isColumnStruct(t reflect.Type) bool {
	if _, ok := LookupSQLType(t, DialectPostgres); ok {
		return true
	}
	ptr := reflect.PointerTo(t)
	return ptr.Implements(scannerType) || t.Implements(valuerType) || ptr.Implements(valuerType)
}

// hasTableName reports whether a type declares TableName, which marks it as a
// table in its own right rather than a group of columns.
func hasTableName(t reflect.Type) bool {
	if _, ok := t.MethodByName("TableName"); ok {
		return true
	}
	_, ok := reflect.PointerTo(t).MethodByName("TableName")
	return ok
}

// appendIndex returns prefix with i appended, without aliasing prefix's array.
func appendIndex(prefix []int, i int) []int {
	out := make([]int, len(prefix), len(prefix)+1)
	copy(out, prefix)
	return append(out, i)
}
