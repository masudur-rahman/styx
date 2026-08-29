package core

import (
	"fmt"
	"reflect"
	"strings"
)

// AssignID writes the id an INSERT returned back onto the document, so a
// caller holding the struct sees the key the database actually stored.
//
// The driver's value rarely has the field's type: lib/pq hands back a []byte
// of 36 characters for a uuid column, while a uuid.UUID field is [16]byte. A
// Scanner knows how to read that, so it is tried first — without it, the
// []byte-to-[16]byte conversion Go allows since 1.20 silently keeps the first
// 16 characters and leaves a corrupt id on the struct.
func AssignID(document any, id any) error {
	val := reflect.ValueOf(document)
	if val.Kind() != reflect.Ptr {
		// Not addressable, so there is nothing to write back to.
		return nil
	}

	valElem := val.Elem()
	if valElem.Kind() != reflect.Struct {
		return fmt.Errorf("document must be a pointer to a struct")
	}

	idField := fetchIDField(valElem)
	if !idField.IsValid() || !idField.CanSet() {
		return fmt.Errorf("ID field is not settable")
	}

	if handled, err := scanIntoField(idField, reflect.StructField{Name: "ID"}, id); handled {
		return err
	}

	idVal := reflect.ValueOf(id)
	if !idVal.IsValid() {
		return nil
	}

	target := idField.Type()
	if idField.Kind() == reflect.Ptr {
		target = target.Elem()
	}
	if !idVal.Type().AssignableTo(target) {
		if !convertible(idVal.Type(), target) {
			return fmt.Errorf("ID type %s cannot be assigned or converted to field type %s", idVal.Type(), target)
		}
		idVal = idVal.Convert(target)
	}

	if idField.Kind() == reflect.Ptr {
		ptr := reflect.New(target)
		ptr.Elem().Set(idVal)
		idField.Set(ptr)
		return nil
	}
	idField.Set(idVal)
	return nil
}

// convertible reports whether Go's conversion rules would produce the value
// the caller means, rather than merely compiling.
//
// Slice-to-array is the exception: Go allows it and takes the first len(array)
// elements, so converting a 36-byte uuid text into [16]byte truncates instead
// of failing. Anything that needs that conversion wants a Scanner.
func convertible(from, to reflect.Type) bool {
	if from.Kind() == reflect.Slice && (to.Kind() == reflect.Array || to.Kind() == reflect.Ptr) {
		return false
	}
	return from.ConvertibleTo(to)
}

// fetchIDField finds the field holding the primary key: the one mapped to an
// "id" column, else a field named ID.
func fetchIDField(valElem reflect.Value) reflect.Value {
	for _, f := range WalkFields(valElem.Type()) {
		dbTag := f.Tag.Get("db")
		if dbTag != "" {
			dbTag = strings.Split(dbTag, ",")[0]
		}
		if dbTag == "id" || f.Tag.Get("json") == "id" {
			return f.Value(valElem)
		}
	}

	for _, name := range []string{"ID", "Id"} {
		if field := valElem.FieldByName(name); field.IsValid() {
			return field
		}
	}
	return reflect.Value{}
}
