package validation

import (
	"reflect"

	"github.com/masudur-rahman/styx/dberr"
)

// Validatable is implemented by types that carry custom or cross-field
// validation logic beyond struct tag rules. Its Validate method runs after tag
// rules pass.
type Validatable interface {
	Validate() error
}

// Validate checks a struct's fields against validate struct tags, then invokes
// the type's custom Validatable.Validate if implemented.
// Returns nil if valid, or a *dberr.ValidationError with per-field errors.
func Validate(doc any) error {
	val := reflect.ValueOf(doc)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil
	}

	fieldErrors := make(map[string][]string)
	valType := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := valType.Field(i)
		tag := field.Tag.Get("validate")
		if tag == "" {
			continue
		}

		rules := ParseRules(tag)
		fieldValue := val.Field(i).Interface()

		for _, rule := range rules {
			if msg := ApplyRule(rule, fieldValue, field.Name); msg != "" {
				fieldErrors[field.Name] = append(fieldErrors[field.Name], msg)
			}
		}
	}

	if len(fieldErrors) > 0 {
		return dberr.NewValidationError(fieldErrors)
	}

	if v, ok := doc.(Validatable); ok {
		return v.Validate()
	}
	return nil
}
