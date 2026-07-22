package sql

import (
	"context"
	"reflect"
)

// Lifecycle hook interfaces. A document type may implement any subset of these;
// engine implementations invoke the matching hook around the corresponding
// operation. Implementing none costs a single type assertion per call.

// BeforeCreateHook runs before a document is inserted. A non-nil error aborts the insert.
type BeforeCreateHook interface {
	BeforeCreate(ctx context.Context) error
}

// AfterCreateHook runs after a document is successfully inserted.
type AfterCreateHook interface {
	AfterCreate(ctx context.Context) error
}

// BeforeUpdateHook runs before a document is updated. A non-nil error aborts the update.
type BeforeUpdateHook interface {
	BeforeUpdate(ctx context.Context) error
}

// AfterUpdateHook runs after a document is successfully updated.
type AfterUpdateHook interface {
	AfterUpdate(ctx context.Context) error
}

// BeforeDeleteHook runs before a document is deleted. A non-nil error aborts the delete.
type BeforeDeleteHook interface {
	BeforeDelete(ctx context.Context) error
}

// AfterDeleteHook runs after a document is successfully deleted.
type AfterDeleteHook interface {
	AfterDelete(ctx context.Context) error
}

// AfterFindHook runs after a document is scanned from a query result.
type AfterFindHook interface {
	AfterFind(ctx context.Context) error
}

// RunBeforeCreate invokes the BeforeCreate hook if doc implements it.
func RunBeforeCreate(ctx context.Context, doc any) error {
	if h, ok := doc.(BeforeCreateHook); ok {
		return h.BeforeCreate(ctx)
	}
	return nil
}

// RunAfterCreate invokes the AfterCreate hook if doc implements it.
func RunAfterCreate(ctx context.Context, doc any) error {
	if h, ok := doc.(AfterCreateHook); ok {
		return h.AfterCreate(ctx)
	}
	return nil
}

// RunBeforeUpdate invokes the BeforeUpdate hook if doc implements it.
func RunBeforeUpdate(ctx context.Context, doc any) error {
	if h, ok := doc.(BeforeUpdateHook); ok {
		return h.BeforeUpdate(ctx)
	}
	return nil
}

// RunAfterUpdate invokes the AfterUpdate hook if doc implements it.
func RunAfterUpdate(ctx context.Context, doc any) error {
	if h, ok := doc.(AfterUpdateHook); ok {
		return h.AfterUpdate(ctx)
	}
	return nil
}

// RunBeforeDelete invokes the BeforeDelete hook if doc implements it.
func RunBeforeDelete(ctx context.Context, doc any) error {
	if h, ok := doc.(BeforeDeleteHook); ok {
		return h.BeforeDelete(ctx)
	}
	return nil
}

// RunAfterDelete invokes the AfterDelete hook if doc implements it.
func RunAfterDelete(ctx context.Context, doc any) error {
	if h, ok := doc.(AfterDeleteHook); ok {
		return h.AfterDelete(ctx)
	}
	return nil
}

// RunAfterFind invokes the AfterFind hook if doc implements it.
func RunAfterFind(ctx context.Context, doc any) error {
	if h, ok := doc.(AfterFindHook); ok {
		return h.AfterFind(ctx)
	}
	return nil
}

// RunAfterFindResults invokes AfterFind for every scanned result. documents may
// be a pointer to a struct or a pointer to a slice of structs.
func RunAfterFindResults(ctx context.Context, documents any) error {
	v := reflect.ValueOf(documents)
	if v.Kind() != reflect.Ptr {
		return RunAfterFind(ctx, documents)
	}
	elem := v.Elem()
	if elem.Kind() != reflect.Slice {
		return RunAfterFind(ctx, documents)
	}
	for i := 0; i < elem.Len(); i++ {
		item := elem.Index(i)
		if item.CanAddr() {
			if err := RunAfterFind(ctx, item.Addr().Interface()); err != nil {
				return err
			}
		} else if err := RunAfterFind(ctx, item.Interface()); err != nil {
			return err
		}
	}
	return nil
}
