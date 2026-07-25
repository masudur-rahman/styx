package core

import (
	"context"
	"reflect"

	"github.com/masudur-rahman/styx/sql"
)

// RunBeforeCreate invokes the BeforeCreate hook if doc implements it.
func RunBeforeCreate(ctx context.Context, doc any) error {
	if h, ok := doc.(sql.BeforeCreateHook); ok {
		return h.BeforeCreate(ctx)
	}
	return nil
}

// RunAfterCreate invokes the AfterCreate hook if doc implements it.
func RunAfterCreate(ctx context.Context, doc any) error {
	if h, ok := doc.(sql.AfterCreateHook); ok {
		return h.AfterCreate(ctx)
	}
	return nil
}

// RunBeforeUpdate invokes the BeforeUpdate hook if doc implements it.
func RunBeforeUpdate(ctx context.Context, doc any) error {
	if h, ok := doc.(sql.BeforeUpdateHook); ok {
		return h.BeforeUpdate(ctx)
	}
	return nil
}

// RunAfterUpdate invokes the AfterUpdate hook if doc implements it.
func RunAfterUpdate(ctx context.Context, doc any) error {
	if h, ok := doc.(sql.AfterUpdateHook); ok {
		return h.AfterUpdate(ctx)
	}
	return nil
}

// RunBeforeDelete invokes the BeforeDelete hook if doc implements it.
func RunBeforeDelete(ctx context.Context, doc any) error {
	if h, ok := doc.(sql.BeforeDeleteHook); ok {
		return h.BeforeDelete(ctx)
	}
	return nil
}

// RunAfterDelete invokes the AfterDelete hook if doc implements it.
func RunAfterDelete(ctx context.Context, doc any) error {
	if h, ok := doc.(sql.AfterDeleteHook); ok {
		return h.AfterDelete(ctx)
	}
	return nil
}

// RunAfterFind invokes the AfterFind hook if doc implements it.
func RunAfterFind(ctx context.Context, doc any) error {
	if h, ok := doc.(sql.AfterFindHook); ok {
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
