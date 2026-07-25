package sql

import "context"

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
