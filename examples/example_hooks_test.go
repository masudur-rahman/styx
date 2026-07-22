package examples_test

import (
	"context"
	"fmt"
)

// auditLog records hook invocations. Real models would perform side effects
// (set timestamps, emit events) inside the hooks instead.
var auditLog []string

// AuditRecord implements the BeforeCreate and AfterFind lifecycle hooks.
type AuditRecord struct {
	ID   int64  `db:"id,pk autoincr"`
	Name string `db:"name"`
}

// BeforeCreate runs before the record is inserted.
func (a *AuditRecord) BeforeCreate(ctx context.Context) error {
	auditLog = append(auditLog, "before-create:"+a.Name)
	return nil
}

// AfterFind runs after the record is scanned from a query.
func (a *AuditRecord) AfterFind(ctx context.Context) error {
	auditLog = append(auditLog, "after-find:"+a.Name)
	return nil
}

// Example_lifecycleHooks shows model hooks firing around write and read operations.
func Example_lifecycleHooks() {
	auditLog = nil
	db := openDB()
	db.Sync(ctx, AuditRecord{})

	db.Table("audit_record").InsertOne(ctx, &AuditRecord{Name: "alice"})

	var r AuditRecord
	db.Table("audit_record").ID(1).FindOne(ctx, &r)

	for _, entry := range auditLog {
		fmt.Println(entry)
	}

	// Output:
	// before-create:alice
	// after-find:alice
}
