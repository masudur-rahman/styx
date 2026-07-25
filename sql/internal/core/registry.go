package core

import "sync"

// softDeleteByTable maps a table name to its soft-delete column. It is populated
// at Sync time so scalar operations like Count can honor soft delete without
// being handed a model struct.
var softDeleteByTable sync.Map

// RegisterSoftDeleteColumn records the soft-delete column for a table name. An
// empty col records that the table has no soft-delete column.
func RegisterSoftDeleteColumn(table, col string) {
	softDeleteByTable.Store(table, col)
}

// SoftDeleteColumnForTable returns the soft-delete column registered for a table
// name, or "" if the table has none or was never synced.
func SoftDeleteColumnForTable(table string) string {
	if col, ok := softDeleteByTable.Load(table); ok {
		return col.(string)
	}
	return ""
}
