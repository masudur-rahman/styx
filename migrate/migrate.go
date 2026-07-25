// Package migrate provides a small versioned migration runner for destructive
// or order-sensitive schema changes that the engine's additive Sync will not
// perform. Migrations are plain Go functions with an Up and a Down, applied in
// version order and recorded in a schema_migrations table so each runs once.
package migrate

import (
	"context"
	"fmt"
	"sort"
	"strings"

	isql "github.com/masudur-rahman/styx/v2/sql"
)

const (
	tableName       = "schema_migrations"
	createTableStmt = `CREATE TABLE IF NOT EXISTS schema_migrations (` +
		`version BIGINT PRIMARY KEY, name TEXT, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`
)

// Func is a single migration step. It receives a transaction-scoped Engine, so
// its schema changes and the version bookkeeping commit or roll back together.
type Func func(ctx context.Context, e isql.Engine) error

// Migration is one versioned schema change. Version must be unique and is the
// ordering key; Up applies the change and Down reverses it.
type Migration struct {
	Version int64
	Name    string
	Up      Func
	Down    Func
}

// Status reports whether a registered migration has been applied.
type Status struct {
	Version int64
	Name    string
	Applied bool
}

// Migrator applies and reverses a set of registered migrations against an Engine.
type Migrator struct {
	engine     isql.Engine
	migrations []Migration
}

// New returns a Migrator bound to engine.
func New(engine isql.Engine) *Migrator {
	return &Migrator{engine: engine}
}

// Register adds migrations and keeps them ordered by version. It returns the
// Migrator for chaining.
func (m *Migrator) Register(migs ...Migration) *Migrator {
	m.migrations = append(m.migrations, migs...)
	sort.Slice(m.migrations, func(i, j int) bool {
		return m.migrations[i].Version < m.migrations[j].Version
	})
	return m
}

// Up applies every registered migration that has not yet been applied, in
// version order. Each migration runs in its own transaction.
func (m *Migrator) Up(ctx context.Context) error {
	if err := m.ensureTable(ctx); err != nil {
		return err
	}
	applied, err := m.appliedVersions(ctx)
	if err != nil {
		return err
	}
	for _, mig := range m.migrations {
		if applied[mig.Version] {
			continue
		}
		if err := m.apply(ctx, mig); err != nil {
			return fmt.Errorf("migrate up %d %q: %w", mig.Version, mig.Name, err)
		}
	}
	return nil
}

// Down reverses the most recently applied migration. It is a no-op when nothing
// has been applied.
func (m *Migrator) Down(ctx context.Context) error {
	if err := m.ensureTable(ctx); err != nil {
		return err
	}
	applied, err := m.appliedVersions(ctx)
	if err != nil {
		return err
	}
	mig, ok := m.latestApplied(applied)
	if !ok {
		return nil
	}
	if err := m.revert(ctx, mig); err != nil {
		return fmt.Errorf("migrate down %d %q: %w", mig.Version, mig.Name, err)
	}
	return nil
}

// Status returns the applied state of every registered migration, in order.
func (m *Migrator) Status(ctx context.Context) ([]Status, error) {
	if err := m.ensureTable(ctx); err != nil {
		return nil, err
	}
	applied, err := m.appliedVersions(ctx)
	if err != nil {
		return nil, err
	}
	statuses := make([]Status, len(m.migrations))
	for i, mig := range m.migrations {
		statuses[i] = Status{Version: mig.Version, Name: mig.Name, Applied: applied[mig.Version]}
	}
	return statuses, nil
}

// apply runs a migration's Up and records its version in one transaction.
func (m *Migrator) apply(ctx context.Context, mig Migration) error {
	return m.inTx(ctx, func(tx isql.Engine) error {
		if err := mig.Up(ctx, tx); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, recordStmt(mig.Version, mig.Name))
		return err
	})
}

// revert runs a migration's Down and removes its version in one transaction.
func (m *Migrator) revert(ctx context.Context, mig Migration) error {
	return m.inTx(ctx, func(tx isql.Engine) error {
		if mig.Down != nil {
			if err := mig.Down(ctx, tx); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, deleteStmt(mig.Version))
		return err
	})
}

// inTx runs fn inside a transaction, committing on success and rolling back on
// any error.
func (m *Migrator) inTx(ctx context.Context, fn func(tx isql.Engine) error) error {
	tx, err := m.engine.BeginTx(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// latestApplied returns the registered migration with the highest applied version.
func (m *Migrator) latestApplied(applied map[int64]bool) (Migration, bool) {
	var (
		latest Migration
		found  bool
	)
	for _, mig := range m.migrations {
		if applied[mig.Version] && (!found || mig.Version > latest.Version) {
			latest, found = mig, true
		}
	}
	return latest, found
}

func (m *Migrator) ensureTable(ctx context.Context) error {
	_, err := m.engine.Exec(ctx, createTableStmt)
	return err
}

func (m *Migrator) appliedVersions(ctx context.Context) (map[int64]bool, error) {
	rows, err := m.engine.Query(ctx, "SELECT version FROM "+tableName+" ORDER BY version ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := map[int64]bool{}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

// recordStmt builds the INSERT that records an applied version. version is an
// int64 and name is developer-authored, so literal interpolation is safe; the
// name is still single-quote escaped.
func recordStmt(version int64, name string) string {
	return fmt.Sprintf("INSERT INTO %s (version, name) VALUES (%d, '%s')",
		tableName, version, strings.ReplaceAll(name, "'", "''"))
}

func deleteStmt(version int64) string {
	return fmt.Sprintf("DELETE FROM %s WHERE version = %d", tableName, version)
}
