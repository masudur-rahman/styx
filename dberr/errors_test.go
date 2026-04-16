package dberr

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"ErrNotFound", ErrNotFound, true},
		{"DataNotFound alias", DataNotFound, true},
		{"wrapped ErrNotFound", fmt.Errorf("lookup: %w", ErrNotFound), true},
		{"wrapped DataNotFound", fmt.Errorf("lookup: %w", DataNotFound), true},
		{"unrelated error", errors.New("boom"), false},
		{"ErrDuplicateEntry", ErrDuplicateEntry, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsNotFound(tc.err))
		})
	}
}

func TestIsDuplicate(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"ErrDuplicateEntry", ErrDuplicateEntry, true},
		{"wrapped ErrDuplicateEntry", fmt.Errorf("insert: %w", ErrDuplicateEntry), true},
		{"ErrNotFound", ErrNotFound, false},
		{"unrelated error", errors.New("boom"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsDuplicate(tc.err))
		})
	}
}

func TestIsValidationError(t *testing.T) {
	ve := NewValidationError(map[string][]string{"email": {"required"}})
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"ValidationError", ve, true},
		{"wrapped ValidationError", fmt.Errorf("create: %w", ve), true},
		{"ErrValidationFailed sentinel", ErrValidationFailed, false},
		{"unrelated error", errors.New("boom"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsValidationError(tc.err))
		})
	}
}

func TestValidationError_Error(t *testing.T) {
	ve := NewValidationError(map[string][]string{
		"email": {"required"},
	})
	assert.Contains(t, ve.Error(), "email: required")
}

func TestSentinelErrorsAreDistinct(t *testing.T) {
	sentinels := []error{
		ErrNotFound,
		ErrDuplicateEntry,
		ErrInvalidQuery,
		ErrMissingWhereClause,
		ErrTransactionNotStarted,
		ErrTransactionAlreadyStarted,
		ErrInvalidEntityName,
		ErrInvalidID,
		ErrConnectionFailed,
		ErrValidationFailed,
	}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if i == j {
				continue
			}
			assert.NotEqual(t, a, b)
		}
	}
}
