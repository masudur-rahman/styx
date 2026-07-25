package validation

import (
	"errors"
	"testing"

	"github.com/masudur-rahman/styx/dberr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tagUser struct {
	Name   string `validate:"required,min:2"`
	Email  string `validate:"email"`
	Age    int    `validate:"gt:0,lt:150"`
	Status string `validate:"oneof:active inactive"`
	Code   string `validate:"len:4"`
}

func TestValidate_tagRulesPass(t *testing.T) {
	err := Validate(&tagUser{Name: "al", Email: "a@b.com", Age: 30, Status: "active", Code: "abcd"})
	assert.NoError(t, err)
}

func TestValidate_tagRulesFail(t *testing.T) {
	err := Validate(&tagUser{Name: "a", Email: "bad", Age: 0, Status: "unknown", Code: "ab"})
	require.Error(t, err)
	assert.True(t, dberr.IsValidationError(err))

	var ve *dberr.ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Contains(t, ve.FieldErrors, "Name")
	assert.Contains(t, ve.FieldErrors, "Email")
	assert.Contains(t, ve.FieldErrors, "Age")
	assert.Contains(t, ve.FieldErrors, "Status")
	assert.Contains(t, ve.FieldErrors, "Code")
}

type customUser struct {
	Password string
	Confirm  string
}

func (u *customUser) Validate() error {
	if u.Password != u.Confirm {
		return errors.New("passwords do not match")
	}
	return nil
}

func TestValidate_customValidatableRuns(t *testing.T) {
	err := Validate(&customUser{Password: "a", Confirm: "b"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "passwords do not match")

	assert.NoError(t, Validate(&customUser{Password: "x", Confirm: "x"}))
}
