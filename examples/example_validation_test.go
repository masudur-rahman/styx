package examples_test

import (
	"errors"
	"fmt"
)

// Signup uses tag-based validation rules plus a custom Validatable check.
type Signup struct {
	ID       int64  `db:"id,pk autoincr"`
	Email    string `db:"email" validate:"required,email"`
	Age      int    `db:"age" validate:"gt:0,lt:150"`
	Role     string `db:"role" validate:"oneof:admin user"`
	Password string `db:"password" validate:"min:8"`
	Confirm  string `db:"confirm"`
}

// Validate is the custom cross-field rule, run after tag rules pass.
func (s *Signup) Validate() error {
	if s.Password != s.Confirm {
		return errors.New("password confirmation does not match")
	}
	return nil
}

// Example_validation shows tag rules and a custom Validatable implementation.
// Validation runs only when EnableValidation(true) is set.
func Example_validation() {
	db := openDB()
	db.Sync(ctx, Signup{})

	// Fails tag rules: bad email, age out of range, disallowed role, short password.
	_, err := db.Table("signup").EnableValidation(true).InsertOne(ctx,
		&Signup{Email: "not-an-email", Age: 200, Role: "root", Password: "short"})
	fmt.Println("tag rules failed:", err != nil)

	// Tags pass, but the custom cross-field check rejects mismatched passwords.
	_, err = db.Table("signup").EnableValidation(true).InsertOne(ctx,
		&Signup{Email: "a@b.com", Age: 30, Role: "user", Password: "longenough", Confirm: "different"})
	fmt.Println("custom rule error:", err)

	// Output:
	// tag rules failed: true
	// custom rule error: password confirmation does not match
}
