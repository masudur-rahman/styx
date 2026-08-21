---
globs: "**/*_test.go"
---

# Testing Rules

- Every new exported function gets a test. No exceptions.
- Test name format: `TestFunctionName_condition` (e.g., `TestInsertOne_duplicateKey`).
- Use table-driven tests for multiple input/output scenarios.
- No test interdependencies. Each test sets up its own state.
- Mock external services (databases, APIs), not internal logic.
- Use `testify/assert` and `testify/require` for assertions (already a dependency).
- Prefer integration tests for database engine methods, unit tests for utilities/helpers.
- Test files live alongside the code they test (`foo.go` -> `foo_test.go`).
