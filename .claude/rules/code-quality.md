---
globs: "**/*.go"
---

# Code Quality Rules

- Every function has a single responsibility.
- No function exceeds 50 lines (excluding struct definitions and table-driven tests). Extract if it does.
- No file exceeds 300 lines. Split if it does.
- All exported functions/methods have Go doc comments (name + purpose).
- Use concrete types or well-defined interfaces. Minimize `interface{}` / `any` usage.
- Imports: stdlib → external libs → internal packages. Blank line between groups.
- No magic numbers/strings. Extract to named constants.
- Prefer early returns over nested conditionals.
- Error handling: always check returned errors; wrap with context using `fmt.Errorf("...: %w", err)`.
- Receiver names: short, consistent, typically first letter of type name.
