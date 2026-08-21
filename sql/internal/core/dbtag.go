package core

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// The attribute tokens a db tag may carry. Anything else is rejected, so a
// typo surfaces as an error rather than a silently missing constraint.
const (
	TokenPK       = "PK"
	TokenUnique   = "UQ"
	TokenUniqueS  = "UQS"
	TokenAutoIncr = "AUTOINCR"
	TokenNotNull  = "NOTNULL"
	TokenRequired = "REQ"
	TokenJSON     = "JSON"
	TokenArchive  = "ARCHIVE"
	TokenIndex    = "IDX"
	TokenUIndex   = "UIDX"

	// Association tokens taking a "prefix:value" argument. The association
	// kinds themselves are the RelationKind values, registered in init below
	// so the two cannot drift apart.
	TokenForeignKey = "FK"
	TokenReferences = "REF"
	TokenJoinTable  = "JOIN"
)

// knownTokens is the set of bare attribute tokens.
var knownTokens = map[string]bool{
	TokenPK: true, TokenUnique: true, TokenUniqueS: true,
	TokenAutoIncr: true, TokenNotNull: true, TokenRequired: true,
	TokenJSON: true, TokenArchive: true,
	TokenIndex: true, TokenUIndex: true,
}

// The association kinds are declared as RelationKind values, so they are
// registered from those rather than restated here. Restating them is what
// previously put "BELONGSTO" in this set while the tag actually reads "m2o".
func init() {
	for _, kind := range []RelationKind{RelationBelongsTo, RelationHasMany, RelationManyToMany} {
		knownTokens[strings.ToUpper(string(kind))] = true
	}
}

// knownPrefixes is the set of tokens taking a "prefix:value" argument.
var knownPrefixes = map[string]bool{
	TokenIndex:      true,
	TokenUIndex:     true,
	TokenForeignKey: true,
	TokenReferences: true,
	TokenJoinTable:  true,
}

// DBTag is the parsed form of a `db:"..."` struct tag.
//
// The grammar is one comma separating the column name from its attributes,
// and whitespace separating the attributes from each other:
//
//	db:"email,uq notnull"
//
// A second comma is an error rather than a silent truncation: the attributes
// past it used to be dropped without a word, so db:"id,pk,uq" produced a
// column with no unique constraint and no complaint.
type DBTag struct {
	// Name is the explicit column name, empty when the tag only sets attributes.
	Name string

	// Tokens are the upper-cased attribute tokens, in declaration order.
	Tokens []string

	// Ignore reports the db:"-" form, meaning the field is not a column.
	Ignore bool
}

// Has reports whether the tag carries a bare token.
func (t DBTag) Has(token string) bool {
	for _, tok := range t.Tokens {
		if tok == token {
			return true
		}
	}
	return false
}

// Arg returns the argument of the first "prefix:value" token with the given
// prefix, e.g. Arg("IDX") returns "by_email" for `idx:by_email`.
func (t DBTag) Arg(prefix string) (string, bool) {
	want := prefix + ":"
	for _, tok := range t.Tokens {
		if strings.HasPrefix(tok, want) {
			return strings.TrimPrefix(tok, want), true
		}
	}
	return "", false
}

// ParseDBTag parses the db tag of a struct field. A field with no db tag
// yields a zero DBTag and no error.
func ParseDBTag(field reflect.StructField) (DBTag, error) {
	dbTag := field.Tag.Get("db")
	if dbTag == "" {
		return DBTag{}, nil
	}
	if dbTag == "-" {
		return DBTag{Ignore: true}, nil
	}

	parts := strings.Split(dbTag, ",")
	if len(parts) > 2 {
		return DBTag{}, fmt.Errorf(
			"field %s: db tag %q has %d comma-separated sections, expected at most 2 "+
				"(the column name, then space-separated attributes such as %q)",
			field.Name, dbTag, len(parts), "id,pk uq")
	}

	tag := DBTag{Name: parts[0]}
	if len(parts) == 1 {
		return tag, nil
	}

	var errs []error
	for _, part := range strings.Fields(parts[1]) {
		token := strings.ToUpper(part)
		if err := validateToken(field, dbTag, token); err != nil {
			errs = append(errs, err)
			continue
		}
		tag.Tokens = append(tag.Tokens, token)
	}

	return tag, errors.Join(errs...)
}

// validateToken rejects tokens that are neither known bare tokens nor known
// prefixed ones.
func validateToken(field reflect.StructField, dbTag, token string) error {
	if knownTokens[token] {
		return nil
	}

	if prefix, arg, found := strings.Cut(token, ":"); found {
		if !knownPrefixes[prefix] {
			return fmt.Errorf("field %s: db tag %q has unknown option %q", field.Name, dbTag, token)
		}
		if arg == "" {
			return fmt.Errorf("field %s: db tag %q option %q needs a name after the colon",
				field.Name, dbTag, token)
		}
		return nil
	}

	return fmt.Errorf("field %s: db tag %q has unknown option %q", field.Name, dbTag, token)
}

// DBTagTokens returns just the attribute tokens, discarding any parse error.
// Callers that answer a yes/no question about a tag use this; the error is
// reported once, by the table-info walk.
func DBTagTokens(field reflect.StructField) []string {
	tag, _ := ParseDBTag(field)
	return tag.Tokens
}

// HasDBToken reports whether a field's db tag carries a token.
func HasDBToken(field reflect.StructField, token string) bool {
	tag, _ := ParseDBTag(field)
	return tag.Has(token)
}
