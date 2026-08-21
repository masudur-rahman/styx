// Package sqltest holds the shared struct corpus and golden-file comparator
// used by the dialect libraries' SQL-generation tests.
//
// The corpus and the golden files are the contract that lets the postgres and
// sqlite libraries be merged behind a single Dialect implementation: whatever
// SQL they emit today must still be emitted, byte for byte, afterwards.
package sqltest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/masudur-rahman/styx/v2/types"
)

// Scalars exercises every reflect.Kind that getSQLType recognises, in both
// value and pointer form.
type Scalars struct {
	Int     int
	Int32   int32
	Int64   int64
	Uint64  uint64
	Float32 float32
	Float64 float64
	Bool    bool
	String  string
	Bytes   []byte
	Time    time.Time

	IntPtr    *int
	Int64Ptr  *int64
	StringPtr *string
	BoolPtr   *bool
	TimePtr   *time.Time
}

// Constraints exercises every constraint token the db tag understands.
type Constraints struct {
	ID        int64  `db:"id,pk autoincr"`
	Email     string `db:"email,uq"`
	Name      string `db:"name,notnull"`
	Slug      string `db:"slug,uq notnull"`
	Required  string `db:"required,req"`
	Renamed   string `db:"custom_column"`
	Untagged  string
	Ignored   string `db:"-"`
	EmptyName string `db:","`
}

// CompositeUnique exercises multi-column unique groups via the uqs token.
type CompositeUnique struct {
	ID     int64  `db:"id,pk"`
	Tenant string `db:"tenant,uqs"`
	Code   string `db:"code,uqs"`
	Note   string `db:"note"`
}

// Indexed exercises every index tag form: unnamed, named, unique, and a named
// group spanning two columns.
type Indexed struct {
	ID       int64  `db:"id,pk"`
	Email    string `db:"email,uidx"`
	Status   string `db:"status,idx"`
	Tenant   string `db:"tenant,idx:tenant_code"`
	Code     string `db:"code,idx:tenant_code"`
	Region   string `db:"region,uidx:region_zone"`
	Zone     string `db:"zone,uidx:region_zone"`
	Untagged string
}

// SoftDeletable exercises soft-delete column detection.
type SoftDeletable struct {
	ID        int64      `db:"id,pk"`
	Name      string     `db:"name"`
	DeletedAt *time.Time `db:"deleted_at"`
}

// JSONDoc exercises the json tag, which overrides the column type.
type JSONDoc struct {
	ID      int64           `db:"id,pk"`
	Payload map[string]any  `db:"payload,json"`
	Raw     json.RawMessage `db:"raw,json"`
}

// StringPK exercises a non-integer primary key, where autoincr must not apply.
type StringPK struct {
	ID   string `db:"id,pk"`
	Name string `db:"name"`
}

// TimePK exercises time.Time as a primary key.
type TimePK struct {
	At   time.Time `db:"at,pk"`
	Name string    `db:"name"`
}

// TypedColumns exercises column-type resolution: the registry, the type= tag,
// and a raw SQL type passed through verbatim.
type TypedColumns struct {
	ID       types.UUID `db:"id,pk"`
	OwnerID  string     `db:"owner_id,type=uuid"`
	Price    string     `db:"price,type=numeric(10,2)"`
	Nickname string     `db:"nickname"`
	Created  time.Time  `db:"created_at"`
	Expires  time.Time  `db:"expires_at,type=date"`
}

// Auditable is the columns every table repeats, to be embedded rather than
// copied.
type Auditable struct {
	CreatedAt time.Time  `json:"createdAt"`
	CreatedBy string     `json:"createdBy"`
	UpdatedAt *time.Time `json:"updatedAt"`
	UpdatedBy string     `json:"updatedBy"`
	DeletedAt *time.Time `json:"deletedAt" db:"deleted_at,archive"`
}

// Identifiable is an embedded primary key, to check that pk is found through
// an embed.
type Identifiable struct {
	ID types.UUID `db:"id,pk"`
}

// Embedded exercises flattening: two embedded structs whose fields become
// columns of this table.
type Embedded struct {
	Identifiable
	Name string `db:"name,notnull"`
	Auditable
}

// ShadowedEmbed exercises Go's shadowing rule: the outer CreatedBy hides the
// embedded one.
type ShadowedEmbed struct {
	ID        string `db:"id,pk"`
	CreatedBy string `db:"created_by,notnull"`
	Auditable
}

// Tables is the full DDL corpus, in a fixed order so goldens stay stable.
func Tables() []struct {
	Name  string
	Value any
} {
	return []struct {
		Name  string
		Value any
	}{
		{"scalars", Scalars{}},
		{"constraints", Constraints{}},
		{"composite_unique", CompositeUnique{}},
		{"indexed", Indexed{}},
		{"soft_deletable", SoftDeletable{}},
		{"json_doc", JSONDoc{}},
		{"string_pk", StringPK{}},
		{"time_pk", TimePK{}},
		{"typed_columns", TypedColumns{}},
		{"embedded", Embedded{}},
		{"shadowed_embed", ShadowedEmbed{}},
	}
}

// InsertDoc is the document used for insert, update and bulk-insert rendering.
type InsertDoc struct {
	ID    int64  `db:"id,pk autoincr"`
	Name  string `db:"name"`
	Email string `db:"email"`
	Score int    `db:"score"`
}

// InsertDocs returns a fixed set of documents for bulk-insert rendering.
func InsertDocs() []any {
	return []any{
		InsertDoc{Name: "alice", Email: "alice@test.com", Score: 1},
		InsertDoc{Name: "bob", Email: "bob@test.com"},
		InsertDoc{Name: "carol", Score: 3},
	}
}

// Section accumulates named blocks of rendered output into a golden document.
type Section struct {
	sb strings.Builder
}

// Add appends a named block. Multi-line values are indented so blocks stay
// visually distinct in the golden file.
func (s *Section) Add(name, value string) {
	fmt.Fprintf(&s.sb, "## %s\n%s\n\n", name, value)
}

// Addf appends a named block from a format string.
func (s *Section) Addf(name, format string, args ...any) {
	s.Add(name, fmt.Sprintf(format, args...))
}

// String returns the accumulated document.
func (s *Section) String() string { return s.sb.String() }

// Compare checks got against the golden file at testdata/<name>.golden.
// When update is true the golden file is rewritten instead of compared, which
// is how goldens are regenerated (go test ./... -update).
//
// It returns a nil error when the content matches.
func Compare(name, got string, update bool) error {
	path := filepath.Join("testdata", name+".golden")

	if update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			return fmt.Errorf("creating testdata dir: %w", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			return fmt.Errorf("writing golden %s: %w", path, err)
		}
		return nil
	}

	want, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading golden %s (run with -update to create it): %w", path, err)
	}

	if string(want) != got {
		return fmt.Errorf("golden %s mismatch:\n%s", path, diff(string(want), got))
	}
	return nil
}

// diff renders the first differing line with a little context, which is enough
// to locate a regression without pulling in a diffing dependency.
func diff(want, got string) string {
	wantLines, gotLines := strings.Split(want, "\n"), strings.Split(got, "\n")

	for i := 0; i < len(wantLines) || i < len(gotLines); i++ {
		w, g := lineAt(wantLines, i), lineAt(gotLines, i)
		if w == g {
			continue
		}
		return fmt.Sprintf("  line %d:\n    want: %s\n    got:  %s", i+1, w, g)
	}
	return "  (files differ only in trailing content)"
}

func lineAt(lines []string, i int) string {
	if i < len(lines) {
		return lines[i]
	}
	return "<missing>"
}
