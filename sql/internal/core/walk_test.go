package core

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type auditable struct {
	CreatedAt time.Time  `json:"createdAt"`
	CreatedBy string     `json:"createdBy"`
	UpdatedAt *time.Time `json:"updatedAt"`
	DeletedAt *time.Time `db:"deleted_at,archive"`
}

type identifiable struct {
	ID string `db:"id,pk"`
}

type patient struct {
	Name string `db:"name"`
	identifiable
	auditable
}

// ScannerStruct is a struct that maps to one column, so it must stay a leaf.
// Leaf embeds are exported types in practice (time.Time, sql.NullString,
// uuid.UUID): an unexported one could not be assigned as a whole anyway.
type ScannerStruct struct{ raw string }

func (s *ScannerStruct) Scan(any) error { return nil }

func (s ScannerStruct) Value() (driver.Value, error) { return s.raw, nil }

// TaggedEmbed carries a db tag where it is embedded, which declares the embed
// itself to be a single column rather than a group of them.
type TaggedEmbed struct {
	CreatedBy string
	CreatedAt time.Time
}

// Tableish declares TableName, marking it a table in its own right.
type Tableish struct {
	Other string `db:"other"`
}

func (Tableish) TableName() string { return "tableish" }

// columnNames renders a walk as its column names, which is what the SQL layers
// actually consume.
func columnNames(t *testing.T, v any) []string {
	t.Helper()
	var out []string
	for _, f := range WalkFields(reflect.TypeOf(v)) {
		out = append(out, GetFieldName(f.StructField))
	}
	return out
}

func TestWalkFields_flattensEmbedded(t *testing.T) {
	assert.Equal(t,
		[]string{"name", "id", "created_at", "created_by", "updated_at", "deleted_at"},
		columnNames(t, patient{}))
}

func TestWalkFields_flatStructIsUnchanged(t *testing.T) {
	type flat struct {
		ID    string `db:"id,pk"`
		Name  string `db:"name"`
		Email string
	}
	assert.Equal(t, []string{"id", "name", "email"}, columnNames(t, flat{}))
}

func TestWalkFields_indexPaths(t *testing.T) {
	byName := map[string][]int{}
	for _, f := range WalkFields(reflect.TypeOf(patient{})) {
		byName[GetFieldName(f.StructField)] = f.Index
	}

	assert.Equal(t, []int{0}, byName["name"], "outer field is a single hop")
	assert.Equal(t, []int{1, 0}, byName["id"], "embedded field carries its parent index")
	assert.Equal(t, []int{2, 1}, byName["created_by"])
}

// TestWalkFields_outerShadowsEmbedded follows Go's own shadowing rule.
func TestWalkFields_outerShadowsEmbedded(t *testing.T) {
	type shadowing struct {
		CreatedBy string `db:"created_by"`
		auditable
	}

	fields := WalkFields(reflect.TypeOf(shadowing{}))

	var found int
	for _, f := range fields {
		if GetFieldName(f.StructField) == "created_by" {
			found++
			assert.Equal(t, []int{0}, f.Index, "the outer field must win")
		}
	}
	assert.Equal(t, 1, found, "created_by must appear exactly once")
}

func TestWalkFields_valueResolvesThroughEmbed(t *testing.T) {
	p := patient{Name: "ada"}
	p.ID = "abc"
	p.CreatedBy = "root"

	val := reflect.ValueOf(&p).Elem()
	got := map[string]any{}
	for _, f := range WalkFields(reflect.TypeOf(p)) {
		got[GetFieldName(f.StructField)] = f.Value(val).Interface()
	}

	assert.Equal(t, "ada", got["name"])
	assert.Equal(t, "abc", got["id"])
	assert.Equal(t, "root", got["created_by"])
}

// TestWalkFields_leafStructsAreNotDescended guards the types that are one
// column despite being structs. Descending into them would explode a timestamp
// into its unexported wall/ext/loc fields.
func TestWalkFields_leafStructsAreNotDescended(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  []string
	}{
		{
			name:  "time.Time is registered, so it is a column",
			value: struct{ time.Time }{},
			want:  []string{"time"},
		},
		{
			name:  "a Scanner/Valuer struct is a column",
			value: struct{ ScannerStruct }{},
			want:  []string{"scanner_struct"},
		},
		{
			name: "an embedded struct with a db tag is a column",
			value: struct {
				TaggedEmbed `db:"audit,json"`
			}{},
			want: []string{"audit"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, columnNames(t, tc.value))
		})
	}
}

// TestWalkFields_embeddedTableIsNotFlattened checks that a type declaring
// TableName is treated as its own table, not as a group of columns.
func TestWalkFields_embeddedTableIsNotFlattened(t *testing.T) {
	cols := columnNames(t, struct {
		Name string `db:"name"`
		Tableish
	}{})
	assert.Equal(t, []string{"name", "tableish"}, cols)
}

// TestWalkFields_embeddedPointerIsSkipped documents the v1 limitation.
func TestWalkFields_embeddedPointerIsSkipped(t *testing.T) {
	type withPtrEmbed struct {
		Name string `db:"name"`
		*auditable
	}
	assert.Equal(t, []string{"name"}, columnNames(t, withPtrEmbed{}))
}

func TestWalkFields_ignoredAndUnexported(t *testing.T) {
	type mixed struct {
		Name    string `db:"name"`
		Skipped string `db:"-"`
		hidden  string //nolint:unused // exercises the unexported skip
	}
	assert.Equal(t, []string{"name"}, columnNames(t, mixed{}))
}

func TestWalkFields_pointerAndSliceTypes(t *testing.T) {
	want := []string{"name", "id", "created_at", "created_by", "updated_at", "deleted_at"}
	assert.Equal(t, want, columnNames(t, &patient{}))
	assert.Equal(t, want, columnNames(t, []patient{}))
}

func TestWalkFields_nonStructReturnsNil(t *testing.T) {
	require.Nil(t, WalkFields(reflect.TypeOf("")))
	require.Nil(t, WalkFields(reflect.TypeOf(42)))
}

func TestWalkFields_cachedResultIsStable(t *testing.T) {
	first := WalkFields(reflect.TypeOf(patient{}))
	second := WalkFields(reflect.TypeOf(patient{}))
	assert.Equal(t, first, second)
}
