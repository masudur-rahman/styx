package core

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fieldWithTag builds a StructField carrying the given db tag.
func fieldWithTag(name, dbTag string) reflect.StructField {
	return reflect.StructField{
		Name: name,
		Type: reflect.TypeOf(""),
		Tag:  reflect.StructTag(`db:"` + dbTag + `"`),
	}
}

func TestParseDBTag_valid(t *testing.T) {
	tests := []struct {
		name       string
		tag        string
		wantName   string
		wantTokens []string
		wantIgnore bool
	}{
		{name: "name only", tag: "email", wantName: "email"},
		{name: "name and one token", tag: "email,uq", wantName: "email", wantTokens: []string{"UQ"}},
		{
			name: "several space separated tokens", tag: "id,pk uq notnull",
			wantName: "id", wantTokens: []string{"PK", "UQ", "NOTNULL"},
		},
		{name: "tokens are case insensitive", tag: "id,PK Uq", wantName: "id", wantTokens: []string{"PK", "UQ"}},
		{name: "empty column name", tag: ",pk", wantTokens: []string{"PK"}},
		{name: "named index", tag: "tenant,idx:by_tenant", wantName: "tenant", wantTokens: []string{"IDX:BY_TENANT"}},
		{name: "named unique index", tag: "region,uidx:by_region", wantName: "region", wantTokens: []string{"UIDX:BY_REGION"}},
		{name: "ignored field", tag: "-", wantIgnore: true},
		{name: "extra whitespace is collapsed", tag: "id,  pk   uq  ", wantName: "id", wantTokens: []string{"PK", "UQ"}},
		{
			name: "assignment value may contain a comma", tag: "price,type=numeric(10,2)",
			wantName: "price", wantTokens: []string{"TYPE=NUMERIC(10,2)"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tag, err := ParseDBTag(fieldWithTag("F", tc.tag))
			require.NoError(t, err)
			assert.Equal(t, tc.wantName, tag.Name)
			assert.Equal(t, tc.wantTokens, tag.Tokens)
			assert.Equal(t, tc.wantIgnore, tag.Ignore)
		})
	}
}

func TestParseDBTag_noTag(t *testing.T) {
	tag, err := ParseDBTag(reflect.StructField{Name: "F", Type: reflect.TypeOf("")})
	require.NoError(t, err)
	assert.Equal(t, DBTag{}, tag)
}

// TestParseDBTag_invalid covers the forms that used to be dropped in silence.
func TestParseDBTag_invalid(t *testing.T) {
	tests := []struct {
		name    string
		tag     string
		wantErr string
	}{
		{
			name: "comma separated attributes", tag: "id,pk,uq",
			wantErr: "separated by spaces",
		},
		{
			name: "comma before an index token", tag: "id,pk,idx",
			wantErr: `unknown option "PK,IDX"`,
		},
		{name: "unknown token", tag: "id,pkk", wantErr: `unknown option "PKK"`},
		{name: "unknown prefixed token", tag: "id,zdx:name", wantErr: `unknown option "ZDX:NAME"`},
		{name: "named index without a name", tag: "id,idx:", wantErr: "needs a name after the colon"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDBTag(fieldWithTag("F", tc.tag))
			require.Error(t, err)
			assert.ErrorContains(t, err, tc.wantErr)
			assert.ErrorContains(t, err, "field F")
		})
	}
}

// TestParseDBTag_reportsEveryBadToken checks that one call surfaces all the
// problems in a tag rather than stopping at the first.
func TestParseDBTag_reportsEveryBadToken(t *testing.T) {
	_, err := ParseDBTag(fieldWithTag("F", "id,pk bogus alsobogus"))
	require.Error(t, err)
	assert.ErrorContains(t, err, `"BOGUS"`)
	assert.ErrorContains(t, err, `"ALSOBOGUS"`)
}

func TestDBTag_HasAndArg(t *testing.T) {
	tag, err := ParseDBTag(fieldWithTag("F", "tenant,pk idx:by_tenant"))
	require.NoError(t, err)

	assert.True(t, tag.Has(TokenPK))
	assert.False(t, tag.Has(TokenUnique))

	arg, ok := tag.Arg(TokenIndex)
	assert.True(t, ok)
	assert.Equal(t, "BY_TENANT", arg)

	_, ok = tag.Arg(TokenUIndex)
	assert.False(t, ok)
}

// TestParseDBTag_relationTokens guards the association tags. They are declared
// as RelationKind values ("m2o", not "belongsto"), and restating them here once
// put the wrong spellings in the known set.
func TestParseDBTag_relationTokens(t *testing.T) {
	tests := []struct {
		name string
		tag  string
		want []string
	}{
		{name: "to-one", tag: "author,m2o fk:author_id", want: []string{"M2O", "FK:AUTHOR_ID"}},
		{name: "to-many", tag: "books,o2m fk:author_id", want: []string{"O2M", "FK:AUTHOR_ID"}},
		{
			name: "through a join table", tag: "tags,m2m join:book_tags fk:book_id ref:tag_id",
			want: []string{"M2M", "JOIN:BOOK_TAGS", "FK:BOOK_ID", "REF:TAG_ID"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tag, err := ParseDBTag(fieldWithTag("F", tc.tag))
			require.NoError(t, err)
			assert.Equal(t, tc.want, tag.Tokens)
		})
	}
}
