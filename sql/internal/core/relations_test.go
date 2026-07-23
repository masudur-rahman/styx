package core

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type relTag struct{ Name string }

type relParent struct {
	ID       int64    `db:"id,pk"`
	OwnerID  int64    `db:"owner_id"`
	Owner    *relTag  `db:"-,m2o fk:owner_id"`
	Children []relTag `db:"-,o2m fk:parent_id"`
	Labels   []relTag `db:"-,m2m join:parent_labels fk:parent_id ref:label_id"`
	Ignored  string   `db:"ignored"`
	Nested   relTag   `db:"nested,json"`
}

func TestGetRelations_parsesAllKinds(t *testing.T) {
	rels := GetRelations(relParent{})
	require.Len(t, rels, 3)

	byField := map[string]RelationInfo{}
	for _, r := range rels {
		byField[r.Field] = r
	}

	owner := byField["Owner"]
	assert.Equal(t, RelationBelongsTo, owner.Kind)
	assert.Equal(t, "owner_id", owner.FK)
	assert.False(t, owner.Many)
	assert.Equal(t, reflect.TypeOf(relTag{}), owner.Target)

	children := byField["Children"]
	assert.Equal(t, RelationHasMany, children.Kind)
	assert.Equal(t, "parent_id", children.FK)
	assert.True(t, children.Many)

	labels := byField["Labels"]
	assert.Equal(t, RelationManyToMany, labels.Kind)
	assert.Equal(t, "parent_labels", labels.JoinTable)
	assert.Equal(t, "parent_id", labels.FK)
	assert.Equal(t, "label_id", labels.Ref)
}

func TestIsRelationField(t *testing.T) {
	t.Helper()
	typ := reflect.TypeOf(relParent{})

	rel, _ := typ.FieldByName("Owner")
	assert.True(t, IsRelationField(rel))

	plain, _ := typ.FieldByName("Ignored")
	assert.False(t, IsRelationField(plain))

	// A json-tagged struct field is data, not a relation.
	jsonField, _ := typ.FieldByName("Nested")
	assert.False(t, IsRelationField(jsonField))
}

func TestFindRelation_matchesByFieldOrSnake(t *testing.T) {
	rels := GetRelations(relParent{})
	_, ok := findRelation(rels, "Children")
	assert.True(t, ok)
	_, ok = findRelation(rels, "children")
	assert.True(t, ok)
	_, ok = findRelation(rels, "nope")
	assert.False(t, ok)
}
