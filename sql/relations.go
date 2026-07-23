package sql

import (
	"context"
	"reflect"
	"strings"
	"sync"

	"github.com/iancoleman/strcase"
)

// RelationKind identifies the kind of association between two entities.
type RelationKind string

const (
	// RelationBelongsTo marks a field whose foreign key lives on this struct's table.
	RelationBelongsTo RelationKind = "belongs_to"
	// RelationHasMany marks a slice field whose foreign key lives on the child table.
	RelationHasMany RelationKind = "has_many"
	// RelationManyToMany marks a slice field linked through a join table.
	RelationManyToMany RelationKind = "many_to_many"
)

// RelationInfo describes an association declared with db-tag options such as
// `db:"-,has_many fk:user_id"`. Options: has_many | belongs_to | many_to_many,
// plus fk:<col>, ref:<col> (many_to_many), join:<table> (many_to_many).
type RelationInfo struct {
	Field      string       // Go field name holding the related entity(ies)
	FieldIndex int          // index of that field on the parent struct
	Kind       RelationKind // association kind
	FK         string       // foreign key column
	Ref        string       // reference column on the join table (many_to_many)
	JoinTable  string       // join table name (many_to_many)
	Target     reflect.Type // related entity struct type
	Many       bool         // true when the field is a slice
}

var relationCache sync.Map // map[reflect.Type][]RelationInfo

// GetRelations returns the associations declared on doc's struct type, cached.
func GetRelations(doc any) []RelationInfo {
	t := reflect.TypeOf(doc)
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice {
		t = t.Elem()
	}
	if cached, ok := relationCache.Load(t); ok {
		return cached.([]RelationInfo)
	}

	var rels []RelationInfo
	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			ri, ok := parseRelation(f)
			if !ok {
				continue
			}
			ri.Field = f.Name
			ri.FieldIndex = i
			ri.Target, ri.Many = relationTarget(f.Type)
			rels = append(rels, ri)
		}
	}

	relationCache.Store(t, rels)
	return rels
}

// IsRelationField reports whether a struct field declares an association and so
// must be excluded from DDL, INSERT, and UPDATE column handling.
func IsRelationField(f reflect.StructField) bool {
	_, ok := parseRelation(f)
	return ok
}

// parseRelation extracts a RelationInfo from a field's db tag options.
func parseRelation(f reflect.StructField) (RelationInfo, bool) {
	dbTag := f.Tag.Get("db")
	if dbTag == "" {
		return RelationInfo{}, false
	}
	parts := strings.SplitN(dbTag, ",", 2)
	if len(parts) < 2 {
		return RelationInfo{}, false
	}

	var ri RelationInfo
	found := false
	for _, opt := range strings.Fields(parts[1]) {
		switch {
		case opt == string(RelationBelongsTo):
			ri.Kind, found = RelationBelongsTo, true
		case opt == string(RelationHasMany):
			ri.Kind, found = RelationHasMany, true
		case opt == string(RelationManyToMany):
			ri.Kind, found = RelationManyToMany, true
		case strings.HasPrefix(opt, "fk:"):
			ri.FK = opt[len("fk:"):]
		case strings.HasPrefix(opt, "ref:"):
			ri.Ref = opt[len("ref:"):]
		case strings.HasPrefix(opt, "join:"):
			ri.JoinTable = opt[len("join:"):]
		}
	}
	return ri, found
}

// relationTarget resolves the related entity struct type behind pointers and
// slices, reporting whether the field is a slice (to-many).
func relationTarget(ft reflect.Type) (reflect.Type, bool) {
	many := false
	for ft.Kind() == reflect.Ptr {
		ft = ft.Elem()
	}
	if ft.Kind() == reflect.Slice {
		many = true
		ft = ft.Elem()
		for ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
	}
	return ft, many
}

// PreloadRelations loads the named associations for already-fetched docs using
// base to run batched child queries, then assigns the results onto each parent's
// relation field. docs must be a pointer to a struct or a pointer to a slice.
func PreloadRelations(ctx context.Context, base Engine, docs any, names []string) error {
	if len(names) == 0 {
		return nil
	}

	rv := reflect.ValueOf(docs)
	if rv.Kind() != reflect.Ptr {
		return nil
	}
	rv = rv.Elem()

	var parents []reflect.Value
	switch rv.Kind() {
	case reflect.Slice:
		for i := 0; i < rv.Len(); i++ {
			parents = append(parents, rv.Index(i))
		}
	case reflect.Struct:
		parents = append(parents, rv)
	default:
		return nil
	}
	if len(parents) == 0 {
		return nil
	}

	relations := GetRelations(docs)
	for _, name := range names {
		ri, ok := findRelation(relations, name)
		if !ok {
			continue
		}
		if err := loadRelation(ctx, base, parents, ri); err != nil {
			return err
		}
	}
	return nil
}

// findRelation matches a preload name against a relation's Go field name,
// case-insensitively and ignoring snake/camel differences.
func findRelation(relations []RelationInfo, name string) (RelationInfo, bool) {
	want := strcase.ToSnake(name)
	for _, ri := range relations {
		if strings.EqualFold(ri.Field, name) || strcase.ToSnake(ri.Field) == want {
			return ri, true
		}
	}
	return RelationInfo{}, false
}

func loadRelation(ctx context.Context, base Engine, parents []reflect.Value, ri RelationInfo) error {
	switch ri.Kind {
	case RelationBelongsTo:
		return loadBelongsTo(ctx, base, parents, ri)
	case RelationHasMany:
		return loadHasMany(ctx, base, parents, ri)
	case RelationManyToMany:
		return loadManyToMany(ctx, base, parents, ri)
	}
	return nil
}

// queryByIn fetches all rows of target whose keyCol is in keys, returning the
// resulting slice value. An empty key set yields an empty slice without a query.
func queryByIn(ctx context.Context, base Engine, target reflect.Type, keyCol string, keys []any) (reflect.Value, error) {
	slicePtr := reflect.New(reflect.SliceOf(target))
	if len(keys) == 0 {
		return slicePtr.Elem(), nil
	}
	table := GetTableName(reflect.New(target).Interface())
	err := base.Table(table).In(keyCol, keys...).FindMany(ctx, slicePtr.Interface())
	return slicePtr.Elem(), err
}

// uniqueKeys collects distinct field values from parents at the given field index.
func uniqueKeys(parents []reflect.Value, fieldIdx int) []any {
	seen := map[any]bool{}
	var keys []any
	for _, p := range parents {
		k := p.Field(fieldIdx).Interface()
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	return keys
}

// assignOne assigns a single related struct value onto a field that is either a
// struct or a pointer to a struct.
func assignOne(field, child reflect.Value) {
	if !field.CanSet() {
		return
	}
	if field.Kind() == reflect.Ptr {
		p := reflect.New(child.Type())
		p.Elem().Set(child)
		field.Set(p)
		return
	}
	field.Set(child)
}

func loadBelongsTo(ctx context.Context, base Engine, parents []reflect.Value, ri RelationInfo) error {
	childType := ri.Target
	childPK := GetPKColumn(reflect.New(childType).Interface())
	fkIdx, ok := GetDBFieldMap(parents[0].Interface())[ri.FK]
	if !ok {
		return nil
	}

	children, err := queryByIn(ctx, base, childType, childPK, uniqueKeys(parents, fkIdx))
	if err != nil {
		return err
	}

	childPKIdx := GetDBFieldMap(reflect.New(childType).Interface())[childPK]
	byKey := map[any]reflect.Value{}
	for i := 0; i < children.Len(); i++ {
		c := children.Index(i)
		byKey[c.Field(childPKIdx).Interface()] = c
	}

	for _, p := range parents {
		if c, ok := byKey[p.Field(fkIdx).Interface()]; ok {
			assignOne(p.Field(ri.FieldIndex), c)
		}
	}
	return nil
}

func loadHasMany(ctx context.Context, base Engine, parents []reflect.Value, ri RelationInfo) error {
	parentPK := GetPKColumn(parents[0].Interface())
	parentPKIdx := GetDBFieldMap(parents[0].Interface())[parentPK]

	children, err := queryByIn(ctx, base, ri.Target, ri.FK, uniqueKeys(parents, parentPKIdx))
	if err != nil {
		return err
	}

	childFKIdx, ok := GetDBFieldMap(reflect.New(ri.Target).Interface())[ri.FK]
	if !ok {
		return nil
	}
	grouped := groupByKey(children, childFKIdx, ri.Target)

	for _, p := range parents {
		field := p.Field(ri.FieldIndex)
		if s, ok := grouped[p.Field(parentPKIdx).Interface()]; ok {
			field.Set(s)
		} else if field.IsNil() {
			field.Set(reflect.MakeSlice(field.Type(), 0, 0))
		}
	}
	return nil
}

// groupByKey buckets child rows into slices keyed by the value at keyIdx.
func groupByKey(children reflect.Value, keyIdx int, target reflect.Type) map[any]reflect.Value {
	sliceType := reflect.SliceOf(target)
	grouped := map[any]reflect.Value{}
	for i := 0; i < children.Len(); i++ {
		c := children.Index(i)
		k := c.Field(keyIdx).Interface()
		s, ok := grouped[k]
		if !ok {
			s = reflect.MakeSlice(sliceType, 0, 0)
		}
		grouped[k] = reflect.Append(s, c)
	}
	return grouped
}

func loadManyToMany(ctx context.Context, base Engine, parents []reflect.Value, ri RelationInfo) error {
	parentPK := GetPKColumn(parents[0].Interface())
	parentPKIdx := GetDBFieldMap(parents[0].Interface())[parentPK]
	childPK := GetPKColumn(reflect.New(ri.Target).Interface())

	// Fetch join-table rows (parentKey, childKey) via a dynamically-typed struct
	// so the existing query builder handles placeholders and scanning.
	joinType := reflect.StructOf([]reflect.StructField{
		{Name: "FK", Type: parents[0].Field(parentPKIdx).Type(), Tag: dbTag(ri.FK)},
		{Name: "Ref", Type: reflect.New(ri.Target).Elem().Field(pkIndex(ri.Target, childPK)).Type(), Tag: dbTag(ri.Ref)},
	})
	joinSlicePtr := reflect.New(reflect.SliceOf(joinType))
	if keys := uniqueKeys(parents, parentPKIdx); len(keys) > 0 {
		if err := base.Table(ri.JoinTable).In(ri.FK, keys...).FindMany(ctx, joinSlicePtr.Interface()); err != nil {
			return err
		}
	}
	joinRows := joinSlicePtr.Elem()

	parentToRefs := map[any][]any{}
	var refKeys []any
	refSeen := map[any]bool{}
	for i := 0; i < joinRows.Len(); i++ {
		row := joinRows.Index(i)
		pk := row.Field(0).Interface()
		ref := row.Field(1).Interface()
		parentToRefs[pk] = append(parentToRefs[pk], ref)
		if !refSeen[ref] {
			refSeen[ref] = true
			refKeys = append(refKeys, ref)
		}
	}

	children, err := queryByIn(ctx, base, ri.Target, childPK, refKeys)
	if err != nil {
		return err
	}
	childPKIdx := GetDBFieldMap(reflect.New(ri.Target).Interface())[childPK]
	byPK := map[any]reflect.Value{}
	for i := 0; i < children.Len(); i++ {
		c := children.Index(i)
		byPK[c.Field(childPKIdx).Interface()] = c
	}

	assignManyToMany(parents, parentPKIdx, ri, parentToRefs, byPK)
	return nil
}

// assignManyToMany builds each parent's related slice from the resolved children.
func assignManyToMany(parents []reflect.Value, parentPKIdx int, ri RelationInfo, parentToRefs map[any][]any, byPK map[any]reflect.Value) {
	sliceType := reflect.SliceOf(ri.Target)
	for _, p := range parents {
		field := p.Field(ri.FieldIndex)
		s := reflect.MakeSlice(sliceType, 0, 0)
		for _, ref := range parentToRefs[p.Field(parentPKIdx).Interface()] {
			if c, ok := byPK[ref]; ok {
				s = reflect.Append(s, c)
			}
		}
		field.Set(s)
	}
}

func dbTag(col string) reflect.StructTag {
	return reflect.StructTag(`db:"` + col + `"`)
}

func pkIndex(t reflect.Type, pk string) int {
	if idx, ok := GetDBFieldMap(reflect.New(t).Interface())[pk]; ok {
		return idx
	}
	return 0
}
