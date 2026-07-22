package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hookDoc struct {
	Name       string
	beforeRan  bool
	afterFinds int
}

func (d *hookDoc) BeforeCreate(ctx context.Context) error {
	d.beforeRan = true
	return nil
}

func (d *hookDoc) AfterFind(ctx context.Context) error {
	d.afterFinds++
	return nil
}

type failHookDoc struct{}

func (failHookDoc) BeforeCreate(ctx context.Context) error {
	return errors.New("boom")
}

func TestRunBeforeCreate_invokesHook(t *testing.T) {
	d := &hookDoc{}
	require.NoError(t, RunBeforeCreate(context.Background(), d))
	assert.True(t, d.beforeRan)
}

func TestRunBeforeCreate_noHookIsNoop(t *testing.T) {
	assert.NoError(t, RunBeforeCreate(context.Background(), &struct{ X int }{}))
}

func TestRunBeforeCreate_propagatesError(t *testing.T) {
	assert.Error(t, RunBeforeCreate(context.Background(), failHookDoc{}))
}

func TestRunAfterFindResults_perElement(t *testing.T) {
	docs := []hookDoc{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	require.NoError(t, RunAfterFindResults(context.Background(), &docs))
	for i := range docs {
		assert.Equal(t, 1, docs[i].afterFinds, "AfterFind should run once per element")
	}
}

func TestRunAfterFindResults_singleStruct(t *testing.T) {
	d := &hookDoc{}
	require.NoError(t, RunAfterFindResults(context.Background(), d))
	assert.Equal(t, 1, d.afterFinds)
}
