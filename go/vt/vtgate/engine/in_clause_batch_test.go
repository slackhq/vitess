/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func makeTestTuple(n int) *querypb.BindVariable {
	values := make([]*querypb.Value, n)
	for i := range values {
		values[i] = &querypb.Value{Type: querypb.Type_INT64, Value: []byte{byte('0' + i%10)}}
	}
	return &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: values}
}

func TestFindLargestTupleBV(t *testing.T) {
	t.Run("no tuples", func(t *testing.T) {
		bvs := map[string]*querypb.BindVariable{
			"v1": {Type: querypb.Type_INT64, Value: []byte("1")},
		}
		name, bv := findLargestTupleBV(bvs, 3)
		assert.Empty(t, name)
		assert.Nil(t, bv)
	})

	t.Run("tuple under threshold", func(t *testing.T) {
		bvs := map[string]*querypb.BindVariable{
			"v1": makeTestTuple(3),
		}
		name, bv := findLargestTupleBV(bvs, 3)
		assert.Empty(t, name)
		assert.Nil(t, bv)
	})

	t.Run("tuple over threshold", func(t *testing.T) {
		bvs := map[string]*querypb.BindVariable{
			"v1": makeTestTuple(5),
		}
		name, bv := findLargestTupleBV(bvs, 3)
		assert.Equal(t, "v1", name)
		require.NotNil(t, bv)
		assert.Len(t, bv.Values, 5)
	})

	t.Run("picks the largest tuple", func(t *testing.T) {
		bvs := map[string]*querypb.BindVariable{
			"v1": makeTestTuple(5),
			"v2": makeTestTuple(10),
			"v3": makeTestTuple(7),
		}
		name, bv := findLargestTupleBV(bvs, 3)
		assert.Equal(t, "v2", name)
		require.NotNil(t, bv)
		assert.Len(t, bv.Values, 10)
	})

	t.Run("skips __vals", func(t *testing.T) {
		bvs := map[string]*querypb.BindVariable{
			ListVarName: makeTestTuple(100),
		}
		name, bv := findLargestTupleBV(bvs, 3)
		assert.Empty(t, name)
		assert.Nil(t, bv)
	})

	t.Run("skips __vals0 and __vals1", func(t *testing.T) {
		bvs := map[string]*querypb.BindVariable{
			ListVarName + "0": makeTestTuple(100),
			ListVarName + "1": makeTestTuple(100),
		}
		name, bv := findLargestTupleBV(bvs, 3)
		assert.Empty(t, name)
		assert.Nil(t, bv)
	})

	t.Run("skips __vals but finds other tuple", func(t *testing.T) {
		bvs := map[string]*querypb.BindVariable{
			ListVarName: makeTestTuple(100),
			"v1":        makeTestTuple(5),
		}
		name, bv := findLargestTupleBV(bvs, 3)
		assert.Equal(t, "v1", name)
		require.NotNil(t, bv)
		assert.Len(t, bv.Values, 5)
	})
}

func TestChunkTupleValues(t *testing.T) {
	t.Run("exact division", func(t *testing.T) {
		values := makeTestTuple(6).Values
		chunks := chunkTupleValues(values, 3)
		require.Len(t, chunks, 2)
		assert.Len(t, chunks[0], 3)
		assert.Len(t, chunks[1], 3)
	})

	t.Run("remainder", func(t *testing.T) {
		values := makeTestTuple(7).Values
		chunks := chunkTupleValues(values, 3)
		require.Len(t, chunks, 3)
		assert.Len(t, chunks[0], 3)
		assert.Len(t, chunks[1], 3)
		assert.Len(t, chunks[2], 1)
	})

	t.Run("single chunk when under batch size", func(t *testing.T) {
		values := makeTestTuple(2).Values
		chunks := chunkTupleValues(values, 5)
		require.Len(t, chunks, 1)
		assert.Len(t, chunks[0], 2)
	})

	t.Run("empty values", func(t *testing.T) {
		chunks := chunkTupleValues(nil, 3)
		require.Len(t, chunks, 1)
		assert.Nil(t, chunks[0])
	})

	t.Run("shares underlying pointers", func(t *testing.T) {
		values := makeTestTuple(4).Values
		chunks := chunkTupleValues(values, 2)
		require.Len(t, chunks, 2)
		// Verify the chunks reference the same underlying values
		assert.Same(t, values[0], chunks[0][0])
		assert.Same(t, values[2], chunks[1][0])
	})
}

func TestCloneBindVarsWithTuple(t *testing.T) {
	original := map[string]*querypb.BindVariable{
		"v1": {Type: querypb.Type_INT64, Value: []byte("1")},
		"v2": makeTestTuple(5),
	}
	chunk := makeTestTuple(2).Values

	cloned := cloneBindVarsWithTuple(original, "v2", chunk)

	t.Run("original is not modified", func(t *testing.T) {
		assert.Len(t, original["v2"].Values, 5)
	})

	t.Run("cloned has the replacement", func(t *testing.T) {
		assert.Len(t, cloned["v2"].Values, 2)
		assert.Equal(t, querypb.Type_TUPLE, cloned["v2"].Type)
	})

	t.Run("other keys are shallow copied", func(t *testing.T) {
		assert.Same(t, original["v1"], cloned["v1"])
	})

	t.Run("cloned has same number of keys", func(t *testing.T) {
		assert.Len(t, cloned, len(original))
	})
}
