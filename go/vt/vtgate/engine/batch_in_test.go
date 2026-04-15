/*
Copyright 2026 The Vitess Authors.

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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestBatchIN_Passthrough_WhenDisabled(t *testing.T) {
	fields := sqltypes.MakeTestFields("id|name", "int64|varchar")
	inputResult := sqltypes.MakeTestResult(fields, "1|a", "2|b")

	fp := &fakePrimitive{results: []*sqltypes.Result{inputResult}}
	batch := &BatchIN{Input: fp}

	// Batch size 0 = disabled; should pass through to Input directly.
	vc := &loggingVCursor{inClauseBatchSize: 0}
	result, err := batch.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
}

func TestBatchIN_Passthrough_WhenUnderThreshold(t *testing.T) {
	fields := sqltypes.MakeTestFields("id|name", "int64|varchar")
	inputResult := sqltypes.MakeTestResult(fields, "1|a", "2|b")

	fp := &fakePrimitive{results: []*sqltypes.Result{inputResult}}
	batch := &BatchIN{Input: fp}

	// 3 values in the tuple but batch size is 5 — no splitting needed.
	bvs := map[string]*querypb.BindVariable{
		"ids": makeTupleBindVar(1, 2, 3),
	}

	vc := &loggingVCursor{inClauseBatchSize: 5}
	result, err := batch.TryExecute(context.Background(), vc, bvs, true)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
}

func TestBatchIN_Passthrough_VindexTuples(t *testing.T) {
	fields := sqltypes.MakeTestFields("id|name", "int64|varchar")
	inputResult := sqltypes.MakeTestResult(fields, "1|a", "2|b")

	fp := &fakePrimitive{results: []*sqltypes.Result{inputResult}}
	batch := &BatchIN{Input: fp}

	// Even though __vals has 5 values and batch size is 2, vindex vars are skipped.
	bvs := map[string]*querypb.BindVariable{
		"__vals": makeTupleBindVar(1, 2, 3, 4, 5),
	}

	vc := &loggingVCursor{inClauseBatchSize: 2}
	result, err := batch.TryExecute(context.Background(), vc, bvs, true)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
}

func TestBatchIN_SplitsSingleTuple(t *testing.T) {
	fields := sqltypes.MakeTestFields("id|name", "int64|varchar")

	// fakePrimitive returns one result per TryExecute call.
	// With 5 values and batch size 2, we get 3 chunks: [1,2], [3,4], [5].
	fp := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(fields, "1|a", "2|b"),
			sqltypes.MakeTestResult(fields, "3|c", "4|d"),
			sqltypes.MakeTestResult(fields, "5|e"),
		},
	}
	batch := &BatchIN{Input: fp}

	bvs := map[string]*querypb.BindVariable{
		"ids": makeTupleBindVar(1, 2, 3, 4, 5),
	}

	vc := &loggingVCursor{inClauseBatchSize: 2}
	result, err := batch.TryExecute(context.Background(), vc, bvs, true)
	require.NoError(t, err)
	assert.Equal(t, 5, len(result.Rows))
}

func TestBatchIN_SplitWithOrderBy(t *testing.T) {
	fields := sqltypes.MakeTestFields("id|name", "int64|varchar")

	// Return results in chunk order (not globally sorted).
	fp := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(fields, "3|c", "5|e"),
			sqltypes.MakeTestResult(fields, "1|a", "2|b"),
			sqltypes.MakeTestResult(fields, "4|d"),
		},
	}

	orderBy := evalengine.Comparison{
		evalengine.OrderByParams{
			WeightStringCol: -1,
			Col:             0,
		},
	}

	batch := &BatchIN{
		Input:   fp,
		OrderBy: orderBy,
	}

	bvs := map[string]*querypb.BindVariable{
		"ids": makeTupleBindVar(3, 5, 1, 2, 4),
	}

	vc := &loggingVCursor{inClauseBatchSize: 2}
	result, err := batch.TryExecute(context.Background(), vc, bvs, true)
	require.NoError(t, err)
	require.Equal(t, 5, len(result.Rows))

	// Verify sorted order by id (ascending).
	for i, row := range result.Rows {
		expected := fmt.Sprintf("%d", i+1)
		assert.Equal(t, expected, row[0].ToString(), "row %d should have id %s", i, expected)
	}
}

func TestBatchIN_SplitWithLimit(t *testing.T) {
	fields := sqltypes.MakeTestFields("id|name", "int64|varchar")

	fp := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(fields, "1|a", "2|b"),
			sqltypes.MakeTestResult(fields, "3|c", "4|d"),
			sqltypes.MakeTestResult(fields, "5|e"),
		},
	}

	limit := 3
	batch := &BatchIN{
		Input: fp,
		Limit: &limit,
	}

	bvs := map[string]*querypb.BindVariable{
		"ids": makeTupleBindVar(1, 2, 3, 4, 5),
	}

	vc := &loggingVCursor{inClauseBatchSize: 2}
	result, err := batch.TryExecute(context.Background(), vc, bvs, true)
	require.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows))
}

func TestBatchIN_SplitWithOrderByAndLimit(t *testing.T) {
	fields := sqltypes.MakeTestFields("id|name", "int64|varchar")

	fp := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(fields, "5|e", "3|c"),
			sqltypes.MakeTestResult(fields, "1|a", "4|d"),
			sqltypes.MakeTestResult(fields, "2|b"),
		},
	}

	orderBy := evalengine.Comparison{
		evalengine.OrderByParams{
			WeightStringCol: -1,
			Col:             0,
		},
	}
	limit := 3

	batch := &BatchIN{
		Input:   fp,
		OrderBy: orderBy,
		Limit:   &limit,
	}

	bvs := map[string]*querypb.BindVariable{
		"ids": makeTupleBindVar(5, 3, 1, 4, 2),
	}

	vc := &loggingVCursor{inClauseBatchSize: 2}
	result, err := batch.TryExecute(context.Background(), vc, bvs, true)
	require.NoError(t, err)
	require.Equal(t, 3, len(result.Rows))

	// Should be first 3 in sorted order: 1, 2, 3.
	for i, row := range result.Rows {
		expected := fmt.Sprintf("%d", i+1)
		assert.Equal(t, expected, row[0].ToString(), "row %d should have id %s", i, expected)
	}
}

func TestBatchIN_StreamPassthrough_WhenDisabled(t *testing.T) {
	fields := sqltypes.MakeTestFields("id|name", "int64|varchar")
	inputResult := sqltypes.MakeTestResult(fields, "1|a", "2|b")

	fp := &fakePrimitive{results: []*sqltypes.Result{inputResult}}
	batch := &BatchIN{Input: fp}

	vc := &loggingVCursor{inClauseBatchSize: 0}
	result, err := wrapStreamExecute(batch, vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))
}

func TestBatchIN_StreamFallsBackToBatched(t *testing.T) {
	fields := sqltypes.MakeTestFields("id|name", "int64|varchar")

	fp := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(fields, "1|a", "2|b"),
			sqltypes.MakeTestResult(fields, "3|c"),
		},
	}
	batch := &BatchIN{Input: fp}

	bvs := map[string]*querypb.BindVariable{
		"ids": makeTupleBindVar(1, 2, 3),
	}

	vc := &loggingVCursor{inClauseBatchSize: 2}
	result, err := wrapStreamExecute(batch, vc, bvs, true)
	require.NoError(t, err)
	assert.Equal(t, 3, len(result.Rows))
}

func TestBatchIN_InputError(t *testing.T) {
	fp := &fakePrimitive{sendErr: fmt.Errorf("test error")}
	batch := &BatchIN{Input: fp}

	bvs := map[string]*querypb.BindVariable{
		"ids": makeTupleBindVar(1, 2, 3),
	}

	vc := &loggingVCursor{inClauseBatchSize: 2}
	_, err := batch.TryExecute(context.Background(), vc, bvs, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestBatchIN_GetFields(t *testing.T) {
	fields := sqltypes.MakeTestFields("id|name", "int64|varchar")
	fp := &fakePrimitive{results: []*sqltypes.Result{{Fields: fields}}}
	batch := &BatchIN{Input: fp}

	result, err := batch.GetFields(context.Background(), &noopVCursor{}, nil)
	require.NoError(t, err)
	assert.Equal(t, fields, result.Fields)
}

func TestBatchIN_NeedsTransaction(t *testing.T) {
	fp := &fakePrimitive{}
	batch := &BatchIN{Input: fp}
	assert.False(t, batch.NeedsTransaction())
}

func TestBatchIN_Inputs(t *testing.T) {
	fp := &fakePrimitive{}
	batch := &BatchIN{Input: fp}
	inputs, _ := batch.Inputs()
	require.Len(t, inputs, 1)
	assert.Equal(t, fp, inputs[0])
}

func TestBatchIN_Description(t *testing.T) {
	batch := &BatchIN{Input: &fakePrimitive{}}
	desc := batch.description()
	assert.Equal(t, "BatchIN", desc.OperatorType)
	assert.Empty(t, desc.Other)

	orderBy := evalengine.Comparison{
		evalengine.OrderByParams{
			WeightStringCol: -1,
			Col:             0,
		},
	}
	limit := 10
	batch = &BatchIN{
		Input:   &fakePrimitive{},
		OrderBy: orderBy,
		Limit:   &limit,
	}
	desc = batch.description()
	assert.Equal(t, "BatchIN", desc.OperatorType)
	assert.Contains(t, desc.Other, "OrderBy")
	assert.Equal(t, 10, desc.Other["Limit"])
}

// --- Helper functions for chunking/cartesian logic ---

func TestFindOversizedTuples(t *testing.T) {
	bvs := map[string]*querypb.BindVariable{
		"small": makeTupleBindVar(1, 2),
		"big":   makeTupleBindVar(1, 2, 3, 4, 5),
		// Vindex bind var — should be skipped even though it exceeds threshold.
		"__vals": makeTupleBindVar(1, 2, 3, 4, 5, 6, 7),
		// Non-tuple bind var — should be skipped.
		"scalar": sqltypes.Int64BindVariable(42),
	}

	result := findOversizedTuples(bvs, 3)
	require.Len(t, result, 1)
	assert.Equal(t, "big", result[0].name)
	assert.Len(t, result[0].chunks, 2) // ceil(5/3) = 2
}

func TestFindOversizedTuples_MultipleTuples(t *testing.T) {
	bvs := map[string]*querypb.BindVariable{
		"b_ids": makeTupleBindVar(1, 2, 3, 4, 5),
		"a_ids": makeTupleBindVar(10, 20, 30, 40),
	}

	result := findOversizedTuples(bvs, 2)
	require.Len(t, result, 2)
	// Sorted by name.
	assert.Equal(t, "a_ids", result[0].name)
	assert.Equal(t, "b_ids", result[1].name)
}

func TestChunkTupleValues(t *testing.T) {
	values := make([]*querypb.Value, 7)
	for i := range values {
		values[i] = &querypb.Value{Type: querypb.Type_INT64, Value: []byte(fmt.Sprintf("%d", i+1))}
	}

	chunks := chunkTupleValues(values, 3)
	require.Len(t, chunks, 3) // [1,2,3], [4,5,6], [7]
	assert.Len(t, chunks[0], 3)
	assert.Len(t, chunks[1], 3)
	assert.Len(t, chunks[2], 1)
}

func TestChunkTupleValues_ExactMultiple(t *testing.T) {
	values := make([]*querypb.Value, 6)
	for i := range values {
		values[i] = &querypb.Value{Type: querypb.Type_INT64, Value: []byte(fmt.Sprintf("%d", i+1))}
	}

	chunks := chunkTupleValues(values, 3)
	require.Len(t, chunks, 2)
	assert.Len(t, chunks[0], 3)
	assert.Len(t, chunks[1], 3)
}

func TestChunkTupleValues_ZeroBatchSize(t *testing.T) {
	values := make([]*querypb.Value, 3)
	for i := range values {
		values[i] = &querypb.Value{Type: querypb.Type_INT64, Value: []byte(fmt.Sprintf("%d", i+1))}
	}

	chunks := chunkTupleValues(values, 0)
	require.Len(t, chunks, 1)
	assert.Len(t, chunks[0], 3)
}

func TestCartesianBatchCombinations_Single(t *testing.T) {
	tuples := []oversizedTuple{
		{
			name: "ids",
			chunks: [][]*querypb.Value{
				{{Type: querypb.Type_INT64, Value: []byte("1")}},
				{{Type: querypb.Type_INT64, Value: []byte("2")}},
				{{Type: querypb.Type_INT64, Value: []byte("3")}},
			},
		},
	}

	combos := cartesianBatchCombinations(tuples)
	require.Len(t, combos, 3)
	for i, combo := range combos {
		require.Len(t, combo, 1)
		assert.Equal(t, "ids", combo[0].name)
		assert.Equal(t, fmt.Sprintf("%d", i+1), string(combo[0].chunk[0].Value))
	}
}

func TestCartesianBatchCombinations_Multiple(t *testing.T) {
	tuples := []oversizedTuple{
		{
			name: "a",
			chunks: [][]*querypb.Value{
				{{Type: querypb.Type_INT64, Value: []byte("1")}},
				{{Type: querypb.Type_INT64, Value: []byte("2")}},
			},
		},
		{
			name: "b",
			chunks: [][]*querypb.Value{
				{{Type: querypb.Type_INT64, Value: []byte("x")}},
				{{Type: querypb.Type_INT64, Value: []byte("y")}},
				{{Type: querypb.Type_INT64, Value: []byte("z")}},
			},
		},
	}

	combos := cartesianBatchCombinations(tuples)
	// 2 * 3 = 6 combinations.
	require.Len(t, combos, 6)

	// Verify all combinations are present (odometer order: a advances slowly, b fast).
	expected := [][2]string{
		{"1", "x"},
		{"1", "y"},
		{"1", "z"},
		{"2", "x"},
		{"2", "y"},
		{"2", "z"},
	}
	for i, combo := range combos {
		require.Len(t, combo, 2)
		assert.Equal(t, expected[i][0], string(combo[0].chunk[0].Value), "combo %d, tuple a", i)
		assert.Equal(t, expected[i][1], string(combo[1].chunk[0].Value), "combo %d, tuple b", i)
	}
}

func TestCartesianBatchCombinations_Empty(t *testing.T) {
	combos := cartesianBatchCombinations(nil)
	assert.Nil(t, combos)
}

func TestCloneBindVarsWithTuples(t *testing.T) {
	original := map[string]*querypb.BindVariable{
		"ids":    makeTupleBindVar(1, 2, 3, 4, 5),
		"name":   sqltypes.StringBindVariable("test"),
		"status": sqltypes.Int64BindVariable(1),
	}

	replacements := []tupleReplacement{
		{
			name:  "ids",
			chunk: makeTupleBindVar(1, 2).Values,
		},
	}

	cloned := cloneBindVarsWithTuples(original, replacements)

	// The cloned map should have the replaced tuple.
	assert.Equal(t, 2, len(cloned["ids"].Values))
	// Other bind vars should be preserved.
	assert.Equal(t, "test", string(cloned["name"].Value))
	statusVal, err := sqltypes.MakeTrusted(cloned["status"].Type, cloned["status"].Value).ToInt64()
	require.NoError(t, err)
	assert.Equal(t, int64(1), statusVal)

	// Original should be unchanged.
	assert.Equal(t, 5, len(original["ids"].Values))
}

// makeTupleBindVar builds a TUPLE bind variable from a list of int64 values.
func makeTupleBindVar(vals ...int64) *querypb.BindVariable {
	bv := &querypb.BindVariable{Type: querypb.Type_TUPLE}
	for _, v := range vals {
		bv.Values = append(bv.Values, &querypb.Value{
			Type:  querypb.Type_INT64,
			Value: []byte(fmt.Sprintf("%d", v)),
		})
	}
	return bv
}
