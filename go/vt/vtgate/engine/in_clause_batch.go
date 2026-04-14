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
	"maps"
	"sort"
	"strings"

	"vitess.io/vitess/go/stats"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var inClauseBatchCount = stats.NewCounter("InClauseBatchCount", "Number of queries where IN-clause batching was applied")

// oversizedTuple holds the name and bind variable of a tuple that exceeds the
// batch size threshold, along with its pre-computed chunks.
type oversizedTuple struct {
	name   string
	chunks [][]*querypb.Value
}

// findOversizedTuples scans a bind variable map and returns all TUPLE-type
// bind variables whose value count exceeds the threshold. Results are sorted
// by name for deterministic batching order. It skips bind vars used for vindex
// routing (__vals, __vals0, etc.).
func findOversizedTuples(bvs map[string]*querypb.BindVariable, threshold int) []oversizedTuple {
	var result []oversizedTuple
	for name, bv := range bvs {
		if bv.Type != querypb.Type_TUPLE {
			continue
		}
		if strings.HasPrefix(name, ListVarName) {
			continue
		}
		if len(bv.Values) > threshold {
			result = append(result, oversizedTuple{
				name:   name,
				chunks: chunkTupleValues(bv.Values, threshold),
			})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].name < result[j].name
	})
	return result
}

// findLargestTupleBV scans a bind variable map and returns the name of the
// TUPLE-type bind variable with the most values that exceeds the given
// threshold. It skips bind vars used for vindex routing (__vals, __vals0, etc.).
// Returns ("", nil) if no tuple exceeds the threshold.
func findLargestTupleBV(bvs map[string]*querypb.BindVariable, threshold int) (string, *querypb.BindVariable) {
	var bestName string
	var bestBV *querypb.BindVariable
	bestLen := threshold

	for name, bv := range bvs {
		if bv.Type != querypb.Type_TUPLE {
			continue
		}
		if strings.HasPrefix(name, ListVarName) {
			continue
		}
		if len(bv.Values) > bestLen {
			bestName = name
			bestBV = bv
			bestLen = len(bv.Values)
		}
	}

	return bestName, bestBV
}

// chunkTupleValues splits a slice of bind variable values into chunks of at
// most batchSize elements. The returned slices share the underlying pointers
// with the input (no deep copy).
func chunkTupleValues(values []*querypb.Value, batchSize int) [][]*querypb.Value {
	if batchSize <= 0 || len(values) == 0 {
		return [][]*querypb.Value{values}
	}

	numChunks := (len(values) + batchSize - 1) / batchSize
	chunks := make([][]*querypb.Value, 0, numChunks)

	for i := 0; i < len(values); i += batchSize {
		end := min(i+batchSize, len(values))
		chunks = append(chunks, values[i:end])
	}

	return chunks
}

// cloneBindVarsWithTuple creates a shallow copy of the bind var map,
// replacing the named tuple bind variable with a new one containing the
// provided chunk of values.
func cloneBindVarsWithTuple(original map[string]*querypb.BindVariable, tupleName string, chunk []*querypb.Value) map[string]*querypb.BindVariable {
	out := make(map[string]*querypb.BindVariable, len(original))
	maps.Copy(out, original)
	out[tupleName] = &querypb.BindVariable{
		Type:   querypb.Type_TUPLE,
		Values: chunk,
	}
	return out
}

// cloneBindVarsWithTuples creates a shallow copy of the bind var map,
// replacing multiple named tuple bind variables with new ones containing
// the provided chunk values.
func cloneBindVarsWithTuples(original map[string]*querypb.BindVariable, replacements []tupleReplacement) map[string]*querypb.BindVariable {
	out := make(map[string]*querypb.BindVariable, len(original))
	maps.Copy(out, original)
	for _, r := range replacements {
		out[r.name] = &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: r.chunk,
		}
	}
	return out
}

type tupleReplacement struct {
	name  string
	chunk []*querypb.Value
}

// cartesianBatchCombinations generates all combinations of chunk indices across
// multiple oversized tuples. For example, if tuple A has 2 chunks and tuple B
// has 3 chunks, this returns 6 combinations: [(0,0), (0,1), (0,2), (1,0), (1,1), (1,2)].
// Each combination is a slice of tupleReplacement ready for cloneBindVarsWithTuples.
func cartesianBatchCombinations(tuples []oversizedTuple) [][]tupleReplacement {
	if len(tuples) == 0 {
		return nil
	}

	totalCombinations := 1
	for _, t := range tuples {
		totalCombinations *= len(t.chunks)
	}

	result := make([][]tupleReplacement, 0, totalCombinations)
	indices := make([]int, len(tuples))

	for i := 0; i < totalCombinations; i++ {
		combo := make([]tupleReplacement, len(tuples))
		for j, t := range tuples {
			combo[j] = tupleReplacement{
				name:  t.name,
				chunk: t.chunks[indices[j]],
			}
		}
		result = append(result, combo)

		// Increment indices (odometer-style, rightmost advances first)
		for j := len(indices) - 1; j >= 0; j-- {
			indices[j]++
			if indices[j] < len(tuples[j].chunks) {
				break
			}
			indices[j] = 0
		}
	}

	return result
}
