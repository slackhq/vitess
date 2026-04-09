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
	"strings"

	"vitess.io/vitess/go/stats"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var inClauseBatchCount = stats.NewCounter("InClauseBatchCount", "Number of queries where IN-clause batching was applied")

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
		end := i + batchSize
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[i:end])
	}

	return chunks
}

// cloneBindVarsWithTuple creates a shallow copy of the bind var map,
// replacing the named tuple bind variable with a new one containing the
// provided chunk of values.
func cloneBindVarsWithTuple(original map[string]*querypb.BindVariable, tupleName string, chunk []*querypb.Value) map[string]*querypb.BindVariable {
	out := make(map[string]*querypb.BindVariable, len(original))
	for k, v := range original {
		out[k] = v
	}
	out[tupleName] = &querypb.BindVariable{
		Type:   querypb.Type_TUPLE,
		Values: chunk,
	}
	return out
}
