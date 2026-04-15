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
	"maps"
	"runtime"
	"sort"
	"strings"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*BatchIN)(nil)

var (
	inClauseBatchCount      = stats.NewCounter("InClauseBatchCount", "Number of queries where IN-clause batching was applied")
	inClauseBatchChunkCount = stats.NewCountersWithSingleLabel("InClauseBatchChunkCount", "Number of chunks executed per batched query", "chunks")
	inClauseBatchErrors     = stats.NewCounter("InClauseBatchErrors", "Errors during batch execution")
)

type (
	// BatchIN wraps a plan sub-tree and transparently splits large IN-clause
	// bind variables into batches at execution time. It re-executes the wrapped
	// plan once per batch (or once per cartesian combination when multiple
	// IN-clauses exceed the threshold), then merges the partial results using
	// the merge descriptor populated by the planner.
	BatchIN struct {
		noTxNeeded

		// Input is the full plan sub-tree to execute per batch.
		Input Primitive

		// OrderBy specifies how to re-sort the merged result across batches.
		OrderBy evalengine.Comparison

		// Limit re-truncates the merged result to at most this many rows.
		// nil means no limit.
		Limit *int
	}

	// oversizedTuple holds the name and bind variable of a tuple that exceeds
	// the batch size threshold, along with its pre-computed chunks.
	oversizedTuple struct {
		name   string
		chunks [][]*querypb.Value
	}

	// tupleReplacement pairs a bind var name with a chunk of values to
	// substitute for one batch iteration.
	tupleReplacement struct {
		name  string
		chunk []*querypb.Value
	}

	// batchResult collects the result from a single goroutine.
	batchResult struct {
		idx    int
		result *sqltypes.Result
		err    error
	}
)

// TryExecute implements the Primitive interface.
func (b *BatchIN) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	batchSize := vcursor.GetInClauseBatchSize()
	if batchSize <= 0 {
		return vcursor.ExecutePrimitive(ctx, b.Input, bindVars, wantfields)
	}

	tuples := findOversizedTuples(bindVars, batchSize)
	if len(tuples) == 0 {
		return vcursor.ExecutePrimitive(ctx, b.Input, bindVars, wantfields)
	}

	inClauseBatchCount.Add(1)
	combinations := cartesianBatchCombinations(tuples)
	inClauseBatchChunkCount.Add(fmt.Sprintf("%d", len(combinations)), 1)

	results, err := b.fanOutBatches(ctx, vcursor, bindVars, wantfields, combinations)
	if err != nil {
		inClauseBatchErrors.Add(1)
		return nil, err
	}

	return b.mergeResults(results)
}

// TryStreamExecute implements the Primitive interface. When batching is needed,
// it falls back to non-streaming execution since we need to collect all results
// to re-sort and merge across batches.
func (b *BatchIN) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	batchSize := vcursor.GetInClauseBatchSize()
	if batchSize <= 0 {
		return vcursor.StreamExecutePrimitive(ctx, b.Input, bindVars, wantfields, callback)
	}

	tuples := findOversizedTuples(bindVars, batchSize)
	if len(tuples) == 0 {
		return vcursor.StreamExecutePrimitive(ctx, b.Input, bindVars, wantfields, callback)
	}

	// Fall back to non-streaming for batched queries.
	result, err := b.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(result)
}

// GetFields implements the Primitive interface.
func (b *BatchIN) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return b.Input.GetFields(ctx, vcursor, bindVars)
}

// NeedsTransaction implements the Primitive interface.
func (b *BatchIN) NeedsTransaction() bool {
	return b.Input.NeedsTransaction()
}

// Inputs implements the Primitive interface.
func (b *BatchIN) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{b.Input}, nil
}

func (b *BatchIN) description() PrimitiveDescription {
	other := map[string]any{}
	if len(b.OrderBy) > 0 {
		orderBy := make([]string, 0, len(b.OrderBy))
		for _, o := range b.OrderBy {
			orderBy = append(orderBy, o.String())
		}
		other["OrderBy"] = strings.Join(orderBy, ", ")
	}
	if b.Limit != nil {
		other["Limit"] = *b.Limit
	}
	return PrimitiveDescription{
		OperatorType: "BatchIN",
		Other:        other,
	}
}

// fanOutBatches executes the Input primitive once per cartesian combination,
// using goroutines with a concurrency cap.
func (b *BatchIN) fanOutBatches(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	wantfields bool,
	combinations [][]tupleReplacement,
) ([]*sqltypes.Result, error) {
	if len(combinations) == 1 {
		bvs := cloneBindVarsWithTuples(bindVars, combinations[0])
		result, err := vcursor.ExecutePrimitive(ctx, b.Input, bvs, wantfields)
		if err != nil {
			return nil, err
		}
		return []*sqltypes.Result{result}, nil
	}

	maxConcurrency := max(8, runtime.GOMAXPROCS(0))
	if maxConcurrency > len(combinations) {
		maxConcurrency = len(combinations)
	}
	sem := make(chan struct{}, maxConcurrency)

	ch := make(chan batchResult, len(combinations))
	var wg sync.WaitGroup
	wg.Add(len(combinations))

	for i, combo := range combinations {
		sem <- struct{}{}
		go func(idx int, combo []tupleReplacement) {
			defer wg.Done()
			defer func() { <-sem }()

			bvs := cloneBindVarsWithTuples(bindVars, combo)
			result, err := vcursor.ExecutePrimitive(ctx, b.Input, bvs, idx == 0)
			ch <- batchResult{idx: idx, result: result, err: err}
		}(i, combo)
	}

	wg.Wait()
	close(ch)

	results := make([]*sqltypes.Result, len(combinations))
	for br := range ch {
		if br.err != nil {
			return nil, br.err
		}
		results[br.idx] = br.result
	}

	return results, nil
}

// mergeResults concatenates rows from all batch results, re-sorts if
// OrderBy is set, and truncates to Limit if specified.
func (b *BatchIN) mergeResults(results []*sqltypes.Result) (*sqltypes.Result, error) {
	if len(results) == 0 {
		return &sqltypes.Result{}, nil
	}

	combined := results[0]
	for _, r := range results[1:] {
		combined.AppendResult(r)
	}

	if len(b.OrderBy) > 0 && len(results) > 1 {
		if err := b.OrderBy.SortResult(combined); err != nil {
			return nil, vterrors.Wrapf(err, "BatchIN: failed to re-sort merged results")
		}
	}

	if b.Limit != nil && len(combined.Rows) > *b.Limit {
		combined.Rows = combined.Rows[:*b.Limit]
	}

	return combined, nil
}

// --- Chunking and cartesian product helpers ---

// findOversizedTuples scans a bind variable map and returns all TUPLE-type
// bind variables whose value count exceeds the threshold. Results are sorted
// by name for deterministic batching order. Bind vars used for vindex routing
// (__vals, __vals0, etc.) are skipped.
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

// chunkTupleValues splits a slice of bind variable values into chunks of at
// most batchSize elements.
func chunkTupleValues(values []*querypb.Value, batchSize int) [][]*querypb.Value {
	if batchSize <= 0 || len(values) == 0 {
		return [][]*querypb.Value{values}
	}

	chunks := make([][]*querypb.Value, 0, (len(values)+batchSize-1)/batchSize)
	for i := 0; i < len(values); i += batchSize {
		end := min(i+batchSize, len(values))
		chunks = append(chunks, values[i:end])
	}
	return chunks
}

// cloneBindVarsWithTuples creates a shallow copy of the bind var map,
// replacing multiple named tuple bind variables with the provided chunks.
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

// cartesianBatchCombinations generates all combinations of chunk indices across
// multiple oversized tuples. For a single tuple with N chunks, this returns N
// single-element combinations. For multiple tuples this produces the full
// cartesian product.
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

	for range totalCombinations {
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
