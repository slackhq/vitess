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

package loadshed

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// idxReq builds a droppable request at the given priority for index tests.
func idxReq(priority float64) *Request {
	return newRequest(priority)
}

// TestDroppableIndex_Empty: min of an empty index returns nil.
func TestDroppableIndex_Empty(t *testing.T) {
	var idx droppableIndex
	idx.init()
	assert.Nil(t, idx.min())
}

// TestDroppableIndex_LowestPriorityWins: min returns the request in the
// lowest-numbered non-empty bucket.
func TestDroppableIndex_LowestPriorityWins(t *testing.T) {
	var idx droppableIndex
	idx.init()

	idx.insert(idxReq(10))
	r1 := idxReq(1)
	idx.insert(r1)
	idx.insert(idxReq(5))

	assert.Same(t, r1, idx.min())
}

// TestDroppableIndex_FIFOWithinBucket: among equal priorities, min returns the
// oldest (first inserted) — matching the front-most tie-break of the old scan.
func TestDroppableIndex_FIFOWithinBucket(t *testing.T) {
	var idx droppableIndex
	idx.init()

	first := idxReq(5)
	second := idxReq(5)
	idx.insert(first)
	idx.insert(second)

	assert.Same(t, first, idx.min())
	idx.remove(first)
	assert.Same(t, second, idx.min(), "after removing the oldest, next-oldest at same priority is picked")
}

// TestDroppableIndex_RemoveMiddle: removing a request that is not the min is
// O(1) and leaves min unchanged.
func TestDroppableIndex_RemoveMiddle(t *testing.T) {
	var idx droppableIndex
	idx.init()

	lowest := idxReq(1)
	mid := idxReq(5)
	idx.insert(lowest)
	idx.insert(mid)

	idx.remove(mid)
	assert.Same(t, lowest, idx.min())
}

// TestDroppableIndex_RemoveEmptiesBucket: removing the last entry of a bucket
// clears its occupancy bit so min advances to the next non-empty bucket.
func TestDroppableIndex_RemoveEmptiesBucket(t *testing.T) {
	var idx droppableIndex
	idx.init()

	low := idxReq(1)
	high := idxReq(50)
	idx.insert(low)
	idx.insert(high)

	require.Same(t, low, idx.min())
	idx.remove(low)
	assert.Same(t, high, idx.min())
	idx.remove(high)
	assert.Nil(t, idx.min())
}

// TestDroppableIndex_Priority0 and Priority100 are the domain boundaries
// (production priorities are integers in [0,100]).
func TestDroppableIndex_DomainBoundaries(t *testing.T) {
	var idx droppableIndex
	idx.init()

	r100 := idxReq(100)
	r0 := idxReq(0)
	idx.insert(r100)
	idx.insert(r0)

	assert.Same(t, r0, idx.min())
	idx.remove(r0)
	assert.Same(t, r100, idx.min())
}

// TestDroppableIndex_Overflow: non-integer, out-of-range, and +Inf priorities
// land in the overflow list. They are only picked when no in-domain bucket has
// entries, and among overflow entries the oldest wins.
func TestDroppableIndex_Overflow(t *testing.T) {
	var idx droppableIndex
	idx.init()

	inf := idxReq(math.Inf(1))
	idx.insert(inf)
	assert.Same(t, inf, idx.min(), "overflow entry is picked when it is the only one")

	// An in-domain bucket always outranks overflow (overflow is treated as the
	// highest/last priority).
	r5 := idxReq(5)
	idx.insert(r5)
	assert.Same(t, r5, idx.min())
	idx.remove(r5)
	assert.Same(t, inf, idx.min())
}

// TestDroppableIndex_OverflowFIFO: multiple overflow entries preserve insertion
// order.
func TestDroppableIndex_OverflowFIFO(t *testing.T) {
	var idx droppableIndex
	idx.init()

	first := idxReq(math.Inf(1))
	second := idxReq(1000) // out of [0,100] range → overflow
	idx.insert(first)
	idx.insert(second)

	assert.Same(t, first, idx.min())
	idx.remove(first)
	assert.Same(t, second, idx.min())
}

// TestDroppableIndex_SecondWordBoundary exercises the 64-bit word split in the
// occupancy bitset: bucket 63 (word 0) vs bucket 64 (word 1).
func TestDroppableIndex_SecondWordBoundary(t *testing.T) {
	var idx droppableIndex
	idx.init()

	r64 := idxReq(64)
	idx.insert(r64)
	assert.Same(t, r64, idx.min())

	r63 := idxReq(63)
	idx.insert(r63)
	assert.Same(t, r63, idx.min(), "bucket 63 (word 0) outranks bucket 64 (word 1)")
}
