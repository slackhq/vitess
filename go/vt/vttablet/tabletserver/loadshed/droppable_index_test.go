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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testDroppableIndex = droppableIndex[struct{}]

// idxReq builds a droppable request at the given priority for index tests.
func idxReq(priority int) *testRequest {
	return newRequest(struct{}{}, priority)
}

// TestDroppableIndex_Empty: max of an empty index returns nil.
func TestDroppableIndex_Empty(t *testing.T) {
	var idx testDroppableIndex
	idx.init()
	assert.Nil(t, idx.max())
}

// TestDroppableIndex_LowestPriorityWins: max returns the request with the
// highest numeric priority.
func TestDroppableIndex_LowestPriorityWins(t *testing.T) {
	var idx testDroppableIndex
	idx.init()

	r10 := idxReq(10)
	idx.insert(r10)
	idx.insert(idxReq(1))
	idx.insert(idxReq(5))

	assert.Same(t, r10, idx.max())
}

// TestDroppableIndex_FIFOWithinBucket: among equal priorities, max returns the
// oldest (first inserted) — matching the front-most tie-break of the old scan.
func TestDroppableIndex_FIFOWithinBucket(t *testing.T) {
	var idx testDroppableIndex
	idx.init()

	first := idxReq(5)
	second := idxReq(5)
	idx.insert(first)
	idx.insert(second)

	assert.Same(t, first, idx.max())
	idx.remove(first)
	assert.Same(t, second, idx.max(), "after removing the oldest, next-oldest at same priority is picked")
}

// TestDroppableIndex_RemoveMiddle: removing a request that is not the max is
// O(1) and leaves max unchanged.
func TestDroppableIndex_RemoveMiddle(t *testing.T) {
	var idx testDroppableIndex
	idx.init()

	highest := idxReq(1)
	mid := idxReq(5)
	idx.insert(highest)
	idx.insert(mid)

	idx.remove(highest)
	assert.Same(t, mid, idx.max())
}

// TestDroppableIndex_RemoveEmptiesBucket: removing the last entry of a bucket
// clears its occupancy bit so max advances to the next non-empty bucket.
func TestDroppableIndex_RemoveEmptiesBucket(t *testing.T) {
	var idx testDroppableIndex
	idx.init()

	low := idxReq(1)
	high := idxReq(50)
	idx.insert(low)
	idx.insert(high)

	require.Same(t, high, idx.max())
	idx.remove(high)
	assert.Same(t, low, idx.max())
	idx.remove(low)
	assert.Nil(t, idx.max())
}

// TestDroppableIndex_Priority1 and Priority100 are the droppable domain
// boundaries.
func TestDroppableIndex_DomainBoundaries(t *testing.T) {
	var idx testDroppableIndex
	idx.init()

	r100 := idxReq(100)
	r1 := idxReq(1)
	idx.insert(r100)
	idx.insert(r1)

	assert.Same(t, r100, idx.max())
	idx.remove(r100)
	assert.Same(t, r1, idx.max())
}

// TestDroppableIndex_SecondWordBoundary exercises the 64-bit word split in the
// occupancy bitset: bucket 63 (word 0) vs bucket 64 (word 1).
func TestDroppableIndex_SecondWordBoundary(t *testing.T) {
	var idx testDroppableIndex
	idx.init()

	r64 := idxReq(64)
	idx.insert(r64)
	assert.Same(t, r64, idx.max())

	r63 := idxReq(63)
	idx.insert(r63)
	assert.Same(t, r64, idx.max(), "priority 64 outranks priority 63")
}
