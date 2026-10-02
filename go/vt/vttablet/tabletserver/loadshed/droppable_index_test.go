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

type testPriorityIndex = priorityIndex[struct{}]

// idxReq builds a droppable request at the given priority for index tests.
func idxReq(priority int) *testRequest {
	return newRequest(struct{}{}, priority)
}

// TestPriorityIndex_Empty: max of an empty index returns nil.
func TestPriorityIndex_Empty(t *testing.T) {
	var idx testPriorityIndex
	idx.init()
	assert.Nil(t, idx.max())
}

// TestPriorityIndex_LowestPriorityWins: max returns the request with the
// highest numeric priority.
func TestPriorityIndex_LowestPriorityWins(t *testing.T) {
	var idx testPriorityIndex
	idx.init()

	r10 := idxReq(10)
	idx.insert(r10)
	idx.insert(idxReq(1))
	idx.insert(idxReq(5))

	assert.Same(t, r10, idx.max())
}

// TestPriorityIndex_FIFOWithinBucket: among equal priorities, max returns the
// oldest (first inserted) — matching the front-most tie-break of the old scan.
func TestPriorityIndex_FIFOWithinBucket(t *testing.T) {
	var idx testPriorityIndex
	idx.init()

	first := idxReq(5)
	second := idxReq(5)
	idx.insert(first)
	idx.insert(second)

	assert.Same(t, first, idx.max())
	idx.remove(first)
	assert.Same(t, second, idx.max(), "after removing the oldest, next-oldest at same priority is picked")
}

// TestPriorityIndex_RemoveMiddle: removing a request that is not the max is
// O(1) and leaves max unchanged.
func TestPriorityIndex_RemoveMiddle(t *testing.T) {
	var idx testPriorityIndex
	idx.init()

	highest := idxReq(1)
	mid := idxReq(5)
	idx.insert(highest)
	idx.insert(mid)

	idx.remove(highest)
	assert.Same(t, mid, idx.max())
}

// TestPriorityIndex_RemoveEmptiesBucket: removing the last entry of a bucket
// clears its occupancy bit so max advances to the next non-empty bucket.
func TestPriorityIndex_RemoveEmptiesBucket(t *testing.T) {
	var idx testPriorityIndex
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

// TestPriorityIndex_Priority1 and Priority100 are the droppable domain
// boundaries.
func TestPriorityIndex_DomainBoundaries(t *testing.T) {
	var idx testPriorityIndex
	idx.init()

	r100 := idxReq(100)
	r1 := idxReq(1)
	idx.insert(r100)
	idx.insert(r1)

	assert.Same(t, r100, idx.max())
	idx.remove(r100)
	assert.Same(t, r1, idx.max())
}

// TestPriorityIndex_SecondWordBoundary exercises the 64-bit word split in the
// occupancy bitset: bucket 63 (word 0) vs bucket 64 (word 1).
func TestPriorityIndex_SecondWordBoundary(t *testing.T) {
	var idx testPriorityIndex
	idx.init()

	r64 := idxReq(64)
	idx.insert(r64)
	assert.Same(t, r64, idx.max())

	r63 := idxReq(63)
	idx.insert(r63)
	assert.Same(t, r64, idx.max(), "priority 64 outranks priority 63")
}

func idxReqAt(priority int, enqueuedAtNs int64) *testRequest {
	req := idxReq(priority)
	req.codelqEnqueuedAtNs = enqueuedAtNs
	return req
}

func TestPriorityIndex_MaxIgnoresUndroppable(t *testing.T) {
	var idx testPriorityIndex
	idx.init()

	idx.insert(idxReq(PriorityUndroppable))
	assert.Nil(t, idx.max())

	droppable := idxReq(1)
	idx.insert(droppable)
	assert.Same(t, droppable, idx.max())
}

func TestPriorityIndex_FirstOverTarget_Empty(t *testing.T) {
	var idx testPriorityIndex
	idx.init()
	assert.Nil(t, idx.firstOverTarget(100, nil))
}

func TestPriorityIndex_FirstOverTarget_MostImportantQualifyingBucket(t *testing.T) {
	var idx testPriorityIndex
	idx.init()

	idx.insert(idxReqAt(50, 10))
	mid := idxReqAt(20, 20)
	idx.insert(mid)
	idx.insert(idxReqAt(5, 200))

	assert.Same(t, mid, idx.firstOverTarget(100, nil), "priority 5 is under target, so priority 20 wins")
}

func TestPriorityIndex_FirstOverTarget_UndroppableWins(t *testing.T) {
	var idx testPriorityIndex
	idx.init()

	idx.insert(idxReqAt(1, 10))
	undroppable := idxReqAt(PriorityUndroppable, 20)
	idx.insert(undroppable)

	assert.Same(t, undroppable, idx.firstOverTarget(100, nil))
}

func TestPriorityIndex_FirstOverTarget_NoneQualifies(t *testing.T) {
	var idx testPriorityIndex
	idx.init()

	idx.insert(idxReqAt(1, 200))
	idx.insert(idxReqAt(50, 300))

	assert.Nil(t, idx.firstOverTarget(100, nil))
}

func idxValueAt(value string, priority int, enqueuedAtNs int64) *Request[string] {
	req := newRequest(value, priority)
	req.codelqEnqueuedAtNs = enqueuedAtNs
	return req
}

func TestPriorityIndex_FirstOverTarget_MatchWithinBucket(t *testing.T) {
	var idx priorityIndex[string]
	idx.init()

	idx.insert(idxValueAt("front", 5, 10))
	matching := idxValueAt("matching", 5, 20)
	idx.insert(matching)
	idx.insert(idxValueAt("other-bucket", 50, 5))

	var visited []string
	got := idx.firstOverTarget(100, func(value string) bool {
		visited = append(visited, value)
		return value == "matching"
	})

	assert.Same(t, matching, got)
	assert.Equal(t, []string{"front", "matching"}, visited, "match only sees the winning bucket")
}

func TestPriorityIndex_FirstOverTarget_NoMatchReturnsFront(t *testing.T) {
	var idx priorityIndex[string]
	idx.init()

	front := idxValueAt("front", 5, 10)
	idx.insert(front)
	idx.insert(idxValueAt("young", 5, 200))

	var visited []string
	got := idx.firstOverTarget(100, func(value string) bool {
		visited = append(visited, value)
		return false
	})

	assert.Same(t, front, got)
	assert.Equal(t, []string{"front"}, visited, "walk stops at the first under-target request")
}
