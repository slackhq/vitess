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
	"math/bits"

	"vitess.io/vitess/go/list"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Snake priorities use the query-priority scale: 0 is undroppable, and
// priorities 1 through 100 become progressively less important. We keep one
// FIFO bucket per droppable priority so the least-important request is found in
// O(1) instead of an O(n) list scan.
const (
	numPriorityBuckets = sqlparser.MaxPriorityValue
)

// droppableIndex indexes the droppable requests currently in the CoDel queue by
// priority so the least-important (oldest, on ties) can be found in O(1). Each
// bucket is a FIFO list; a 2-word occupancy bitset marks which buckets are
// non-empty so max() is a leading-zeros scan rather than a walk.
//
// Not safe for concurrent use; the caller holds the queue mutex.
type droppableIndex[T any] struct {
	buckets [numPriorityBuckets]list.List[*Request[T]]
	// occ is the occupancy bitset over buckets: bit i is set iff buckets[i] is
	// non-empty. Two words cover the 100 droppable priority levels.
	occ [2]uint64
}

// init prepares the index for use. The zero list.List is a valid empty list, so
// this only needs to run once (idempotent) and mainly documents intent.
func (idx *droppableIndex[T]) init() {
	for i := range idx.buckets {
		idx.buckets[i].Init()
	}
	idx.occ = [2]uint64{}
}

// insert adds a droppable request to its priority bucket (FIFO). Records the
// bucket and list node on the request for O(1) removal. Must not be called for
// an undroppable request.
func (idx *droppableIndex[T]) insert(req *Request[T]) {
	b := req.priority - 1
	req.bucketIdx = b
	req.bucketElem = idx.buckets[b].PushBack(req)
	idx.occ[b>>6] |= 1 << (uint(b) & 63)
}

// remove unlinks a request from its bucket in O(1). No-op if the request is not
// currently indexed. Clears the bucket's occupancy bit if it becomes empty.
func (idx *droppableIndex[T]) remove(req *Request[T]) {
	if req.bucketElem == nil {
		return
	}
	b := req.bucketIdx
	idx.buckets[b].Remove(req.bucketElem)
	if idx.buckets[b].Len() == 0 {
		idx.occ[b>>6] &^= 1 << (uint(b) & 63)
	}
	req.bucketElem = nil
}

// max returns the least-important droppable request — the oldest in the
// highest-numbered non-empty bucket — or nil if the index is empty.
func (idx *droppableIndex[T]) max() *Request[T] {
	if b := idx.highestOccupiedBucket(); b >= 0 {
		return idx.buckets[b].Front().Value
	}
	return nil
}

// highestOccupiedBucket returns the highest non-empty bucket index, or -1 if
// all buckets are empty. O(1) via leading-zeros on the
// occupancy words.
func (idx *droppableIndex[T]) highestOccupiedBucket() int {
	if idx.occ[1] != 0 {
		return 64 + 63 - bits.LeadingZeros64(idx.occ[1])
	}
	if idx.occ[0] != 0 {
		return 63 - bits.LeadingZeros64(idx.occ[0])
	}
	return -1
}
