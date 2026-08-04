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
	"container/list"
	"math/bits"
)

// Production priorities are integers in [0, sqlparser.MaxPriorityValue] (0..100,
// with 0 the lowest/most-shed). We keep one FIFO bucket per integer priority so
// the lowest-priority droppable request is found in O(1) instead of an O(n) list
// scan. Anything outside this integer domain (non-integer, out-of-range, or
// +Inf — which the tests exercise) falls into an overflow list treated as the
// highest priority (shed last among droppables). Overflow is empty in
// production, so it is scanned only when non-empty.
const (
	maxPriorityBucket  = 100 // inclusive; sqlparser.MaxPriorityValue
	numPriorityBuckets = maxPriorityBucket + 1
	overflowBucket     = -1
)

// droppableIndex indexes the droppable requests currently in the CoDel queue by
// priority so the lowest-priority (oldest, on ties) can be found in O(1). Each
// bucket is a FIFO list; a 2-word occupancy bitset marks which buckets are
// non-empty so min() is a trailing-zeros scan rather than a walk.
//
// Not safe for concurrent use; the caller holds the queue mutex.
type droppableIndex struct {
	buckets  [numPriorityBuckets]list.List
	overflow list.List
	// occ is the occupancy bitset over buckets: bit i is set iff buckets[i] is
	// non-empty. Two words cover 0..127, which spans the 0..100 domain.
	occ [2]uint64
}

// init prepares the index for use. The zero list.List is a valid empty list, so
// this only needs to run once (idempotent) and mainly documents intent.
func (idx *droppableIndex) init() {
	for i := range idx.buckets {
		idx.buckets[i].Init()
	}
	idx.overflow.Init()
	idx.occ = [2]uint64{}
}

// bucketFor returns the bucket index for a priority: its integer value when it
// is an integer in [0, maxPriorityBucket], else overflowBucket.
func bucketFor(priority float64) int {
	if priority < 0 || priority > maxPriorityBucket {
		return overflowBucket
	}
	if i := int(priority); float64(i) == priority {
		return i
	}
	return overflowBucket
}

// insert adds a droppable request to its priority bucket (FIFO). Records the
// bucket and list node on the request for O(1) removal. Must not be called for
// an undroppable request.
func (idx *droppableIndex) insert(req *Request) {
	b := bucketFor(req.priority)
	req.bucketIdx = b
	if b == overflowBucket {
		req.bucketElem = idx.overflow.PushBack(req)
		return
	}
	req.bucketElem = idx.buckets[b].PushBack(req)
	idx.occ[b>>6] |= 1 << (uint(b) & 63)
}

// remove unlinks a request from its bucket in O(1). No-op if the request is not
// currently indexed. Clears the bucket's occupancy bit if it becomes empty.
func (idx *droppableIndex) remove(req *Request) {
	if req.bucketElem == nil {
		return
	}
	b := req.bucketIdx
	if b == overflowBucket {
		idx.overflow.Remove(req.bucketElem)
	} else {
		idx.buckets[b].Remove(req.bucketElem)
		if idx.buckets[b].Len() == 0 {
			idx.occ[b>>6] &^= 1 << (uint(b) & 63)
		}
	}
	req.bucketElem = nil
}

// min returns the lowest-priority droppable request — the oldest in the
// lowest-numbered non-empty bucket — or nil if the index is empty. In-domain
// buckets always outrank the overflow list.
func (idx *droppableIndex) min() *Request {
	if b := idx.lowestOccupiedBucket(); b >= 0 {
		return idx.buckets[b].Front().Value.(*Request)
	}
	if e := idx.overflow.Front(); e != nil {
		return e.Value.(*Request)
	}
	return nil
}

// lowestOccupiedBucket returns the lowest non-empty in-domain bucket index, or
// -1 if all in-domain buckets are empty. O(1) via trailing-zeros on the
// occupancy words.
func (idx *droppableIndex) lowestOccupiedBucket() int {
	if idx.occ[0] != 0 {
		return bits.TrailingZeros64(idx.occ[0])
	}
	if idx.occ[1] != 0 {
		return 64 + bits.TrailingZeros64(idx.occ[1])
	}
	return -1
}
