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
// FIFO bucket per priority so the least-important request is found in O(1)
// instead of an O(n) list scan.
const (
	numPriorityBuckets = sqlparser.MaxPriorityValue + 1
)

// priorityIndex indexes the requests currently in the CoDel queue by priority.
// Each bucket is a FIFO list; a 2-word occupancy bitset marks which buckets are
// non-empty so lookups scan bits rather than walk the queue.
//
// Not safe for concurrent use; the caller holds the queue mutex.
type priorityIndex[T any] struct {
	buckets [numPriorityBuckets]list.List[*Request[T]]
	// occ is the occupancy bitset over buckets: bit i is set iff buckets[i] is
	// non-empty. Two words cover the 101 priority levels.
	occ [2]uint64
}

// init prepares the index for use. The zero list.List is a valid empty list, so
// this only needs to run once (idempotent) and mainly documents intent.
func (idx *priorityIndex[T]) init() {
	for i := range idx.buckets {
		idx.buckets[i].Init()
	}
	idx.occ = [2]uint64{}
}

// insert adds a request to its priority bucket (FIFO). Records the bucket and
// list node on the request for O(1) removal.
func (idx *priorityIndex[T]) insert(req *Request[T]) {
	b := req.priority
	req.bucketIdx = b
	req.bucketElem = idx.buckets[b].PushBack(req)
	idx.occ[b>>6] |= 1 << (uint(b) & 63)
}

// remove unlinks a request from its bucket in O(1). No-op if the request is not
// currently indexed. Clears the bucket's occupancy bit if it becomes empty.
func (idx *priorityIndex[T]) remove(req *Request[T]) {
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
// highest-numbered non-empty droppable bucket — or nil if there is none.
func (idx *priorityIndex[T]) max() *Request[T] {
	if b := idx.highestOccupiedDroppableBucket(); b > PriorityUndroppable {
		return idx.buckets[b].Front().Value
	}
	return nil
}

// highestOccupiedDroppableBucket returns the highest non-empty bucket index
// above PriorityUndroppable, or -1 if there is none. O(1) via leading-zeros on
// the occupancy words.
func (idx *priorityIndex[T]) highestOccupiedDroppableBucket() int {
	if idx.occ[1] != 0 {
		return 64 + 63 - bits.LeadingZeros64(idx.occ[1])
	}
	if w := idx.occ[0] &^ (1 << PriorityUndroppable); w != 0 {
		return 63 - bits.LeadingZeros64(w)
	}
	return -1
}

// firstOverTarget returns a request enqueued at or before cutoffNs from the
// most important bucket whose front qualifies: the first such request in that
// bucket accepted by match, else the bucket's front. Returns nil if no bucket
// front qualifies. Priority inheritance appends to a bucket's back, so a bucket
// may not be in enqueue order; the walk stops at the first request after
// cutoffNs, which can miss a candidate but never returns one after cutoffNs.
func (idx *priorityIndex[T]) firstOverTarget(cutoffNs int64, match func(T) bool) *Request[T] {
	for w := range idx.occ {
		for word := idx.occ[w]; word != 0; word &= word - 1 {
			b := w<<6 + bits.TrailingZeros64(word)
			front := idx.buckets[b].Front()
			if front.Value.codelqEnqueuedAtNs > cutoffNs {
				continue
			}
			if match == nil {
				return front.Value
			}
			for e := front; e != nil && e.Value.codelqEnqueuedAtNs <= cutoffNs; e = e.Next() {
				if match(e.Value.value) {
					return e.Value
				}
			}
			return front.Value
		}
	}
	return nil
}
