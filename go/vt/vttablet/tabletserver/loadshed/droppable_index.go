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
)

const (
	maxPriorityBucket  = 100
	numPriorityBuckets = maxPriorityBucket + 1
	overflowBucket     = -1
)

type droppableIndex[T any] struct {
	buckets  [numPriorityBuckets]list.List[*Request[T]]
	overflow list.List[*Request[T]]
	occupied [2]uint64
}

func (idx *droppableIndex[T]) init() {
	for i := range idx.buckets {
		idx.buckets[i].Init()
	}
	idx.overflow.Init()
}

func bucketFor(priority float64) int {
	if priority < 0 || priority > maxPriorityBucket {
		return overflowBucket
	}
	bucket := int(priority)
	if float64(bucket) != priority {
		return overflowBucket
	}
	return bucket
}

func (idx *droppableIndex[T]) insert(req *Request[T]) {
	bucket := bucketFor(req.priority)
	req.bucketIdx = bucket
	if bucket == overflowBucket {
		req.bucketElem = idx.overflow.PushBack(req)
		return
	}
	req.bucketElem = idx.buckets[bucket].PushBack(req)
	idx.occupied[bucket>>6] |= 1 << (uint(bucket) & 63)
}

func (idx *droppableIndex[T]) remove(req *Request[T]) {
	if req.bucketElem == nil {
		return
	}
	bucket := req.bucketIdx
	if bucket == overflowBucket {
		idx.overflow.Remove(req.bucketElem)
	} else {
		idx.buckets[bucket].Remove(req.bucketElem)
		if idx.buckets[bucket].Len() == 0 {
			idx.occupied[bucket>>6] &^= 1 << (uint(bucket) & 63)
		}
	}
	req.bucketElem = nil
}

func (idx *droppableIndex[T]) min() *Request[T] {
	if bucket := idx.lowestOccupiedBucket(); bucket >= 0 {
		return idx.buckets[bucket].Front().Value
	}
	if elem := idx.overflow.Front(); elem != nil {
		return elem.Value
	}
	return nil
}

func (idx *droppableIndex[T]) lowestOccupiedBucket() int {
	if idx.occupied[0] != 0 {
		return bits.TrailingZeros64(idx.occupied[0])
	}
	if idx.occupied[1] != 0 {
		return 64 + bits.TrailingZeros64(idx.occupied[1])
	}
	return -1
}
