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
	"vitess.io/vitess/go/list"
	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	Request[T any] struct {
		priority           int
		codelqEnqueuedAtNs int64
		codelqElem         *list.Element[*Request[T]]
		value              T

		// bucketElem locates this request in the priorityIndex while it is a
		// queue entry: it is the request's node in its priority bucket's FIFO
		// list, enabling O(1) removal. bucketIdx is the bucket that node lives
		// in. bucketElem is nil when the request is not queued.
		bucketElem *list.Element[*Request[T]]
		bucketIdx  int

		// skips counts dequeues that bypassed this request while it was the
		// over-target head; bounds its wait under priority dequeue.
		skips int
	}
)

// PriorityUndroppable is a sentinel priority indicating a request that must
// never be dropped by CoDel.
const PriorityUndroppable = 0

func IsValidPriority(priority int) bool {
	return priority >= PriorityUndroppable && priority <= sqlparser.MaxPriorityValue
}

func newRequest[T any](value T, priority int) *Request[T] {
	return &Request[T]{
		priority: priority,
		value:    value,
	}
}

func (r *Request[T]) isDroppable() bool {
	return r.priority != PriorityUndroppable
}
