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
	outcome uint8

	// Request represents an entry in the CoDel queue. Named a 'request' since
	// it may be dropped or dequeued.
	Request[T any] struct {
		priority           int
		codelqEnqueuedAtNs int64
		codelqElem         *list.Element[*Request[T]]
		valveID            string
		outcome            outcome
		value              T

		priorityInheritanceKey string

		// bucketElem locates this request in the droppableIndex while it is a
		// droppable queue entry: it is the request's node in its priority
		// bucket's FIFO list, enabling O(1) removal. bucketIdx is the bucket that
		// node lives in. bucketElem is nil when the request is not indexed
		// (undroppable, dequeued, or removed).
		bucketElem *list.Element[*Request[T]]
		bucketIdx  int
	}
)

const (
	outcomePending outcome = iota
	outcomeDequeued
	outcomeCanceled
	outcomeShed
	outcomeDrained
)

// PriorityUndroppable is a sentinel priority indicating a request that must
// never be dropped by CoDel.
const PriorityUndroppable = 0

func IsValidPriority(priority int) bool {
	return priority >= PriorityUndroppable && priority <= sqlparser.MaxPriorityValue
}

func newRequest[T any](value T, priority int) *Request[T] {
	return &Request[T]{
		value:    value,
		priority: priority,
	}
}

func (r *Request[T]) isDroppable() bool {
	return r.priority != PriorityUndroppable
}

func (r *Request[T]) done() bool {
	return r.outcome != outcomePending
}

func (r *Request[T]) markDone(outcome outcome) {
	if r.done() {
		panic("loadshed: request completed more than once")
	}
	r.outcome = outcome
}
