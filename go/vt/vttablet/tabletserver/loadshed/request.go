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
	"errors"
	"math"
)

type (
	// Request represents an entry in the CoDel queue. Named a 'request' since
	// it may be dropped or dequeued. signaledValue allows the queue to inspect
	// terminal state without removing anything from the queue.
	Request[T any] struct {
		priority           float64
		codelqEnqueuedAtNs int64
		codelqElem         *list.Element
		valveID            string
		signaledValue      error
		value              T

		// bucketElem locates this request in the droppableIndex while it is a
		// droppable queue entry: it is the request's node in its priority
		// bucket's FIFO list, enabling O(1) removal. bucketIdx is the bucket that
		// node lives in (0..maxPriorityBucket, or overflowBucket). bucketElem is
		// nil when the request is not indexed (undroppable, dequeued, or removed).
		bucketElem *list.Element
		bucketIdx  int
	}
)

// PriorityUndroppable is a sentinel priority indicating a request that must
// never be dropped by CoDel. We use negative infinity so it's distinguishable
// from any real priority value (e.g. health-check queries against system
// schemas).
var PriorityUndroppable = math.Inf(-1)

var grantSentinel = errors.New("granted") //nolint:staticcheck // sentinel for request state

func newRequest[T any](priority float64) *Request[T] {
	return &Request[T]{
		priority: priority,
	}
}

func (r *Request[T]) isDroppable() bool {
	return r.priority != PriorityUndroppable
}

func (r *Request[T]) signal(val error) {
	if r.signaledValue != nil {
		panic("loadshed: signal called more than once")
	}
	r.signaledValue = val
}
