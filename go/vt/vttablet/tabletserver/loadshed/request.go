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
	"math"
	"sync/atomic"
)

type (
	// Request represents an entry in the CoDel queue. It may be granted (lock
	// acquired) or dropped (load shed). Each request owns a result channel that
	// receives nil on grant or a *DroppedRequestError on drop. The signaled
	// flag and outcome field allow non-consuming inspection of the outcome
	// (used by lockedPeek to avoid channel pop/push-back).
	Request struct {
		priority    *float64
		enqueuedAtNs int64
		result      chan error
		signaled    atomic.Bool
		outcome     error
		elem        *list.Element
		// valveID is stored so that cancel can look up the valve.
		valveID string
	}
)

// priorityUndroppable is a sentinel value indicating a request that must never
// be dropped by CoDel. We use negative infinity so it's distinguishable from
// any real priority value.
var priorityUndroppable = math.Inf(-1)

func isUndroppable(priority *float64) bool {
	return priority != nil && *priority == priorityUndroppable
}

func newUndroppablePriority() *float64 {
	v := priorityUndroppable
	return &v
}

func newRequest(priority *float64) *Request {
	return &Request{
		priority: priority,
		result:   make(chan error, 1),
	}
}

func (r *Request) isDroppable() bool {
	return !isUndroppable(r.priority)
}

// signal writes the outcome to both the inspectable field and the blocking
// channel. It must be called at most once per request.
func (r *Request) signal(err error) {
	r.outcome = err
	r.signaled.Store(true)
	r.result <- err
}

// isDone reports whether the request has been signaled.
func (r *Request) isDone() bool {
	return r.signaled.Load()
}

// NewPriority returns a pointer to the given float64, for use as a droppable
// priority value.
func NewPriority(v float64) *float64 { //nolint:modernize
	return &v
}
