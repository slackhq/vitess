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
	"sync/atomic"
)

type (
	// Request represents an entry in the CoDel queue. It may be granted (lock
	// acquired) or dropped (load shed). Each request owns a done channel that
	// receives nil on grant or a *DroppedRequestError on drop. The signaled
	// flag and result field allow non-consuming inspection of the outcome
	// (used by lockedPeek to avoid channel pop/push-back).
	Request struct {
		priority   *float64
		enqueuedAt int64
		done       chan error
		signaled   atomic.Bool
		result     error
		droppable  bool
		elem       *list.Element
		// contentionID is stored so that cancel can look up the valve.
		contentionID string
	}
)

// PriorityUndroppable indicates a request that must never be dropped by CoDel.
// Pass this as the priority to Lock.Acquire when load-shedding is not allowed.
var PriorityUndroppable *float64 // nil sentinel

func newRequest(priority *float64, enqueuedAt int64) *Request {
	droppable := priority != nil
	return &Request{
		priority:   priority,
		enqueuedAt: enqueuedAt,
		done:       make(chan error, 1),
		droppable:  droppable,
	}
}

// signal writes the result to both the inspectable field and the blocking
// channel. It must be called at most once per request.
func (r *Request) signal(err error) {
	r.result = err
	r.signaled.Store(true)
	r.done <- err
}

// isDone reports whether the request has been signaled.
func (r *Request) isDone() bool {
	return r.signaled.Load()
}

// NewPriority returns a pointer to the given float64, for use as a droppable
// priority value. nil means undroppable.
func NewPriority(v float64) *float64 { //nolint:modernize
	return &v
}
