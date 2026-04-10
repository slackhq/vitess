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

import "container/list"

type (
	// Request represents an entry in the CoDel queue. It may be granted (lock
	// acquired) or dropped (load shed). Each request owns a done channel that
	// receives nil on grant or a *DroppedRequestError on drop.
	Request struct {
		priority   *float64
		enqueuedAt int64
		done       chan error
		droppable  bool
		elem       *list.Element
		// contentionID is stored so that cancel can look up the valve.
		contentionID string
	}
)

func newRequest(priority *float64, enqueuedAt int64) *Request {
	droppable := priority != nil
	return &Request{
		priority:   priority,
		enqueuedAt: enqueuedAt,
		done:       make(chan error, 1),
		droppable:  droppable,
	}
}

// isDone reports whether the request's done channel has been written to.
func (r *Request) isDone() bool {
	return len(r.done) > 0
}

// NewPriority returns a pointer to the given float64, for use as a droppable
// priority value. nil means undroppable.
func NewPriority(v float64) *float64 {
	return &v
}
