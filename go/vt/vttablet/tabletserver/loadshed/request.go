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
	// it may be rejected(dropped) or granted. Each request owns a signalChan
	// that receives nil on grant or a *DroppedRequestError on drop. The
	// signaledValue field allows non-consuming inspection of signal state
	// (used by lockedPeek to avoid channel pop/push-back): nil means
	// unsignaled, grantSentinel means granted, any other value means dropped.
	Request struct {
		priority           *float64
		codelqEnqueuedAtNs int64
		signalChan         chan error
		signaledValue      error
		elem               *list.Element
		// Needed so that cancel can look up the valve
		valveID string
	}
)

// priorityUndroppable is a sentinel value indicating a request that must never
// be dropped by CoDel. We use negative infinity so it's distinguishable from
// any real priority value.
var priorityUndroppable = math.Inf(-1)

var grantSentinel = errors.New("granted") //nolint:staticcheck // not an error; sentinel for non-consuming signal state inspection

func isUndroppable(priority *float64) bool {
	return priority != nil && *priority == priorityUndroppable
}

func newUndroppablePriority() *float64 {
	v := priorityUndroppable
	return &v
}

func newRequest(priority *float64) *Request {
	return &Request{
		priority:   priority,
		signalChan: make(chan error, 1),
	}
}

func (r *Request) isDroppable() bool {
	return !isUndroppable(r.priority)
}

// Pass grantSentinel on grant and *DroppedRequestError on drop. Must be called
// exactly once per request.
func (r *Request) signal(val error) {
	if r.signaledValue != nil {
		panic("loadshed: signal called more than once")
	}
	r.signaledValue = val
	r.signalChan <- val
}

func NewPriority(v float64) *float64 { //nolint:modernize
	return &v
}
