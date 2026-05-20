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
	// Request represents an entry in the CoDel queue. Named a 'request' since
	// it may be rejected(dropped) or granted. Each request owns a signalChan
	// that receives nil on grant or a *DroppedRequestError on drop. The
	// signaled flag and signaledValue field allow non-consuming inspection of the
	// signaledValue (used by lockedPeek to avoid channel pop/push-back).
	Request struct {
		priority      *float64
		enqueuedAtNs  int64
		signalChan    chan error
		signaled      atomic.Bool
		signaledValue error
		elem          *list.Element
		// Needed so that cancel can look up the valve
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
		priority:   priority,
		signalChan: make(chan error, 1),
	}
}

func (r *Request) isDroppable() bool {
	return !isUndroppable(r.priority)
}

// Pass nil on grant and *DroppedRequestError on drop. It must be called exactly
// once per request.
func (r *Request) signal(err error) {
	if !r.signaled.CompareAndSwap(false, true) {
		panic("loadshed: signal called more than once")
	}
	r.signaledValue = err
	r.signalChan <- err
}

func NewPriority(v float64) *float64 { //nolint:modernize
	return &v
}
