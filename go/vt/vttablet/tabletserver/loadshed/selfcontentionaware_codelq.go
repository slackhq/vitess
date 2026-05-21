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

type (
	// SelfContentionAwareCoDelQueue wraps a CoDelQueue with self-contention awareness.
	// For a given valve ID, only one request is in the CoDel queue at a
	// time. Additional requests wait in per-ID "valve" queues and are promoted
	// when the active request completes (dequeue, drop, or cancel).
	//
	// Context
	//
	// Application code may issue multiple lock acquires in parallel. One
	// example is a fan-out (e.g. errgroup) at a high level in the request
	// handler, written without cognizance of lower-level lock acquires for a
	// shared resource needed by each goroutine (e.g. a database connection).
	// Self-contention is artificial contention and doesn't represent true
	// contention on the resource.
	//
	// That case has two downsides:
	//   1. The context can become un-serviceable even in absence of other
	//      clients of the lock. It self-contends and can push the queue into
	//      a dropping state.
	//   2. At best, it floods the queue with lock acquire requests that inflate
	//      sojourn time for other clients of the lock (e.g. other requests).
	//
	// Loadshedding is the right call when genuinely overloaded, but here the
	// load is self-inflicted and also not actual load since the gathered
	// goroutines will end up executing serially anyway, as if they had been
	// written in a for-loop with no fan-out at all.
	//
	// Design
	//
	// We control queue entry with valves so that the queue can only contain
	// one droppable request from a given valve ID at a time. The system
	// supports an arbitrary notion of "valve ID", typically a request ID or
	// job execution instance ID.
	//
	// Invariant: each nonempty valve always has exactly one droppable entry
	// in the CoDel queue. A valve may also have one undroppable (granted)
	// entry — but the droppable entry is always present so CoDel can measure
	// sojourn time and shed load when necessary.
	//
	// This approach has a few benefits:
	//   1. Pushes successive requests back to the end of the queue, which is
	//      fairer to other requests.
	//   2. No preferential treatment around dropping when the queue is under
	//      contention.
	//   3. Allows the queue to empty (assuming no other requests) between
	//      enqueues, thus preventing drops due to self-contention.
	//
	// A valve is a FIFO queue of pending CoDel queue insertions, one per
	// valve ID. An entry cannot exist in both the valve and the CoDel queue
	// simultaneously. When attempting to acquire the lock, the request goes
	// directly into the CoDel queue if there are no entries there for the
	// valve ID. Otherwise, it is inserted into the valve.
	//
	// When the droppable slot for a valve ID is freed (grant, drop, cancel,
	// or release), the next pending request is promoted into the CoDel queue.
	// All promotion runs under the parent mutex, so there is no race between
	// removal and promotion.
	//
	// We are protected against valves entering an unpromotable state because
	// entries are only inserted there if there is already an entry for that
	// valve ID in the CoDel queue, and that entry will trigger promotion on
	// exit.
	//
	// All methods are prefixed locked* and assume the caller holds the parent
	// mutex.
	SelfContentionAwareCoDelQueue struct {
		codelq *CoDelQueue

		// pendingRequests is the per-valve-ID queue. Requests here are
		// waiting for the active request to complete before entering the CoDel
		// queue. There may be entries with the same valve ID in the CoDel queue.
		pendingRequests map[string][]*Request

		// outstandingCounts tracks the total number of outstanding requests
		// per valve ID (in CoDel queue + in valve). We need this separate
		// from len(pendingRequests[valveID]) because pendingRequests does not
		// include the active request in the CoDel queue — without the count
		// we couldn't tell whether to valve a new arrival.
		outstandingCounts map[string]int

		// droppablePerValve tracks which request is the current droppable
		// representative in the CoDel queue for each valve ID. Maintains the
		// invariant that each nonempty valve always has exactly one droppable
		// entry in the CoDel queue.
		droppablePerValve map[string]*Request
	}
)

func newSelfContentionAwareCoDelQueue(cfg CoDelConfig, nowNs func() int64, scheduleDropTimer func(delayNs int64), stopDropTimer func()) *SelfContentionAwareCoDelQueue {
	q := &SelfContentionAwareCoDelQueue{
		pendingRequests:   make(map[string][]*Request),
		outstandingCounts: make(map[string]int),
		droppablePerValve: make(map[string]*Request),
	}
	q.codelq = newCoDelQueue(cfg, nowNs, scheduleDropTimer, stopDropTimer, q.onPeekCleanup)
	return q
}

// onPeekCleanup is called by the CoDel queue when lockedPeek defensively
// removes a done-with-error request from the list head. Decrements the
// outstanding count for the request's valve ID.
func (q *SelfContentionAwareCoDelQueue) onPeekCleanup(req *Request) {
	q.decrementOutstanding(req.valveID)
}

// lockedLen returns the number of requests in the CoDel queue.
func (q *SelfContentionAwareCoDelQueue) lockedLen() int {
	return q.codelq.lockedLen()
}

// lockedIsHealthy reports whether the CoDel queue is healthy.
func (q *SelfContentionAwareCoDelQueue) lockedIsHealthy() bool {
	return q.codelq.lockedIsHealthy()
}

// lockedPeek returns the head of the CoDel queue without removing it.
func (q *SelfContentionAwareCoDelQueue) lockedPeek() *Request {
	return q.codelq.lockedPeek()
}

func (q *SelfContentionAwareCoDelQueue) lockedEnqueue(valveID string, priority float64) *Request {
	req := newRequest(priority)
	req.valveID = valveID

	if valveID != "" {
		q.outstandingCounts[valveID]++
		if q.outstandingCounts[valveID] > 1 {
			q.pendingRequests[valveID] = append(q.pendingRequests[valveID], req)
			return req
		}
	}

	q.lockedEnqueueToCoDel(req, valveID)
	return req
}

// lockedComplete removes a granted (undroppable) request from the queue on
// Release. Decrements outstanding counts for the valve ID. Promotes the next
// valve entry if this was the last active entry for the valve ID (i.e., the
// eager promotion at grant time found nothing to promote, but requests arrived
// in the valve between grant and release).
func (q *SelfContentionAwareCoDelQueue) lockedComplete(req *Request) {
	q.codelq.lockedComplete(req)
	q.decrementOutstanding(req.valveID)
	if req.valveID != "" {
		// Promote if no droppable entry exists. This handles the case where
		// the valve was empty at grant time (nothing to promote), but new
		// requests arrived between grant and release — they're stranded in
		// the valve with no droppable representative to trigger promotion.
		if _, has := q.droppablePerValve[req.valveID]; !has {
			q.lockedPromote(req.valveID)
		}
	}
}

func (q *SelfContentionAwareCoDelQueue) lockedDrop(req *Request) {
	q.codelq.lockedRemove(req)
	if req.signaledValue == nil {
		req.signal(&DroppedRequestError{})
	}
	q.lockedPromoteOnEvict(req)
}

// lockedCancel cancels a request. If it's in the CoDel queue, it removes it
// and promotes the next from the valve. If it's pending in the valve, it
// signals it in place and lets clearDone handle removal during the next
// promotion — this avoids an O(N) scan of the valve.
func (q *SelfContentionAwareCoDelQueue) lockedCancel(req *Request) {
	if req.codelqElem != nil {
		q.codelq.lockedRemove(req)
		q.lockedPromoteOnEvict(req)
		return
	}
	// The request may already have been signaled if it was promoted into
	// the CoDel queue and dropped between the caller's default-branch
	// (signalChan empty) and mutex acquisition.
	if req.signaledValue == nil {
		req.signal(&DroppedRequestError{})
	}
}

// lockedRunScheduledDrop runs the CoDel drop logic, finding and dropping the
// lowest-priority request and triggering valve promotion.
func (q *SelfContentionAwareCoDelQueue) lockedRunScheduledDrop() {
	dropFn := func() bool {
		elem := q.codelq.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		req := elem.Value.(*Request)
		q.lockedDrop(req)
		return true
	}
	q.codelq.lockedRunScheduledDrop(dropFn)
}

func (q *SelfContentionAwareCoDelQueue) lockedOnGrant(r *Request) {
	q.codelq.lockedOnGrant(r)
	if r.valveID != "" {
		delete(q.droppablePerValve, r.valveID)
		q.lockedPromote(r.valveID)
	}
}

func (q *SelfContentionAwareCoDelQueue) lockedFirstWaiting() *Request {
	return q.codelq.lockedFirstWaiting()
}

// --- private helpers ---

func (q *SelfContentionAwareCoDelQueue) lockedEnqueueToCoDel(req *Request, valveID string) {
	if valveID != "" {
		q.droppablePerValve[valveID] = req
	}
	q.codelq.lockedEnqueue(req)
}

// lockedPromoteOnEvict handles involuntary removal of the active request
// (drop or cancel). Decrements outstanding, then promotes.
func (q *SelfContentionAwareCoDelQueue) lockedPromoteOnEvict(req *Request) {
	valveID := req.valveID
	if valveID == "" {
		return
	}
	q.decrementOutstanding(valveID)
	delete(q.droppablePerValve, valveID)
	q.lockedPromote(valveID)
}

func (q *SelfContentionAwareCoDelQueue) lockedPromote(valveID string) {
	if valveID == "" {
		return
	}

	q.clearDone(valveID)

	pending, ok := q.pendingRequests[valveID]
	if !ok || len(pending) == 0 {
		return
	}

	next := pending[0]
	pending[0] = nil
	s := pending[1:]
	if len(s) == 0 {
		delete(q.pendingRequests, valveID)
	} else {
		q.pendingRequests[valveID] = s
	}

	q.lockedEnqueueToCoDel(next, valveID)
}

// clearDone removes done (cancelled) requests from the head of the valve.
// Their outstanding counts are decremented since the CoDel queue never learned
// about them.
func (q *SelfContentionAwareCoDelQueue) clearDone(valveID string) {
	pending, ok := q.pendingRequests[valveID]
	if !ok {
		return
	}

	for len(pending) > 0 && pending[0].signaledValue != nil {
		pending[0] = nil
		pending = pending[1:]
		q.decrementOutstanding(valveID)
	}

	if len(pending) == 0 {
		delete(q.pendingRequests, valveID)
	} else {
		q.pendingRequests[valveID] = pending
	}
}

func (q *SelfContentionAwareCoDelQueue) decrementOutstanding(valveID string) {
	if valveID == "" {
		return
	}
	q.outstandingCounts[valveID]--
	if q.outstandingCounts[valveID] <= 0 {
		delete(q.outstandingCounts, valveID)
	}
}
