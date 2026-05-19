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
	// All methods are prefixed locked* and assume the caller holds the parent
	// mutex.
	SelfContentionAwareCoDelQueue struct {
		codelq *CoDelQueue

		// pendingRequests is the per-valve-ID queue. Requests here are
		// waiting for the active request to complete before entering the CoDel
		// queue.
		pendingRequests map[string][]*Request

		// outstandingCounts tracks the total number of outstanding requests
		// per valve ID (in CoDel queue + in valve).
		outstandingCounts map[string]int

		// activePerValve tracks which request is currently in the CoDel queue
		// for each valve ID, enabling O(1) lookup on dequeue.
		activePerValve map[string]*Request
	}
)

func newSelfContentionAwareCoDelQueue(cfg CoDelConfig, now func() int64, scheduleDropTimer func(delayNs int64)) *SelfContentionAwareCoDelQueue {
	q := &SelfContentionAwareCoDelQueue{
		pendingRequests:   make(map[string][]*Request),
		outstandingCounts: make(map[string]int),
		activePerValve:    make(map[string]*Request),
	}
	q.codelq = newCoDelQueue(cfg, now, scheduleDropTimer, q.onPeekCleanup)
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

// lockedEnqueue enqueues a request. If the valve ID already has an active
// request in the CoDel queue, the new request is placed in the valve instead.
func (q *SelfContentionAwareCoDelQueue) lockedEnqueue(valveID string, priority *float64) *Request {
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

// lockedDequeue dequeues the head request from the CoDel queue. After removal,
// promotes the next pending request for the same valve ID from the valve.
func (q *SelfContentionAwareCoDelQueue) lockedDequeue() *Request {
	req := q.codelq.lockedDequeue()
	if req == nil {
		return nil
	}
	q.decrementOutstanding(req.valveID)
	q.lockedClearActiveAndPromote(req.valveID)
	return req
}

// lockedDropActive drops the active request for a valve ID (called by the
// CoDel drop timer). Promotes the next pending request from the valve.
func (q *SelfContentionAwareCoDelQueue) lockedDropActive(req *Request) {
	q.codelq.lockedRemove(req)
	if !req.isDone() {
		req.signal(&DroppedRequestError{})
	}
	q.lockedPromoteOnEvict(req)
}

// lockedCancel cancels a request. If it's the active request in the CoDel
// queue, it removes it and promotes the next from the valve. If it's pending
// in the valve, it removes it from there.
func (q *SelfContentionAwareCoDelQueue) lockedCancel(req *Request) {
	if req.elem != nil {
		q.codelq.lockedRemove(req)
		q.lockedPromoteOnEvict(req)
		return
	}
	q.removeFromValve(req.valveID, req)
	q.decrementOutstanding(req.valveID)
}

// lockedRunScheduledDrop runs the CoDel drop logic, finding and dropping the
// lowest-priority request and handling valve promotion.
func (q *SelfContentionAwareCoDelQueue) lockedRunScheduledDrop() {
	dropFn := func() bool {
		elem := q.codelq.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		req := elem.Value.(*Request)
		q.lockedDropActive(req)
		return true
	}
	q.codelq.lockedRunScheduledDrop(dropFn)
}

// lockedMarkNotDroppable forwards to the CoDel queue.
func (q *SelfContentionAwareCoDelQueue) lockedMarkNotDroppable(r *Request) {
	q.codelq.lockedMarkNotDroppable(r)
}

// --- private helpers ---

func (q *SelfContentionAwareCoDelQueue) lockedEnqueueToCoDel(req *Request, valveID string) {
	if valveID != "" {
		q.activePerValve[valveID] = req
	}
	q.codelq.lockedEnqueueRequest(req)
}

// lockedPromoteOnEvict handles involuntary removal of the active request
// (drop or cancel). Decrements outstanding, then clears and promotes.
func (q *SelfContentionAwareCoDelQueue) lockedPromoteOnEvict(req *Request) {
	valveID := req.valveID
	if valveID == "" {
		return
	}
	q.decrementOutstanding(valveID)
	q.lockedClearActiveAndPromote(valveID)
}

// lockedClearActiveAndPromote removes the active tracking for a valve ID
// and promotes the next pending request from the valve into the CoDel queue.
// Empty valve ID is a no-op: it means the caller didn't provide one — requests
// without a valve ID bypass the valve entirely rather than sharing one.
func (q *SelfContentionAwareCoDelQueue) lockedClearActiveAndPromote(valveID string) {
	if valveID == "" {
		return
	}
	delete(q.activePerValve, valveID)
	q.lockedPromote(valveID)
}

func (q *SelfContentionAwareCoDelQueue) lockedPromote(valveID string) {
	q.clearDone(valveID)

	pending, ok := q.pendingRequests[valveID]
	if !ok || len(pending) == 0 {
		return
	}

	next := pending[0]
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

	for len(pending) > 0 && pending[0].isDone() {
		pending = pending[1:]
		q.decrementOutstanding(valveID)
	}

	if len(pending) == 0 {
		delete(q.pendingRequests, valveID)
	} else {
		q.pendingRequests[valveID] = pending
	}
}

func (q *SelfContentionAwareCoDelQueue) removeFromValve(valveID string, r *Request) {
	pending, ok := q.pendingRequests[valveID]
	if !ok {
		return
	}
	for i, p := range pending {
		if p == r {
			q.pendingRequests[valveID] = append(pending[:i], pending[i+1:]...)
			break
		}
	}
	if len(q.pendingRequests[valveID]) == 0 {
		delete(q.pendingRequests, valveID)
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
