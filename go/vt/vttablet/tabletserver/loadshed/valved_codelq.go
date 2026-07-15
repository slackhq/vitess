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
	// ValvedCoDelQueue wraps a CoDelQueue with self-contention awareness.
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
	ValvedCoDelQueue struct {
		codelq *CoDelQueue

		// valves is the per-valve-ID queue. Requests here are waiting for the
		// active request to complete before entering the CoDel queue. There may
		// be entries with the same valve ID in the CoDel queue.
		valves map[string][]*Request

		// outstandingCounts tracks the total number of outstanding requests per
		// valve ID (in CoDel queue + in valve). Note that there may be multiple
		// requests for any one valve in the CoDel queue (one droppable and
		// one-or-more granted).
		outstandingCounts map[string]int

		// droppablePerValve tracks which request is the current droppable
		// representative in the CoDel queue for each valve ID. Maintains the
		// invariant that each nonempty valve always has exactly one droppable
		// entry in the CoDel queue.
		droppablePerValve map[string]*Request

		// droppedTotal is a monotonic count of requests actually shed (via
		// lockedDrop). Snake reads it as a delta around a drop pass to attribute
		// drops to system state (e.g. whether a slot was free). It counts real
		// drops only, so valve promotion re-enqueuing a droppable successor during
		// the same pass does not mask it — a droppableLen delta would.
		droppedTotal int64

		// pendingSignals collects requests that lockedDrop marked (signaledValue
		// set) but whose channel send is deferred until the queue mutex is
		// released. Draining the goready storm outside the lock keeps grants and
		// arrivals from serializing behind a large batch drop. The caller takes
		// this slice before unlocking and sends each afterward (see
		// lockedTakePendingSignals).
		pendingSignals []*Request
	}
)

func newValvedCoDelQueue(cfg CoDelConfig, nowNs func() int64, scheduleDropTimer func(delayNs int64), stopDropTimer func()) *ValvedCoDelQueue {
	q := &ValvedCoDelQueue{
		valves:            make(map[string][]*Request),
		outstandingCounts: make(map[string]int),
		droppablePerValve: make(map[string]*Request),
	}
	q.codelq = newCoDelQueue(cfg, nowNs, scheduleDropTimer, stopDropTimer, q.onPeekCleanup)
	return q
}

// onPeekCleanup is called by the CoDel queue when lockedPeek defensively
// removes a done-with-error request from the list head. Decrements the
// outstanding count for the request's valve ID.
func (q *ValvedCoDelQueue) onPeekCleanup(req *Request) {
	q.decrementOutstanding(req.valveID)
}

func (q *ValvedCoDelQueue) lockedCurrentInterval() int64 {
	return q.codelq.lockedCurrentInterval()
}

func (q *ValvedCoDelQueue) lockedDroppableLen() int {
	return q.codelq.droppableLen
}

func (q *ValvedCoDelQueue) lockedCount() int {
	return q.codelq.count
}

// lockedLastDropOvershootNs returns how late (now - dropNextNs) the most recent
// due-drop pass was serviced, on either the timer or the synchronous dequeue
// path. Zero if the last pass had no drop due.
func (q *ValvedCoDelQueue) lockedLastDropOvershootNs() int64 {
	return q.codelq.lastDropOvershootNs
}

// lockedLastDropsPerFire returns how many requests the most recent control-law
// advance shed — the size of the shed burst under one lock acquisition. Zero if
// it dropped nothing.
func (q *ValvedCoDelQueue) lockedLastDropsPerFire() int {
	return q.codelq.lastDropsPerFire
}

// lockedLen returns the number of requests in the CoDel queue.
func (q *ValvedCoDelQueue) lockedLen() int {
	return q.codelq.lockedLen()
}

func (q *ValvedCoDelQueue) lockedValveDepth(valveID string) int {
	return len(q.valves[valveID])
}

// lockedIsHealthy reports whether the CoDel queue is healthy.
func (q *ValvedCoDelQueue) lockedIsHealthy() bool {
	return q.codelq.lockedIsHealthy()
}

// lockedNeedsAdvance reports whether the dequeue path has any CoDel work to do:
// an episode is active or armed (dropping, or dropNextNs seeded), the count is
// still easing down, or a droppable backlog exists that a head-sojourn trigger
// could arm on. When false, lockedDequeue is a guaranteed no-op, so the release
// path can skip the call — and its clock read — entirely on the healthy fast
// path.
func (q *ValvedCoDelQueue) lockedNeedsAdvance() bool {
	return q.codelq.dropping || q.codelq.dropNextNs != 0 || q.codelq.droppableLen > 0
}

// lockedPeek returns the head of the CoDel queue without removing it.
func (q *ValvedCoDelQueue) lockedPeek() *Request {
	return q.codelq.lockedPeek()
}

func (q *ValvedCoDelQueue) lockedEnqueue(valveID string, priority float64) *Request {
	req := newRequest(priority)
	req.valveID = valveID

	if valveID != "" {
		q.outstandingCounts[valveID]++
		if q.droppablePerValve[valveID] != nil {
			q.valves[valveID] = append(q.valves[valveID], req)
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
func (q *ValvedCoDelQueue) lockedComplete(req *Request) {
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

func (q *ValvedCoDelQueue) lockedDrop(req *Request) {
	q.droppedTotal++
	q.codelq.lockedRemove(req)
	// Mark the rejection under the lock (so under-lock readers see signaledValue
	// immediately) but defer the channel send: it goreadys the parked Acquire
	// goroutine, and doing that in a batch under s.mu is what serializes grants
	// behind the drop storm. The caller drains pendingSignals after unlocking.
	if req.signaledValue == nil {
		req.markSignaled(&DroppedRequestError{})
		q.pendingSignals = append(q.pendingSignals, req)
	}
	q.lockedPromoteOnEvict(req)
}

// lockedTakePendingSignals hands off the requests marked-but-not-yet-sent by the
// drop path, clearing the queue's reference. The caller must send each
// (req.sendSignal()) AFTER releasing the queue mutex. Ownership transfers fully:
// the buffer is set to nil (not truncated in place) so a concurrent drop pass
// that re-appends under the lock cannot corrupt the slice the caller is draining
// after unlocking.
func (q *ValvedCoDelQueue) lockedTakePendingSignals() []*Request {
	pending := q.pendingSignals
	q.pendingSignals = nil
	return pending
}

// lockedDroppedTotal returns the monotonic count of requests actually shed.
func (q *ValvedCoDelQueue) lockedDroppedTotal() int64 {
	return q.droppedTotal
}

// lockedCancel cancels a request. If it's in the CoDel queue, it removes it
// and promotes the next from the valve. If it's pending in the valve, it
// signals it in place and lets clearDone handle removal during the next
// promotion — this avoids an O(N) scan of the valve.
func (q *ValvedCoDelQueue) lockedCancel(req *Request) {
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

// lockedDropFn builds the drop callback shared by the timer and the synchronous
// advance path: drop the lowest-priority droppable request and promote its
// valve successor.
func (q *ValvedCoDelQueue) lockedDropFn() func() bool {
	return func() bool {
		// Keep-droppable floor: refuse to drop while the droppable backlog is at or
		// below the floor, keeping a reserve of warm requests so freeing slots have
		// something to grant instead of underfilling the semaphore.
		if floor := q.codelq.keepDroppableFloor(); floor > 0 && q.codelq.droppableLen <= floor {
			return false
		}
		elem := q.codelq.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		req := elem.Value.(*Request)
		q.lockedDrop(req)
		return true
	}
}

// lockedRunTimer runs the CoDel drop logic, finding and dropping the
// lowest-priority request and triggering valve promotion. It is driven both by
// the backstop timer and synchronously from the release/dequeue path, so
// shedding tracks target as slots free rather than waiting for the timer.
func (q *ValvedCoDelQueue) lockedRunTimer() {
	q.codelq.lockedRunTimer(q.lockedDropFn())
}

func (q *ValvedCoDelQueue) lockedOnGrant(r *Request) {
	q.codelq.lockedOnGrant(r)
	if r.valveID != "" {
		delete(q.droppablePerValve, r.valveID)
		q.lockedPromote(r.valveID)
	}
}

func (q *ValvedCoDelQueue) lockedFirstWaiting() *Request {
	return q.codelq.lockedFirstWaiting()
}

// --- private helpers ---

func (q *ValvedCoDelQueue) lockedEnqueueToCoDel(req *Request, valveID string) {
	if valveID != "" {
		q.droppablePerValve[valveID] = req
	}
	q.codelq.lockedEnqueue(req)
}

// lockedAdmitToCoDel is lockedEnqueueToCoDel for the intake merge path: it
// preserves req.codelqEnqueuedAtNs (its original arrival time) instead of
// restamping now.
func (q *ValvedCoDelQueue) lockedAdmitToCoDel(req *Request, valveID string) {
	if valveID != "" {
		q.droppablePerValve[valveID] = req
	}
	q.codelq.lockedAdmit(req)
}

// lockedMergeExisting admits an already-built request (valveID, priority, and
// codelqEnqueuedAtNs already set — e.g. from the per-CPU intake) through the
// valve fairness layer, preserving its arrival time. Mirrors lockedEnqueue but
// takes the request as-is rather than creating one. Returns true if the request
// entered the CoDel queue now, false if it was parked behind an existing
// droppable entry for its valve (it will be promoted later, exactly as a
// normal enqueue would be).
func (q *ValvedCoDelQueue) lockedMergeExisting(req *Request) bool {
	valveID := req.valveID
	if valveID != "" {
		q.outstandingCounts[valveID]++
		if q.droppablePerValve[valveID] != nil {
			q.valves[valveID] = append(q.valves[valveID], req)
			return false
		}
	}
	q.lockedAdmitToCoDel(req, valveID)
	return true
}

// lockedPromoteOnEvict handles involuntary removal of the active request
// (drop or cancel). Decrements outstanding, then promotes.
func (q *ValvedCoDelQueue) lockedPromoteOnEvict(req *Request) {
	valveID := req.valveID
	if valveID == "" {
		return
	}
	q.decrementOutstanding(valveID)
	delete(q.droppablePerValve, valveID)
	q.lockedPromote(valveID)
}

func (q *ValvedCoDelQueue) lockedPromote(valveID string) {
	if valveID == "" {
		return
	}

	q.clearDone(valveID)

	pending, ok := q.valves[valveID]
	if !ok || len(pending) == 0 {
		return
	}

	next := pending[0]
	pending[0] = nil
	s := pending[1:]
	if len(s) == 0 {
		delete(q.valves, valveID)
	} else {
		q.valves[valveID] = s
	}

	q.lockedEnqueueToCoDel(next, valveID)
}

// clearDone removes done (cancelled) requests from the head of the valve.
// Their outstanding counts are decremented since the CoDel queue never learned
// about them.
func (q *ValvedCoDelQueue) clearDone(valveID string) {
	pending, ok := q.valves[valveID]
	if !ok {
		return
	}

	for len(pending) > 0 && pending[0].signaledValue != nil {
		pending[0] = nil
		pending = pending[1:]
		q.decrementOutstanding(valveID)
	}

	if len(pending) == 0 {
		delete(q.valves, valveID)
	} else {
		q.valves[valveID] = pending
	}
}

func (q *ValvedCoDelQueue) decrementOutstanding(valveID string) {
	if valveID == "" {
		return
	}
	q.outstandingCounts[valveID]--
	if q.outstandingCounts[valveID] <= 0 {
		delete(q.outstandingCounts, valveID)
	}
}
