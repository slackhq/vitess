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
	// in the CoDel queue. The droppable entry is always present so CoDel can
	// measure sojourn time and shed load when necessary.
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
	ValvedCoDelQueue[T any] struct {
		codelq *CoDelQueue[T]

		// valves is the per-valve-ID queue. Requests here are waiting for the
		// active request to complete before entering the CoDel queue. There may
		// be entries with the same valve ID in the CoDel queue.
		valves map[string][]*Request[T]

		// outstandingCounts tracks the total number of outstanding requests per
		// valve ID (in CoDel queue + in valve).
		outstandingCounts map[string]int

		// droppablePerValve tracks which request is the current droppable
		// representative in the CoDel queue for each valve ID. Maintains the
		// invariant that each nonempty valve always has exactly one droppable
		// entry in the CoDel queue.
		droppablePerValve map[string]*Request[T]

		// pendingSignals collects requests that lockedDrop marked (signaledValue
		// set) but whose channel send is deferred until the queue mutex is
		// released. Draining the goready storm outside the lock keeps grants and
		// arrivals from serializing behind a large batch drop. The caller takes
		// this slice before unlocking and sends each afterward (see
		// lockedTakePendingSignals).
		pendingSignals []*Request[T]
	}
)

func newValvedCoDelQueue[T any](cfg CoDelConfig, nowNs func() int64, scheduleDropTimer func(delayNs int64), stopDropTimer func()) *ValvedCoDelQueue[T] {
	q := &ValvedCoDelQueue[T]{
		valves:            make(map[string][]*Request[T]),
		outstandingCounts: make(map[string]int),
		droppablePerValve: make(map[string]*Request[T]),
	}
	q.codelq = newCoDelQueue(cfg, nowNs, scheduleDropTimer, stopDropTimer, q.onPeekCleanup)
	return q
}

// onPeekCleanup is called by the CoDel queue when lockedPeek defensively
// removes a done-with-error request from the list head. Decrements the
// outstanding count for the request's valve ID.
func (q *ValvedCoDelQueue[T]) onPeekCleanup(req *Request[T]) {
	q.decrementOutstanding(req.valveID)
}

func (q *ValvedCoDelQueue[T]) lockedCurrentInterval() int64 {
	return q.codelq.lockedCurrentInterval()
}

func (q *ValvedCoDelQueue[T]) lockedDroppableLen() int {
	return q.codelq.droppableLen
}

func (q *ValvedCoDelQueue[T]) lockedCount() int {
	return q.codelq.count
}

// lockedLen returns the number of requests in the CoDel queue.
func (q *ValvedCoDelQueue[T]) lockedLen() int {
	return q.codelq.lockedLen()
}

func (q *ValvedCoDelQueue[T]) lockedValveDepth(valveID string) int {
	return len(q.valves[valveID])
}

// lockedIsHealthy reports whether the CoDel queue is healthy.
func (q *ValvedCoDelQueue[T]) lockedIsHealthy() bool {
	return q.codelq.lockedIsHealthy()
}

// lockedNeedsAdvance reports whether the dequeue path has any CoDel work to do:
// an episode is active or armed (dropping, or dropNextNs seeded), the count is
// still easing down, or a droppable backlog exists that a head-sojourn trigger
// could arm on. When false, lockedDequeue is a guaranteed no-op, so the release
// path can skip the call — and its clock read — entirely on the healthy fast
// path.
func (q *ValvedCoDelQueue[T]) lockedNeedsAdvance() bool {
	return q.codelq.dropping || q.codelq.dropNextNs != 0 || q.codelq.droppableLen > 0
}

// lockedPeek returns the head of the CoDel queue without removing it.
func (q *ValvedCoDelQueue[T]) lockedPeek() *Request[T] {
	return q.codelq.lockedPeek()
}

func (q *ValvedCoDelQueue[T]) lockedEnqueue(valveID string, priority float64) *Request[T] {
	req := newRequest[T](priority)
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

// lockedRelease updates valve accounting for a granted request and promotes a
// pending request when needed.
func (q *ValvedCoDelQueue[T]) lockedRelease(req *Request[T]) {
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

func (q *ValvedCoDelQueue[T]) lockedDrop(req *Request[T]) {
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
func (q *ValvedCoDelQueue[T]) lockedTakePendingSignals() []*Request[T] {
	pending := q.pendingSignals
	q.pendingSignals = nil
	return pending
}

// lockedCancel cancels a request. If it's in the CoDel queue, it removes it
// and promotes the next from the valve. If it's pending in the valve, it
// signals it in place and lets clearDone handle removal during the next
// promotion — this avoids an O(N) scan of the valve.
func (q *ValvedCoDelQueue[T]) lockedCancel(req *Request[T]) {
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

// keepDroppableFloor is the number of droppable requests kept as a reserve
// before shedding begins: while the droppable backlog is at or below this floor,
// drops are refused. This improves prioritization — it keeps a pool of droppable
// requests on hand so the lowest-priority ones are available to shed first,
// rather than the queue draining to a shallow set that forces CoDel to drop
// whatever is present (including higher-priority requests). The guard only
// engages in the queue's near-empty troughs — under a real backlog
// (droppableLen > floor) shedding is unchanged.
const keepDroppableFloor = 4

// lockedDropFn builds the drop callback shared by the timer and the synchronous
// advance path: drop the lowest-priority droppable request and promote its
// valve successor.
func (q *ValvedCoDelQueue[T]) lockedDropFn() func() bool {
	return func() bool {
		return q.lockedDropOne()
	}
}

// lockedRunTimer runs the CoDel drop logic, finding and dropping the
// lowest-priority request and triggering valve promotion. It is driven both by
// the backstop timer and synchronously from the release/dequeue path, so
// shedding tracks target as slots free rather than waiting for the timer.
func (q *ValvedCoDelQueue[T]) lockedRunTimer() {
	q.lockedRunTimerIf(func() bool { return true })
}

func (q *ValvedCoDelQueue[T]) lockedRunTimerIf(loadsheddingAllowed func() bool) {
	enabled := loadsheddingAllowed()
	if enabled {
		q.codelq.lockedRunTimer(q.lockedDropFn())
		return
	}
	maxDrops := max(q.codelq.droppableLen-keepDroppableFloor, 0)
	q.codelq.lockedRunTimerLimited(func() bool {
		return q.codelq.lockedFindLowestPriorityDroppable() != nil
	}, maxDrops)
}

func (q *ValvedCoDelQueue[T]) lockedDropOne() bool {
	if q.codelq.droppableLen <= keepDroppableFloor {
		return false
	}
	elem := q.codelq.lockedFindLowestPriorityDroppable()
	if elem == nil {
		return false
	}
	q.lockedDrop(elem.Value.(*Request[T]))
	return true
}

func (q *ValvedCoDelQueue[T]) lockedOnGrant(r *Request[T]) {
	q.codelq.lockedOnGrant(r)
	if r.valveID != "" {
		delete(q.droppablePerValve, r.valveID)
		q.lockedPromote(r.valveID)
	}
}

func (q *ValvedCoDelQueue[T]) lockedFirstWaiting() *Request[T] {
	return q.codelq.lockedFirstWaiting()
}

// --- private helpers ---

func (q *ValvedCoDelQueue[T]) lockedEnqueueToCoDel(req *Request[T], valveID string) {
	if valveID != "" {
		q.droppablePerValve[valveID] = req
	}
	q.codelq.lockedEnqueue(req)
}

// lockedPromoteOnEvict handles involuntary removal of the active request
// (drop or cancel). Decrements outstanding, then promotes.
func (q *ValvedCoDelQueue[T]) lockedPromoteOnEvict(req *Request[T]) {
	valveID := req.valveID
	if valveID == "" {
		return
	}
	q.decrementOutstanding(valveID)
	delete(q.droppablePerValve, valveID)
	q.lockedPromote(valveID)
}

func (q *ValvedCoDelQueue[T]) lockedPromote(valveID string) {
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
func (q *ValvedCoDelQueue[T]) clearDone(valveID string) {
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

func (q *ValvedCoDelQueue[T]) decrementOutstanding(valveID string) {
	if valveID == "" {
		return
	}
	q.outstandingCounts[valveID]--
	if q.outstandingCounts[valveID] <= 0 {
		delete(q.outstandingCounts, valveID)
	}
}
