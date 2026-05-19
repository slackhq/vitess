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
	// For a given contention ID, only one request is in the CoDel queue at a
	// time. Additional requests wait in per-ID "valve" queues and are promoted
	// when the active request completes (dequeue, drop, or cancel).
	//
	// All methods are prefixed locked* and assume the caller holds the parent
	// mutex.
	SelfContentionAwareCoDelQueue struct {
		codelq *CoDelQueue

		// pendingRequests is the per-contention-ID valve. Requests here are
		// waiting for the active request to complete before entering the CoDel
		// queue.
		pendingRequests map[string][]*Request

		// outstandingCounts tracks the total number of outstanding requests
		// per contention ID (in CoDel queue + in valve).
		outstandingCounts map[string]int

		// activeRequests tracks which request is currently in the CoDel queue
		// for each contention ID, enabling O(1) lookup on dequeue.
		activeRequests map[string]*Request
	}
)

func newSelfContentionAwareCoDelQueue(cfg CoDelConfig, clockFunc func() int64) *SelfContentionAwareCoDelQueue {
	return &SelfContentionAwareCoDelQueue{
		codelq:            newCoDelQueue(cfg, clockFunc),
		pendingRequests:   make(map[string][]*Request),
		outstandingCounts: make(map[string]int),
		activeRequests:    make(map[string]*Request),
	}
}

// lockedLen returns the number of requests in the CoDel queue.
func (s *SelfContentionAwareCoDelQueue) lockedLen() int {
	return s.codelq.lockedLen()
}

// lockedIsHealthy reports whether the CoDel queue is healthy.
func (s *SelfContentionAwareCoDelQueue) lockedIsHealthy() bool {
	return s.codelq.lockedIsHealthy()
}

// lockedPeek returns the head of the CoDel queue without removing it.
func (s *SelfContentionAwareCoDelQueue) lockedPeek() *Request {
	return s.codelq.lockedPeek()
}

// lockedEnqueue enqueues a request. If the contention ID already has an active
// request in the CoDel queue, the new request is placed in the valve instead.
// Returns the request, whether to schedule a drop timer, and the delay.
func (s *SelfContentionAwareCoDelQueue) lockedEnqueue(contentionID string, priority *float64) (*Request, bool, int64) {
	req := newRequest(priority, s.codelq.clockFunc())
	req.contentionID = contentionID

	if contentionID != "" {
		s.outstandingCounts[contentionID]++
		if s.outstandingCounts[contentionID] > 1 {
			// already have an active request in the CoDel queue for this ID
			s.pendingRequests[contentionID] = append(s.pendingRequests[contentionID], req)
			return req, false, 0
		}
	}

	return s.codelqEnqueue(req, contentionID)
}

// lockedDequeue dequeues the head request from the CoDel queue. After removal,
// promotes the next pending request for the same contention ID from the valve.
// Returns the request, whether the parent should schedule a drop timer, and the
// delay in nanoseconds.
func (s *SelfContentionAwareCoDelQueue) lockedDequeue() (*Request, bool, int64) {
	req, needSchedule, delayNs := s.codelq.lockedDequeue()
	if req == nil {
		return nil, false, 0
	}

	promoteNeedSchedule, promoteDelay := s.onRequestComplete(req)
	if promoteNeedSchedule && !needSchedule {
		needSchedule = true
		delayNs = promoteDelay
	}
	return req, needSchedule, delayNs
}

// lockedDropActive drops the active request for a contention ID (called by the
// CoDel drop timer). Promotes the next pending request from the valve.
func (s *SelfContentionAwareCoDelQueue) lockedDropActive(contentionID string, r *Request) {
	s.codelq.lockedCancel(r)
	if !r.isDone() {
		r.signal(&DroppedRequestError{})
	}
	// Discard schedule signal: called from within lockedRunScheduledDrop which
	// manages its own rescheduling based on droppableLen.
	s.onRequestComplete(r)
}

// lockedCancel cancels a request. If it's the active request in the CoDel
// queue, it removes it and promotes the next from the valve. If it's pending
// in the valve, it removes it from there. Returns whether the parent should
// schedule a drop timer (from valve promotion).
func (s *SelfContentionAwareCoDelQueue) lockedCancel(contentionID string, r *Request) (needSchedule bool, delayNs int64) {
	if r.elem != nil {
		// in the CoDel queue: remove and promote
		s.codelq.lockedCancel(r)
		return s.onRequestComplete(r)
	}
	// in the valve: find and remove
	s.removeFromValve(contentionID, r)
	s.decrementOutstanding(contentionID)
	return false, 0
}

// lockedRunScheduledDrop runs the CoDel drop logic, finding and dropping the
// lowest-priority request and handling valve promotion. Returns whether to
// reschedule and the delay in nanoseconds.
func (s *SelfContentionAwareCoDelQueue) lockedRunScheduledDrop() (bool, int64) {
	dropFn := func() bool {
		elem := s.codelq.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		req := elem.Value.(*Request)
		s.lockedDropActive(req.contentionID, req)
		return true
	}
	return s.codelq.lockedRunScheduledDrop(dropFn)
}

// lockedMarkNotDroppable forwards to the CoDel queue.
func (s *SelfContentionAwareCoDelQueue) lockedMarkNotDroppable(r *Request) {
	s.codelq.lockedMarkNotDroppable(r)
}

// --- private helpers ---

func (s *SelfContentionAwareCoDelQueue) codelqEnqueue(req *Request, contentionID string) (*Request, bool, int64) {
	if contentionID != "" {
		s.activeRequests[contentionID] = req
	}

	return s.codelq.lockedEnqueueRequest(req)
}

func (s *SelfContentionAwareCoDelQueue) onRequestComplete(req *Request) (needSchedule bool, delayNs int64) {
	contentionID := req.contentionID
	if contentionID == "" {
		return false, 0
	}

	delete(s.activeRequests, contentionID)
	s.decrementOutstanding(contentionID)
	return s.lockedPromote(contentionID)
}

func (s *SelfContentionAwareCoDelQueue) lockedPromote(contentionID string) (needSchedule bool, delayNs int64) {
	s.clearDone(contentionID)

	pending, ok := s.pendingRequests[contentionID]
	if !ok || len(pending) == 0 {
		return false, 0
	}

	// pop the first pending request and enqueue it
	next := pending[0]
	s.pendingRequests[contentionID] = pending[1:]
	if len(s.pendingRequests[contentionID]) == 0 {
		delete(s.pendingRequests, contentionID)
	}

	_, needSchedule, delayNs = s.codelqEnqueue(next, contentionID)
	return needSchedule, delayNs
}

// clearDone removes done (cancelled) requests from the head of the valve.
// Their outstanding counts are decremented since the CoDel queue never learned
// about them.
func (s *SelfContentionAwareCoDelQueue) clearDone(contentionID string) {
	pending, ok := s.pendingRequests[contentionID]
	if !ok {
		return
	}

	removed := 0
	for len(pending) > 0 && pending[0].isDone() {
		pending = pending[1:]
		removed++
	}
	for range removed {
		s.decrementOutstanding(contentionID)
	}

	if len(pending) == 0 {
		delete(s.pendingRequests, contentionID)
	} else {
		s.pendingRequests[contentionID] = pending
	}
}

func (s *SelfContentionAwareCoDelQueue) removeFromValve(contentionID string, r *Request) {
	pending, ok := s.pendingRequests[contentionID]
	if !ok {
		return
	}
	for i, p := range pending {
		if p == r {
			s.pendingRequests[contentionID] = append(pending[:i], pending[i+1:]...)
			break
		}
	}
	if len(s.pendingRequests[contentionID]) == 0 {
		delete(s.pendingRequests, contentionID)
	}
}

func (s *SelfContentionAwareCoDelQueue) decrementOutstanding(contentionID string) {
	if contentionID == "" {
		return
	}
	s.outstandingCounts[contentionID]--
	if s.outstandingCounts[contentionID] <= 0 {
		delete(s.outstandingCounts, contentionID)
	}
}
