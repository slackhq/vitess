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
	// SelfAwareCoDelQueue wraps a CoDelQueue with self-contention awareness.
	// For a given contention ID, only one request is in the CoDel queue at a
	// time. Additional requests wait in per-ID "valve" queues and are promoted
	// when the active request completes (dequeue, drop, or cancel).
	//
	// All methods are prefixed locked* and assume the caller holds the parent
	// mutex.
	SelfAwareCoDelQueue struct {
		cq *CoDelQueue

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

func newSelfAwareCoDelQueue(cfg CoDelConfig, clockFunc func() int64) *SelfAwareCoDelQueue {
	return &SelfAwareCoDelQueue{
		cq:                newCoDelQueue(cfg, clockFunc),
		pendingRequests:   make(map[string][]*Request),
		outstandingCounts: make(map[string]int),
		activeRequests:    make(map[string]*Request),
	}
}

// lockedLen returns the number of requests in the CoDel queue.
func (s *SelfAwareCoDelQueue) lockedLen() int {
	return s.cq.lockedLen()
}

// lockedIsHealthy reports whether the CoDel queue is healthy.
func (s *SelfAwareCoDelQueue) lockedIsHealthy() bool {
	return s.cq.lockedIsHealthy()
}

// lockedPeek returns the head of the CoDel queue without removing it.
func (s *SelfAwareCoDelQueue) lockedPeek() *Request {
	return s.cq.lockedPeek()
}

// lockedEnqueue enqueues a request. If the contention ID already has an active
// request in the CoDel queue, the new request is placed in the valve instead.
// Returns the request, whether to schedule a drop timer, and the delay.
func (s *SelfAwareCoDelQueue) lockedEnqueue(contentionID string, priority *float64) (*Request, bool, int64) {
	req := newRequest(priority, s.cq.clockFunc())
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
func (s *SelfAwareCoDelQueue) lockedDequeue() *Request {
	req := s.cq.lockedDequeue()
	if req == nil {
		return nil
	}

	s.onRequestComplete(req)
	return req
}

// lockedDropActive drops the active request for a contention ID (called by the
// CoDel drop timer). Promotes the next pending request from the valve.
func (s *SelfAwareCoDelQueue) lockedDropActive(contentionID string, r *Request) {
	s.cq.lockedCancel(r)
	if !r.isDone() {
		r.done <- &DroppedRequestError{}
	}
	s.onRequestComplete(r)
}

// lockedCancel cancels a request. If it's the active request in the CoDel
// queue, it removes it and promotes the next from the valve. If it's pending
// in the valve, it removes it from there.
func (s *SelfAwareCoDelQueue) lockedCancel(contentionID string, r *Request) {
	if r.elem != nil {
		// in the CoDel queue: remove and promote
		s.cq.lockedCancel(r)
		s.onRequestComplete(r)
	} else {
		// in the valve: find and remove
		s.removeFromValve(contentionID, r)
		s.decrementOutstanding(contentionID)
	}
}

// lockedMarkNotDroppable forwards to the CoDel queue.
func (s *SelfAwareCoDelQueue) lockedMarkNotDroppable(r *Request) {
	s.cq.lockedMarkNotDroppable(r)
}

// --- private helpers ---

func (s *SelfAwareCoDelQueue) codelqEnqueue(req *Request, contentionID string) (*Request, bool, int64) {
	if contentionID != "" {
		s.activeRequests[contentionID] = req
	}

	// enqueue into the CoDel queue (sets elem, enqueuedAt, etc.)
	now := s.cq.clockFunc()
	req.enqueuedAt = now
	req.elem = s.cq.queue.PushBack(req)
	if req.droppable {
		s.cq.droppableLen++
	}

	needSchedule := false
	delay := int64(0)
	if req.droppable && s.cq.droppableLen > 0 && !s.cq.timerScheduled {
		needSchedule = true
		delay = s.cq.lockedCurrentInterval()
		minDelay := s.cq.cfg.MinDropDelayNs()
		if delay < minDelay {
			delay = minDelay
		}
		s.cq.timerScheduled = true
	}

	return req, needSchedule, delay
}

func (s *SelfAwareCoDelQueue) onRequestComplete(req *Request) {
	contentionID := req.contentionID
	if contentionID == "" {
		return
	}

	delete(s.activeRequests, contentionID)
	s.decrementOutstanding(contentionID)
	s.lockedPromote(contentionID)
}

func (s *SelfAwareCoDelQueue) lockedPromote(contentionID string) {
	s.clearDone(contentionID)

	pending, ok := s.pendingRequests[contentionID]
	if !ok || len(pending) == 0 {
		return
	}

	// pop the first pending request and enqueue it
	next := pending[0]
	s.pendingRequests[contentionID] = pending[1:]
	if len(s.pendingRequests[contentionID]) == 0 {
		delete(s.pendingRequests, contentionID)
	}

	s.codelqEnqueue(next, contentionID)
}

// clearDone removes done (cancelled) requests from the head of the valve.
// Their outstanding counts are decremented since the CoDel queue never learned
// about them.
func (s *SelfAwareCoDelQueue) clearDone(contentionID string) {
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

func (s *SelfAwareCoDelQueue) removeFromValve(contentionID string, r *Request) {
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

func (s *SelfAwareCoDelQueue) decrementOutstanding(contentionID string) {
	if contentionID == "" {
		return
	}
	s.outstandingCounts[contentionID]--
	if s.outstandingCounts[contentionID] <= 0 {
		delete(s.outstandingCounts, contentionID)
	}
}
