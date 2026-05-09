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
)

type (
	// DroppedRequestError is returned when a request is dropped by the CoDel
	// queue due to persistent queue buildup.
	DroppedRequestError struct{}

	// CoDelConfig holds dynamic configuration functions for the CoDel algorithm.
	// All fields are functions to allow runtime tuning.
	CoDelConfig struct {
		IntervalNs     func() int64
		TargetNs       func() int64
		Exponent       func() float64
		MinDropDelayNs func() int64
	}

	// CoDelQueue implements the CoDel (Controlled Delay) load-shedding algorithm.
	// All methods are prefixed locked* and assume the caller holds the parent mutex.
	CoDelQueue struct {
		queue        *list.List
		dropping     bool
		dropNextNs   int64
		count        int
		lastCount    int
		droppableLen int
		// timerScheduled tracks whether the parent has a drop timer armed.
		// Set by the parent after lockedEnqueue returns needSchedule=true.
		timerScheduled bool

		cfg       CoDelConfig
		clockFunc func() int64
	}
)

func (e *DroppedRequestError) Error() string {
	return "request dropped by CoDel queue"
}

func newCoDelQueue(cfg CoDelConfig, clockFunc func() int64) *CoDelQueue {
	return &CoDelQueue{
		queue:     list.New(),
		count:     1,
		lastCount: 1,
		cfg:       cfg,
		clockFunc: clockFunc,
	}
}

// lockedLen returns the number of requests in the queue.
func (q *CoDelQueue) lockedLen() int {
	return q.queue.Len()
}

// lockedIsHealthy reports whether the queue is in the healthy (not dropping)
// state.
func (q *CoDelQueue) lockedIsHealthy() bool {
	return !q.dropping
}

// lockedEnqueue creates a new request and inserts it into the queue.
// Returns the request, whether the parent should schedule a drop timer,
// and the delay in nanoseconds for the timer.
func (q *CoDelQueue) lockedEnqueue(priority *float64) (*Request, bool, int64) {
	now := q.clockFunc()
	req := newRequest(priority, now)
	return q.lockedEnqueueRequest(req)
}

// lockedEnqueueRequest inserts an already-created request into the queue.
// Used by SelfContentionAwareCoDelQueue to enqueue requests that were
// created earlier and held in the valve.
func (q *CoDelQueue) lockedEnqueueRequest(req *Request) (*Request, bool, int64) {
	req.enqueuedAt = q.clockFunc()
	req.elem = q.queue.PushBack(req)

	if req.droppable {
		q.droppableLen++
	}

	needSchedule := false
	delay := int64(0)
	if req.droppable && q.droppableLen > 0 && !q.timerScheduled {
		needSchedule = true
		delay = q.lockedCurrentInterval()
		minDelay := q.cfg.MinDropDelayNs()
		if delay < minDelay {
			delay = minDelay
		}
		q.timerScheduled = true
	}

	return req, needSchedule, delay
}

// lockedDequeue pops the next eligible request from the head of the queue.
// Returns nil if the queue is empty.
func (q *CoDelQueue) lockedDequeue() *Request {
	if q.lockedPeek() == nil {
		return nil
	}
	req := q.lockedPopElem(q.queue.Front(), nil)

	now := q.clockFunc()
	sojournTime := now - req.enqueuedAt
	if sojournTime < q.cfg.TargetNs() {
		q.dropping = false
		q.lockedClearTimerFlag()
	}

	return req
}

// lockedPeek returns the head request without removing it. As a side effect,
// cleans up done-and-not-granted requests at the head (requests whose done
// channel has an error). Empty queue transitions to healthy.
func (q *CoDelQueue) lockedPeek() *Request {
	for q.queue.Len() > 0 {
		front := q.queue.Front()
		req := front.Value.(*Request)
		if !req.isDone() {
			return req
		}
		if req.result == nil {
			return req
		}
		q.queue.Remove(front)
		req.elem = nil
	}
	q.dropping = false
	return nil
}

// lockedPopElem removes the given element from the queue, signals the request's
// done channel, and updates bookkeeping.
func (q *CoDelQueue) lockedPopElem(elem *list.Element, err error) *Request {
	req := elem.Value.(*Request)
	q.queue.Remove(elem)
	req.elem = nil

	if !req.isDone() {
		req.signal(err)
	}

	if req.droppable {
		req.droppable = false
		q.droppableLen--
		if q.droppableLen == 0 {
			q.lockedClearTimerFlag()
		}
	}

	return req
}

// lockedCancel removes a specific request from the queue.
func (q *CoDelQueue) lockedCancel(r *Request) {
	if r.elem == nil {
		return
	}
	q.queue.Remove(r.elem)
	r.elem = nil

	if r.droppable {
		r.droppable = false
		q.droppableLen--
		if q.droppableLen == 0 {
			q.lockedClearTimerFlag()
		}
	}
}

// lockedMarkNotDroppable marks a request as not droppable (e.g., when it is
// granted). Decrements droppableLen if needed.
func (q *CoDelQueue) lockedMarkNotDroppable(r *Request) {
	if !r.droppable {
		return
	}
	r.droppable = false
	q.droppableLen--
	if q.droppableLen == 0 {
		q.lockedClearTimerFlag()
	}
}

// lockedFindLowestPriorityDroppable finds the lowest-priority droppable
// element in the queue. Returns nil if none exists.
func (q *CoDelQueue) lockedFindLowestPriorityDroppable() *list.Element {
	var best *list.Element

	for e := q.queue.Front(); e != nil; e = e.Next() {
		req := e.Value.(*Request)
		if req.isDone() || !req.droppable {
			continue
		}
		// priority 0 is the instant pick
		if req.priority != nil && *req.priority == 0 {
			return e
		}
		if best == nil {
			best = e
		} else {
			bestReq := best.Value.(*Request)
			if *req.priority < *bestReq.priority {
				best = e
			}
		}
	}

	return best
}

// lockedDropLowestPriority finds and drops the lowest-priority droppable
// request. Returns nil if no droppable request exists.
func (q *CoDelQueue) lockedDropLowestPriority() *Request {
	elem := q.lockedFindLowestPriorityDroppable()
	if elem == nil {
		return nil
	}
	return q.lockedPopElem(elem, &DroppedRequestError{})
}

// lockedRunScheduledDrop executes the CoDel drop logic. The dropFn is called
// for each drop; it should remove the request from the queue and handle any
// promotion. Returns whether to reschedule and the delay in nanoseconds.
func (q *CoDelQueue) lockedRunScheduledDrop(dropFn func() bool) (reschedule bool, delayNs int64) {
	q.timerScheduled = false

	if q.droppableLen == 0 {
		return false, 0
	}

	now := q.clockFunc()
	if !q.dropping {
		q.lockedEnterDroppingState()
	}

	loopCount := 0
	for q.droppableLen > 0 && now >= q.dropNextNs {
		loopCount++
		if loopCount > 100 {
			break
		}
		if !dropFn() {
			break
		}
		q.count++
		q.dropNextNs = q.lockedControlLaw(q.dropNextNs)
	}

	if q.droppableLen == 0 {
		return false, 0
	}

	delay := q.dropNextNs - now
	minDelay := q.cfg.MinDropDelayNs()
	if delay < minDelay {
		delay = minDelay
	}
	return true, delay
}

// lockedEnterDroppingState transitions to the dropping state, possibly
// restoring the drop count from a prior dropping period.
func (q *CoDelQueue) lockedEnterDroppingState() {
	now := q.clockFunc()
	q.dropping = true
	delta := q.count - q.lastCount
	q.count = 1

	// restore prior state if the last dropping period was recent
	if delta > 1 && (now-q.dropNextNs < 16*q.cfg.IntervalNs()) {
		q.count = delta
	}

	q.dropNextNs = q.lockedControlLaw(now)
	q.lastCount = q.count
}

// lockedControlLaw computes the next drop time.
func (q *CoDelQueue) lockedControlLaw(t int64) int64 {
	return t + q.lockedCurrentInterval()
}

// lockedCurrentInterval returns the current interval for the control law.
func (q *CoDelQueue) lockedCurrentInterval() int64 {
	interval := q.cfg.IntervalNs()
	if !q.dropping {
		return interval
	}
	exp := q.cfg.Exponent()
	result := int64(float64(interval) / math.Pow(float64(q.count), exp))
	if result < 100 {
		return 100
	}
	return result
}

func (q *CoDelQueue) lockedClearTimerFlag() {
	q.timerScheduled = false
}
