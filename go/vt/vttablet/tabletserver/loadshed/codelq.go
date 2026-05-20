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

		cfg               CoDelConfig
		now               func() int64
		scheduleDropTimer func(delayNs int64)
		onPeekCleanup     func(*Request)
	}
)

func (e *DroppedRequestError) Error() string {
	return "request dropped by CoDel queue"
}

func newCoDelQueue(cfg CoDelConfig, now func() int64, scheduleDropTimer func(delayNs int64), onPeekCleanup func(*Request)) *CoDelQueue {
	return &CoDelQueue{
		queue:             list.New(),
		count:             1,
		lastCount:         1,
		cfg:               cfg,
		now:               now,
		scheduleDropTimer: scheduleDropTimer,
		onPeekCleanup:     onPeekCleanup,
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
func (q *CoDelQueue) lockedEnqueue(priority *float64) *Request {
	req := newRequest(priority)
	q.lockedEnqueueRequest(req)
	return req
}

// lockedEnqueueRequest inserts an already-created request into the queue.
// Used by SelfContentionAwareCoDelQueue to enqueue requests that were
// created earlier and held in the valve.
func (q *CoDelQueue) lockedEnqueueRequest(req *Request) {
	req.enqueuedAtNs = q.now()
	req.elem = q.queue.PushBack(req)

	if req.isDroppable() {
		q.droppableLen++
		q.lockedArmDropTimer()
	}
}

// lockedDequeue pops the next eligible request from the head of the queue.
// Returns nil if the queue is empty.
func (q *CoDelQueue) lockedDequeue() *Request {
	if q.lockedPeek() == nil {
		return nil
	}
	req := q.lockedPopElem(q.queue.Front(), nil)

	now := q.now()
	sojournTime := now - req.enqueuedAtNs
	if sojournTime < q.cfg.TargetNs() {
		q.dropping = false

		if q.droppableLen > 0 {
			q.lockedArmDropTimer()
		}
	}

	return req
}

// lockedPeek returns the head request without removing it. As a side effect,
// cleans up done-and-not-granted requests at the head (requests whose result
// channel has an error). Empty queue transitions to healthy.
func (q *CoDelQueue) lockedPeek() *Request {
	for q.queue.Len() > 0 {
		front := q.queue.Front()
		req := front.Value.(*Request)
		if !req.signaled.Load() {
			return req
		}
		if req.signaledValue == nil {
			return req
		}
		q.queue.Remove(front)
		req.elem = nil
		if req.isDroppable() {
			q.droppableLen--
		}
		if q.onPeekCleanup != nil {
			q.onPeekCleanup(req)
		}
	}
	q.dropping = false
	return nil
}

// lockedPopElem removes the given element from the queue, signals the request's
// result channel, and updates bookkeeping.
func (q *CoDelQueue) lockedPopElem(elem *list.Element, err error) *Request {
	req := elem.Value.(*Request)
	q.queue.Remove(elem)
	req.elem = nil

	if !req.signaled.Load() {
		req.signal(err)
	}

	if req.isDroppable() {
		q.droppableLen--
		if q.droppableLen == 0 && q.dropping {
			q.dropping = false
		}
	}

	return req
}

// lockedRemove removes a specific request from the queue without signaling it.
func (q *CoDelQueue) lockedRemove(r *Request) {
	if r.elem == nil {
		return
	}
	q.queue.Remove(r.elem)
	r.elem = nil

	if r.isDroppable() {
		q.droppableLen--
		if q.droppableLen == 0 && q.dropping {
			q.dropping = false
		}
	}
}

// lockedMarkNotDroppable marks a request as not droppable (e.g., when it is
// granted). Uses the undroppable sentinel priority.
func (q *CoDelQueue) lockedMarkNotDroppable(r *Request) {
	if !r.isDroppable() {
		return
	}
	r.priority = newUndroppablePriority()
	q.droppableLen--
	if q.droppableLen == 0 && q.dropping {
		q.dropping = false
	}
}

// lockedFindLowestPriorityDroppable finds the lowest-priority droppable
// element in the queue. Returns nil if none exists.
func (q *CoDelQueue) lockedFindLowestPriorityDroppable() *list.Element {
	var best *list.Element

	for e := q.queue.Front(); e != nil; e = e.Next() {
		req := e.Value.(*Request)
		if req.signaled.Load() || !req.isDroppable() {
			continue
		}
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

// lockedRunScheduledDrop executes the CoDel drop logic. The dropFn is called
// for each drop; it should remove the request from the queue and handle any
// promotion.
func (q *CoDelQueue) lockedRunScheduledDrop(dropFn func() bool) {
	if q.droppableLen == 0 {
		return
	}

	now := q.now()
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

	if q.droppableLen > 0 {
		q.lockedArmDropTimer()
	}
}

// lockedEnterDroppingState transitions to the dropping state, possibly
// restoring the drop count from a prior dropping period.
func (q *CoDelQueue) lockedEnterDroppingState() {
	now := q.now()
	q.dropping = true
	delta := q.count - q.lastCount
	q.count = 1

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

func (q *CoDelQueue) lockedArmDropTimer() {
	delay := q.lockedCurrentInterval()
	minDelay := q.cfg.MinDropDelayNs()
	if delay < minDelay {
		delay = minDelay
	}
	q.scheduleDropTimer(delay)
}
