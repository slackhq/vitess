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

/*
    The main logic is in dequeue(), which acts as a modern state-space
    controller between "no persistent queue" and "has persistent queue".
    When it detects there is a persistent bad queue, it drops requests
    using the well known non-linear relationship of drop rate vs. throughput
    to achieve linear change in throughput (see lockedControlLaw()).

    Consumers of this module are required to call peek()/dequeue() after the
    queue is empty to signal that the underlying resource is available.

    All time units are in ns.

    Configuration:
        INTERVAL: window of time controller acts on; should be roughly the
            upper bound round trip time to detect persistent queue
        TARGET: 5-10% of round trip time / interval time preferably since
            a very small standing queue gives ~100% util of bottleneck link

    Queue State Vars:
        dropNextNs    int64 : when to drop next request. This may seem
                              redundant since lockedRunScheduledDrop is scheduled
                              to run at a particular time, but we need to track
                              this because that coroutine may not actually run
                              when it's supposed to, e.g. if the server is
                              overloaded.
        count           int : requests dropped in drop state
        lastCount       int : count from previous iteration

    Dropping/not-dropping state
    ===========================

        *---------------------------*
        | healthy state             |
        | * dropping: False         |
        | * count: 1                |
        *---------------------------*
                      |  ^
    scheduled drop    |  |
    runs (wasn't      |  | dequeue & hit target
    canceled or resch-|  |       - or -
    eduled before it  |  | dequeue/peek on empty queue
    got to run        v  |
        *--------------------------*
        | dropping state           |
        | * dropping: True         |
        | * count: >= 1            |
        *--------------------------*

    Drop timer
    ==========

      *----------------*  ----| re-schedules itself at
      | drop scheduled |      | rate of queue health (
      *----------------*  <---| which is current interval)
               ^  |                        1. upon scheduled drop
               |  |                        2. upon healthy dequeue
 droppableLen  |  |                           (stop + reschedule)
 goes 0 -> 1   |  | droppableLen == 0
 during        |  | during any of:
 enqueue()     |  | 1. drop timer runs
               |  | 2. peek() on empty queue
               |  v
      *--------------------*
      | drop not scheduled |
      *--------------------*
*/

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

	// CoDelQueue implements the CoDel (Controlled Delay) load-shedding
	// algorithm. All methods are prefixed locked* and assume the caller holds
	// the mutex, which is defined in the files for the higher-level structure
	CoDelQueue struct {
		queue        *list.List
		dropping     bool
		dropNextNs   int64
		count        int
		lastCount    int
		droppableLen int

		cfg               CoDelConfig
		nowNs             func() int64
		scheduleDropTimer func(delayNs int64)
		stopDropTimer     func()
		onPeekCleanup     func(*Request)
	}
)

func (e *DroppedRequestError) Error() string {
	return "request dropped by CoDel queue"
}

func newCoDelQueue(cfg CoDelConfig, nowNs func() int64, scheduleDropTimer func(delayNs int64), stopDropTimer func(), onPeekCleanup func(*Request)) *CoDelQueue {
	return &CoDelQueue{
		queue:             list.New(),
		count:             1,
		lastCount:         1,
		cfg:               cfg,
		nowNs:             nowNs,
		scheduleDropTimer: scheduleDropTimer,
		stopDropTimer:     stopDropTimer,
		onPeekCleanup:     onPeekCleanup,
	}
}

func (q *CoDelQueue) lockedLen() int {
	return q.queue.Len()
}

func (q *CoDelQueue) lockedIsHealthy() bool {
	return !q.dropping
}

func (q *CoDelQueue) lockedEnqueue(req *Request) {
	req.codelqEnqueuedAtNs = q.nowNs()
	req.codelqElem = q.queue.PushBack(req)

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
	req := q.lockedPopElem(q.queue.Front(), grantSentinel)

	sojournTime := q.nowNs() - req.codelqEnqueuedAtNs
	if sojournTime < q.cfg.TargetNs() {
		q.dropping = false
		q.stopDropTimer()
		q.lockedArmDropTimer()
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
		if req.signaledValue == nil || req.signaledValue == grantSentinel {
			return req
		}
		q.queue.Remove(front)
		req.codelqElem = nil
		if req.isDroppable() {
			q.droppableLen--
		}
		if q.onPeekCleanup != nil {
			q.onPeekCleanup(req)
		}
	}
	// Empty queue means the underlying resource is available.
	q.dropping = false
	return nil
}

// lockedPopElem removes the given element from the queue, signals the request's
// result channel, and updates bookkeeping. Use with care: this bypasses the
// health-state transitions in peek/dequeue, so callers are responsible for
// updating dropping state if appropriate.
func (q *CoDelQueue) lockedPopElem(elem *list.Element, err error) *Request {
	req := elem.Value.(*Request)
	q.queue.Remove(elem)
	req.codelqElem = nil

	if req.signaledValue == nil {
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
	if r.codelqElem == nil {
		return
	}
	q.queue.Remove(r.codelqElem)
	r.codelqElem = nil

	if r.isDroppable() {
		q.droppableLen--
		if q.droppableLen == 0 && q.dropping {
			q.dropping = false
		}
	}
}

// lockedOnGrant marks a request as undroppable. Uses the undroppable sentinel
// priority.
func (q *CoDelQueue) lockedOnGrant(r *Request) {
	if !r.isDroppable() {
		return
	}
	r.priority = priorityUndroppable
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
		if req.signaledValue != nil || !req.isDroppable() {
			continue
		}
		if req.priority == 0 {
			return e
		}
		if best == nil {
			best = e
		} else {
			bestReq := best.Value.(*Request)
			if req.priority < bestReq.priority {
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

	now := q.nowNs()
	if !q.dropping {
		q.lockedEnterDroppingState()
	}

	loopCount := 0
	for q.droppableLen > 0 && now >= q.dropNextNs {
		loopCount++
		if loopCount > 100 {
			break
		}
		// Safe to break: the mutex serializes cancellation with the drop
		// timer, so droppableLen > 0 reliably means the scan will find
		// something. This can only fail on a bookkeeping bug.
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
	now := q.nowNs()
	q.dropping = true
	delta := q.count - q.lastCount
	q.count = 1

	// Restore prior dropping intensity if we recently left the dropping
	// state and re-entered quickly. Without this, every transition back
	// to dropping would ramp up from count=1 (i.e. one drop per full
	// interval), losing the "memory" of how aggressive we needed to be.
	// The 16x threshold is the staleness cutoff — if we've been healthy
	// for much longer than the interval, the old state is irrelevant.
	if delta > 1 && (now-q.dropNextNs < 16*q.cfg.IntervalNs()) {
		q.count = delta
	}

	q.dropNextNs = q.lockedControlLaw(now)
	q.lastCount = q.count
}

// lockedControlLaw computes the next drop time. The interval shrinks in
// inverse proportion to count^exponent, exploiting the non-linear
// relationship between drop rate and throughput to achieve linear change
// in throughput.
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
	// Floor to avoid extreme cases and floating point precision issues.
	return max(result, 100)
}

func (q *CoDelQueue) lockedArmDropTimer() {
	delay := q.lockedCurrentInterval()
	minDelay := q.cfg.MinDropDelayNs()
	if delay < minDelay {
		delay = minDelay
	}
	q.scheduleDropTimer(delay)
}
