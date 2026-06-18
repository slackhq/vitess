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
    The main logic is in lockedRunTimer(), which acts as a modern state-space
    controller between "no persistent queue" and "has persistent queue".
    When it detects there is a persistent bad queue, it drops requests
    using the well known non-linear relationship of drop rate vs. throughput
    to achieve linear change in throughput (see lockedControlLaw()).

    All time units are in ns.

    Configuration:
        INTERVAL: window of time controller acts on; should be roughly the
            upper bound round trip time to detect persistent queue
        TARGET: 5-10% of round trip time / interval time preferably since
            a very small standing queue gives ~100% util of bottleneck link

    Queue State Vars:
        dropNextNs    int64 : when to drop next request. This may seem
                              redundant since lockedRunTimer is scheduled
                              to run at a particular time, but we need to track
                              this because that coroutine may not actually run
                              when it's supposed to, e.g. if the server is
                              overloaded.
        count           int : drop intensity. Determines the timer interval
                              via interval/count^exp. Increases during dropping,
                              decays during easing, until it reaches 1 (idle).
        lastCount       int : count from previous dropping entry

    CoDel states
    ============

        *---------------------------*
        | idle                      |       fully relaxed
        | * dropping: false         |
        | * count: 1                |
        | * timer: not armed        |
        *---------------------------*
                      |  ^
    timer fires w/    |  |  easing timer fires, healthy,
    droppableLen > 0  |  |  count decayed to 1
    (standard entry)  |  |
                      v  |
        *---------------------------*
        | easing                    |       gradually relaxing
        | * dropping: false         |
        | * count: > 1 (decaying)   |
        | * timer: armed            |
        *---------------------------*
              |  ^            ^
    timer     |  |            | lockedComplete() w/ sojourn < target
    fires,    |  |            | (sets dropping=false, sets sawHealthy)
    NOT       |  | timer fires,
    healthy   |  | healthy (sawHealthy OR droppableLen==0)
              v  |
        *---------------------------*
        | dropping                  |       actively shedding
        | * dropping: true          |
        | * count: >= 1 (growing)   |
        | * timer: armed            |
        *---------------------------*

    Health condition (checked each easing timer fire):
      healthy = sawHealthy OR droppableLen==0
      sawHealthy is set by lockedComplete() when sojourn < target,
      cleared each timer fire.

    Timer lifecycle
    ===============

      *-------------*  ----| re-arms itself at interval/count^exp:
      | timer armed |      |   dropping: after each drop (shorter intervals)
      *-------------*  <---|   easing: after each count decay (longer intervals)
               ^  |
               |  |
 droppableLen  |  | easing, healthy, count==1
 goes 0 → 1   |  | (fully relaxed, nothing to do)
 during        |  |
 enqueue()     |  v
      *-----------------*
      | timer not armed |
      *-----------------*

    The timer fires (lockedRunTimer) and branches on state:

      dropping=true:
        drop loop: while droppableLen>0 && now>=dropNextNs, drop+count++.
        re-arm if droppableLen > 0.

      dropping=false (easing), healthy:
        decay count by EasingDivisor.
        if count > 1: re-arm (easing continues)
        if count == 1: stop (→ idle)

      dropping=false (easing), NOT healthy:
        re-enter dropping at current count, then drop.
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
		EasingDivisor  func() float64
	}

	// CoDelQueue implements the CoDel (Controlled Delay) load-shedding
	// algorithm. All methods are prefixed locked* and assume the caller holds
	// the mutex, which is defined in the files for the higher-level structure
	CoDelQueue struct {
		queue           *list.List
		firstWaiting    *list.Element
		dropping        bool
		dropNextNs      int64
		count           int
		lastCount       int
		droppableLen    int
		sawHealthy bool

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

	if q.firstWaiting == nil {
		q.firstWaiting = req.codelqElem
	}

	if req.isDroppable() {
		q.droppableLen++
		q.lockedArmDropTimer()
	}
}

// lockedComplete removes a granted (undroppable) request from the queue on
// Release. Checks sojourn time for CoDel state transition — if the completed
// request spent less than TargetNs in the queue, the system is healthy.
// Rather than hard-stopping the timer, we enter an easing phase
// (!dropping, count > 1) where the timer continues to fire, decaying count
// each time until fully relaxed.
func (q *CoDelQueue) lockedComplete(r *Request) {
	q.queue.Remove(r.codelqElem)
	r.codelqElem = nil

	sojournTime := q.nowNs() - r.codelqEnqueuedAtNs
	if sojournTime < q.cfg.TargetNs() {
		q.dropping = false
		q.sawHealthy = true
	}
}

// lockedFirstWaiting returns the first not-yet-granted request in the queue.
func (q *CoDelQueue) lockedFirstWaiting() *Request {
	if q.firstWaiting == nil {
		return nil
	}
	return q.firstWaiting.Value.(*Request)
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
		q.lockedAdvanceFirstWaiting(front)
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
	q.lockedAdvanceFirstWaiting(elem)
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
	q.lockedAdvanceFirstWaiting(r.codelqElem)
	q.queue.Remove(r.codelqElem)
	r.codelqElem = nil

	if r.isDroppable() {
		q.droppableLen--
		if q.droppableLen == 0 && q.dropping {
			q.dropping = false
		}
	}
}

func (q *CoDelQueue) lockedOnGrant(r *Request) {
	if r.isDroppable() {
		r.priority = priorityUndroppable
		q.droppableLen--
		if q.droppableLen == 0 && q.dropping {
			q.dropping = false
		}
	}
	q.lockedAdvanceFirstWaiting(r.codelqElem)
}

// lockedAdvanceFirstWaiting advances the firstWaiting pointer past elem if
// elem is the current firstWaiting. elem is a queue entry that is no longer
// waiting — either because it was granted, removed, or dropped.
func (q *CoDelQueue) lockedAdvanceFirstWaiting(elem *list.Element) {
	if q.firstWaiting != elem {
		return
	}
	for e := elem.Next(); e != nil; e = e.Next() {
		if e.Value.(*Request).signaledValue == nil {
			q.firstWaiting = e
			return
		}
	}
	q.firstWaiting = nil
}

// lockedFindLowestPriorityDroppable finds the lowest-priority droppable
// element in the queue. Returns nil if none exists. Currently O(n); we
// are planning on an optimization to make this O(log(n)).
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

// lockedRunTimer executes the CoDel drop logic. The dropFn is called
// for each drop; it should remove the request from the queue and handle any
// promotion.
func (q *CoDelQueue) lockedRunTimer(dropFn func() bool) {
	// Dropping: actively shed load.
	if q.dropping {
		now := q.nowNs()

		for q.droppableLen > 0 && now >= q.dropNextNs {
			if !dropFn() {
				break
			}
			q.count++
			q.dropNextNs = q.lockedControlLaw(q.dropNextNs)
		}

		if q.droppableLen > 0 {
			q.lockedArmDropTimer()
			return
		}

		// Queue drained — transition to easing.
		q.dropping = false
		q.sawHealthy = true
		if q.count > 1 {
			q.count = max(int(float64(q.count)/q.cfg.EasingDivisor()), 1)
			if q.count > 1 {
				q.dropNextNs = q.lockedControlLaw(q.nowNs())
				q.lockedArmDropTimer()
			}
		}
		return
	}

	// Easing: check if health was observed since last timer fire.
	// Health means either (a) a completion had sojourn < target, or
	// (b) droppableLen is 0 (nothing waiting = healthy).
	healthy := q.sawHealthy || q.droppableLen == 0
	q.sawHealthy = false

	if healthy {
		// System is healthy this interval — continue easing down.
		if q.count > 1 {
			q.count = max(int(float64(q.count)/q.cfg.EasingDivisor()), 1)
		}
		if q.count > 1 {
			q.dropNextNs = q.lockedControlLaw(q.nowNs())
			q.lockedArmDropTimer()
		}
		// count == 1: fully relaxed, timer not re-armed → idle.
		return
	}

	// Unhealthy during easing: re-enter dropping at current count.
	q.lockedEnterDroppingState()

	now := q.nowNs()

	for q.droppableLen > 0 && now >= q.dropNextNs {
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

	// If re-entering mid-ease-out, count is still > 1 — use it as-is.
	if q.count > 1 {
		q.dropNextNs = q.lockedControlLaw(now)
		q.lastCount = q.count
		return
	}

	// Otherwise, apply the memory heuristic from standard CoDel.
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
// The interval is compressed whenever count > 1 — both in the dropping state
// and during easing (!dropping, count > 1), so that the ease-out timer fires
// at progressively longer intervals as the count decays toward 1.
func (q *CoDelQueue) lockedCurrentInterval() int64 {
	interval := q.cfg.IntervalNs()
	if q.count <= 1 {
		return interval
	}
	exp := q.cfg.Exponent()
	result := int64(float64(interval) / math.Pow(float64(q.count), exp))
	// Floor of 1ns ensures lockedControlLaw always makes forward progress.
	return max(result, 1)
}

func (q *CoDelQueue) lockedArmDropTimer() {
	delay := q.lockedCurrentInterval()
	minDelay := q.cfg.MinDropDelayNs()
	if delay < minDelay {
		delay = minDelay
	}
	q.scheduleDropTimer(delay)
}
