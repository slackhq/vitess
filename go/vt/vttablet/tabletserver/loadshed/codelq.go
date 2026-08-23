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
                              overloaded. Seeded to now+interval/count^exp when a
                              dropping episode begins so the first fire paces
                              drops rather than draining the backlog.
        count           int : drop intensity. Determines the timer interval
                              via interval/count^exp. Increases during dropping,
                              decays during easing, until it reaches 1 (idle).

    CoDel states
    ============

        *---------------------------*
        | idle                      |       fully relaxed
        | * dropping: false         |
        | * count: 1                |
        | * timer: not armed        |
        *---------------------------*
                      |  ^
    episode armed:    |  |  easing timer fires, healthy,
    droppable enqueue  |  |  count decayed to 1
                      |  |
                      v  |
        *---------------------------*
        | easing                    |       gradually relaxing
        | * dropping: false         |       (transiently true after each
        | * count: > 1 (decaying)   |        re-arm; next fire re-checks health)
        | * timer: armed            |
        *---------------------------*
              |  ^            ^
    timer     |  |            | dequeue w/ sojourn < target
    fires,    |  |            | or queue emptied (sets dropping=false)
    NOT       |  | timer fires,
    healthy   |  | healthy (dropping=false)
              v  |
        *---------------------------*
        | dropping                  |       actively shedding
        | * dropping: true          |
        | * count: >= 1 (growing)   |
        | * timer: armed            |
        *---------------------------*

    A droppable enqueue arms the timer immediately. The episode leaves count==1
    only by the control-law ramp (count++ per drop). The timer re-arms while
    droppableLen>0 || count>1.

    Health condition (checked each easing timer fire):
      healthy := dropping=false
      dropping is unset by lockedDequeue() when a dequeued request's queue-wait
      sojourn < target, and by lockedRemove/lockedDequeue when
      droppableLen reaches 0; reset each timer fire. Note:
      while easing, each re-arm transiently re-marks dropping=true; the next
      fire re-evaluates health.

    Timer lifecycle
    ===============

      *-------------*  ----| re-arms itself at interval/count^exp:
      | timer armed |      |   dropping: after each drop (shorter intervals)
      *-------------*  <---|   easing: after each count decay (longer intervals)
               ^  |
               |  |
 episode       |  | count eases to 1
 armed on      |  | (fully relaxed, nothing to do)
 enqueue       |  |
               |  v
      *-----------------*
      | timer not armed |
      *-----------------*

    The timer fires (lockedRunTimer) and branches on state:

      dropping=true:
        drop loop: while droppableLen>0 && now>=dropNextNs, drop+count++.
        re-arm if count > 1.

      dropping=false (easing), healthy:
        decay count via lockedEaseCount (log_EasingLogBase based).
        if count > 1: re-arm (easing continues)
        if count == 1: stop (→ idle)

      dropping=false (easing), NOT healthy:
        re-enter dropping at current count, then drop.
*/

type (
	// CoDelConfig holds dynamic configuration functions for the CoDel algorithm.
	// All fields are functions to allow runtime tuning.
	CoDelConfig struct {
		IntervalNs     func() int64
		TargetNs       func() int64
		Exponent       func() float64
		MinDropDelayNs func() int64

		// EasingLogBase controls how the drop count decays each easing timer
		// fire: count -= floor(log_base(count) / base), floored at 1. A larger
		// base yields a smaller step (gentler ease-out). Defaults to 3 when
		// unset or <= 1.
		EasingLogBase func() float64
	}

	// CoDelQueue implements the CoDel (Controlled Delay) load-shedding
	// algorithm. All methods are prefixed locked* and assume the caller holds
	// the mutex, which is defined in the files for the higher-level structure
	CoDelQueue[T any] struct {
		queue        *list.List
		dropping     bool
		dropNextNs   int64
		count        int
		droppableLen int
		// droppable indexes the droppable queue entries by priority so the
		// lowest-priority one is found in O(1) rather than an O(n) scan. Kept in
		// lockstep with droppableLen: every insert/remove pairs with a ++/--.
		droppable droppableIndex[T]

		cfg               CoDelConfig
		nowNs             func() int64
		scheduleDropTimer func(delayNs int64)
		stopDropTimer     func()
	}
)

func newCoDelQueue[T any](cfg CoDelConfig, nowNs func() int64, scheduleDropTimer func(delayNs int64), stopDropTimer func()) *CoDelQueue[T] {
	q := &CoDelQueue[T]{
		queue:             list.New(),
		count:             1,
		cfg:               cfg,
		nowNs:             nowNs,
		scheduleDropTimer: scheduleDropTimer,
		stopDropTimer:     stopDropTimer,
	}
	q.droppable.init()
	return q
}

func (q *CoDelQueue[T]) lockedLen() int {
	return q.queue.Len()
}

func (q *CoDelQueue[T]) lockedIsHealthy() bool {
	return !q.dropping
}

func (q *CoDelQueue[T]) lockedEnqueue(req *Request[T]) {
	now := q.nowNs()

	req.codelqEnqueuedAtNs = now
	req.codelqElem = q.queue.PushBack(req)
	req.queued = true

	if req.isDroppable() {
		q.droppableLen++
		q.droppable.insert(req)
		// droppableLen == 1 implies easing, so restart the interval
		if q.dropNextNs == 0 || q.droppableLen == 1 {
			// make sure we're all caught up
			if q.dropNextNs > 0 {
				q.lockedAdvance(now, func() bool { return false })
			}
			q.dropNextNs = q.lockedControlLaw(now)
			q.lockedArmDropTimer()
		}
	}
}

// lockedPeek returns the first waiting request in the queue.
func (q *CoDelQueue[T]) lockedPeek() *Request[T] {
	first := q.queue.Front()
	if first == nil {
		return nil
	}
	return first.Value.(*Request[T])
}

// lockedRemove removes a specific request from the queue.
func (q *CoDelQueue[T]) lockedRemove(r *Request[T]) {
	if r.codelqElem == nil {
		return
	}
	q.queue.Remove(r.codelqElem)
	r.codelqElem = nil
	r.queued = false

	if r.isDroppable() {
		q.droppableLen--
		q.droppable.remove(r)
		if q.droppableLen == 0 && q.dropping {
			q.dropping = false
		}
	}
}

func (q *CoDelQueue[T]) lockedDequeue(r *Request[T]) {
	// CoDel health check, measured at dequeue: if this request's queue-wait
	// (now - enqueue) was under target, the system is healthy — leave the
	// dropping state. Separate from the droppableLen==0 clear below.
	if q.nowNs()-r.codelqEnqueuedAtNs < q.cfg.TargetNs() {
		q.dropping = false
	}
	q.lockedRemove(r)
}

// lockedFindLowestPriorityDroppable finds the lowest-priority droppable
// element in the queue — the oldest one at the lowest priority present — or nil
// if none exists. O(1) via the droppable priority index (see droppableIndex).
func (q *CoDelQueue[T]) lockedFindLowestPriorityDroppable() *list.Element {
	req := q.droppable.min()
	if req == nil {
		return nil
	}
	return req.codelqElem
}

// lockedRunTimer runs the CoDel drop logic. It is invoked both by the backstop
// timer and synchronously from the dequeue path, so shedding is driven
// as slots free rather than waiting for the (possibly late) timer to fire.
func (q *CoDelQueue[T]) lockedRunTimer(dropFn func() bool) {
	q.lockedRunTimerLimited(dropFn, -1)
}

func (q *CoDelQueue[T]) lockedRunTimerLimited(dropFn func() bool, maxDrops int) {
	now := q.nowNs()

	// Paced work: only advance the drop/ease control law and re-arm when a drop
	// is actually due. dropNextNs==0 means no episode is armed and none is due.
	if q.dropNextNs == 0 || now < q.dropNextNs {
		return
	}

	q.lockedAdvanceLimited(now, dropFn, maxDrops)

	if q.droppableLen > 0 || q.count > 1 {
		q.lockedArmDropTimer()
	} else {
		q.dropNextNs = 0
	}
}

// lockedAdvance runs the clock-driven core of the CoDel control law: it sheds
// every request that is due (now >= dropNextNs) while dropping, ramping count,
// then eases count back down while healthy. Every action is gated on the fresh
// `now`, so calling it is idempotent and safe outside the timer — the dequeue
// path invokes it to shed stale requests in real time rather than
// waiting on the possibly-late backstop timer. It does NOT arm/disarm the timer.
func (q *CoDelQueue[T]) lockedAdvance(now int64, dropFn func() bool) {
	q.lockedAdvanceLimited(now, dropFn, -1)
}

func (q *CoDelQueue[T]) lockedAdvanceLimited(now int64, dropFn func() bool, maxDrops int) {
	drops := 0
	// Step the control law per interval while a drop is due AND there is still
	// work to do: either a droppable backlog to shed, or an elevated count that
	// must ease back to 1. The count>1 term is essential for recovery — once the
	// queue drains (droppableLen==0) the ease branch still needs to run to decay
	// count and end the episode, otherwise the queue never returns to healthy.
	for now >= q.dropNextNs && (q.droppableLen > 0 || q.count > 1) && (maxDrops < 0 || drops < maxDrops) {
		// Dropping: actively shed load.
		dropped := false
		if q.dropping {
			dropped = dropFn()
			if dropped {
				drops++
				q.count++
				q.dropNextNs = q.lockedControlLaw(q.dropNextNs)
			}
		}
		if !dropped {
			// System is healthy this interval — continue easing down.
			q.count = q.lockedEaseCount()
			q.dropNextNs = q.lockedControlLaw(q.dropNextNs)
		}

		q.dropping = false

		// If a request arrived before the end of the interval then it is a
		// candidate for dropping.
		if q.droppableLen > 0 {
			if first := q.lockedPeek(); first != nil && first.codelqEnqueuedAtNs < q.dropNextNs {
				q.dropping = true
			}
		}
	}
}

// lockedEaseCount returns the next drop count during easing:
// count -= floor(log_base(count) / base), floored at 1. A larger base yields a
// smaller step (gentler ease-out); base defaults to 3 when unset or <= 1.
func (q *CoDelQueue[T]) lockedEaseCount() int {
	base := 3.0
	if q.cfg.EasingLogBase != nil {
		base = q.cfg.EasingLogBase()
	}
	if base <= 1 {
		base = 3.0
	}
	step := int(math.Log(float64(q.count)) / math.Log(base) / base)
	return max(q.count-max(step, 1), 1)
}

// lockedControlLaw computes the next drop time. The interval shrinks in
// inverse proportion to count^exponent, exploiting the non-linear
// relationship between drop rate and throughput to achieve linear change
// in throughput.
func (q *CoDelQueue[T]) lockedControlLaw(t int64) int64 {
	return t + q.lockedCurrentInterval()
}

// lockedCurrentInterval returns the current interval for the control law.
// The interval is compressed whenever count > 1 — both in the dropping state
// and during easing (!dropping, count > 1), so that the ease-out timer fires
// at progressively longer intervals as the count decays toward 1.
func (q *CoDelQueue[T]) lockedCurrentInterval() int64 {
	interval := q.cfg.IntervalNs()
	if q.count <= 1 {
		return interval
	}
	exp := q.cfg.Exponent()
	result := int64(float64(interval) / math.Pow(float64(q.count), exp))
	// Floor of 1ns ensures lockedControlLaw always makes forward progress.
	return max(result, 1)
}

func (q *CoDelQueue[T]) lockedArmDropTimer() {
	// Mark the episode active for this armed interval; the next timer fire
	// re-evaluates health.
	q.dropping = q.droppableLen > 0
	delay := max(q.dropNextNs-q.nowNs(), q.cfg.MinDropDelayNs())
	q.scheduleDropTimer(delay)
}
