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
	"fmt"
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
                              dropping episode begins (on enqueue when ungated, or
                              when the monitor arms an episode when gated) so the
                              first fire paces drops rather than draining the
                              backlog.
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
    ungated -> on     |  |  count decayed to 1
    droppable enqueue;|  |
    gated -> monitor  |  |
    fire w/ head      |  |
    sojourn > trigger |  |
    (count seeded to  |  |
     log2(droppable)) |  |
                      v  |
        *---------------------------*
        | easing                    |       gradually relaxing
        | * dropping: false         |       (transiently true after each
        | * count: > 1 (decaying)   |        re-arm; next fire re-checks health)
        | * timer: armed            |
        *---------------------------*
              |  ^            ^
    timer     |  |            | lockedComplete() w/ sojourn < target
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

    How an episode leaves count==1 depends on DropMode (default DropSlowStart):
      * DropSlowStart (default): a droppable enqueue arms the timer immediately;
        the episode leaves count==1 only by the control-law ramp (count++ per
        drop). The timer re-arms while droppableLen>0 || count>1.
      * DropJumpStart: enqueue does NOT arm. A monitor timer (lockedRunMonitor)
        watches the oldest waiting request and arms an episode ONLY when that
        head's sojourn crosses the trigger threshold (triggerNs(), default
        interval), jumping count to log2(droppableLen). An episode ends when
        count eases back to 1 — even if a backlog remains; the monitor then
        re-checks the head and re-arms when it next crosses the trigger. The
        monitor (not lockedComplete) drives arming so a stuck queue with no
        completions can still shed.
      * DropBoth: arms on enqueue like slow-start, but while count==1 it also
        watches the head's sojourn (lockedTryJump). The episode leaves count==1
        by whichever fires first — the ramp, or a trigger crossing that jumps
        count to max(count, log2(droppableLen)). The timer wakes at the earlier
        of the two deadlines. Once count>1 the jump window closes.

    Health condition (checked each easing timer fire):
      healthy := dropping=false
      dropping is unset by lockedOnGrant() when a granted request's queue-wait
      sojourn < target, and by lockedPeek/lockedPopElem/lockedRemove/
      lockedOnGrant when droppableLen reaches 0; reset each timer fire. Note:
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
 triggered in  |  | (fully relaxed, nothing to do)
 lockedComplete|  |
 (sojourn>trig)|  v
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
	// CoDelDropMode selects how a dropping episode leaves the count==1 state —
	// i.e. how it begins shedding harder than the baseline one-drop-per-interval.
	CoDelDropMode int

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

		// EasingLogBase controls how the drop count decays each easing timer
		// fire: count -= floor(log_base(count) / base), floored at 1. A larger
		// base yields a smaller step (gentler ease-out). Defaults to 3 when
		// unset or <= 1.
		EasingLogBase func() float64

		// TriggerNs is the sojourn threshold that arms a jump in jump/both
		// modes: when the oldest waiting request's sojourn crosses it, the
		// episode jumps count to log2(droppableLen). Defaults to IntervalNs()
		// when nil or <= 0.
		TriggerNs func() int64

		// DropMode selects how a dropping episode leaves the count==1 state.
		// Nil defaults to DropSlowStart (the original always-arm behavior).
		DropMode func() CoDelDropMode

		// GraceCount is a count threshold below which the head request is not
		// actually dropped: the timer, count ramp, and easing all proceed as
		// usual, but the drop is suppressed while count < GraceCount. Jump-start
		// can still fire during the grace window, seeding count to
		// max(count, log2(droppableLen)). Nil or <= 1 disables the grace period
		// (count >= 1 always, so the head is always eligible to drop).
		GraceCount func() int
	}

	// CoDelQueue implements the CoDel (Controlled Delay) load-shedding
	// algorithm. All methods are prefixed locked* and assume the caller holds
	// the mutex, which is defined in the files for the higher-level structure
	CoDelQueue struct {
		queue        *list.List
		firstWaiting *list.Element
		dropping     bool
		dropNextNs   int64
		count        int
		droppableLen int
		// droppable indexes the droppable queue entries by priority so the
		// lowest-priority one is found in O(1) rather than an O(n) scan. Kept in
		// lockstep with droppableLen: every insert/remove pairs with a ++/--.
		droppable droppableIndex

		cfg               CoDelConfig
		nowNs             func() int64
		scheduleDropTimer func(delayNs int64)
		stopDropTimer     func()
		onPeekCleanup     func(*Request)
	}
)

const (
	// DropSlowStart arms a dropping episode on every droppable enqueue and
	// leaves count==1 only by the control-law ramp (count++ per drop). This is
	// the original always-arm CoDel behavior and the default.
	DropSlowStart CoDelDropMode = iota

	// DropJumpStart does not arm on enqueue. A monitor timer watches the oldest
	// waiting request and arms an episode only when its sojourn crosses
	// triggerNs(), jumping count straight to log2(droppableLen). The episode
	// ends when count eases back to 1, then monitoring resumes.
	DropJumpStart

	// DropBoth arms on enqueue like DropSlowStart, but while count==1 it also
	// watches the head's sojourn: the episode leaves count==1 by whichever
	// fires first — the ramp (count++) or a trigger crossing that jumps count
	// to max(count, log2(droppableLen)). Once count>1 the jump window closes
	// and the ordinary ramp/ease machine takes over.
	DropBoth
)

// String returns the canonical flag/config name for the drop mode.
func (m CoDelDropMode) String() string {
	switch m {
	case DropJumpStart:
		return "jump"
	case DropBoth:
		return "both"
	default:
		return "slow"
	}
}

// ParseDropMode maps a drop-mode string (as accepted by the loadshed-drop-mode
// flag and /debug/env) to a CoDelDropMode.
func ParseDropMode(s string) (CoDelDropMode, error) {
	switch s {
	case "slow", "slow-start":
		return DropSlowStart, nil
	case "jump", "jump-start":
		return DropJumpStart, nil
	case "both":
		return DropBoth, nil
	default:
		return DropSlowStart, fmt.Errorf("unknown drop mode %q (expected slow|jump|both)", s)
	}
}

func (e *DroppedRequestError) Error() string {
	return "request dropped by CoDel queue"
}

func newCoDelQueue(cfg CoDelConfig, nowNs func() int64, scheduleDropTimer func(delayNs int64), stopDropTimer func(), onPeekCleanup func(*Request)) *CoDelQueue {
	q := &CoDelQueue{
		queue:             list.New(),
		count:             1,
		cfg:               cfg,
		nowNs:             nowNs,
		scheduleDropTimer: scheduleDropTimer,
		stopDropTimer:     stopDropTimer,
		onPeekCleanup:     onPeekCleanup,
	}
	q.droppable.init()
	return q
}

func (q *CoDelQueue) lockedLen() int {
	return q.queue.Len()
}

func (q *CoDelQueue) lockedIsHealthy() bool {
	return !q.dropping
}

func (q *CoDelQueue) lockedEnqueue(req *Request) {
	now := q.nowNs()

	req.codelqEnqueuedAtNs = now
	req.codelqElem = q.queue.PushBack(req)

	if q.firstWaiting == nil {
		q.firstWaiting = req.codelqElem
	}

	if req.isDroppable() {
		q.droppableLen++
		q.droppable.insert(req)
		// droppableLen == 1 implies easing, so restart the interval
		if q.dropNextNs == 0 || q.droppableLen == 1 {
			// make sure we're all caught up
			if q.dropNextNs > 0 {
				q.lockedAdvance(now, func() bool { return false })
			}
			if q.armsOnEnqueue() {
				// Slow-start / both: arm on enqueue (original always-arm
				// behavior). In both mode the head-trigger deadline is folded
				// into the wake by lockedArmDropTimer while count==1. lockedArmDropTimer
				// sets dropping (droppableLen just went 0->1, so it becomes true).
				q.dropNextNs = q.lockedControlLaw(now)
				q.lockedArmDropTimer()
			} else {
				// Jump-start: monitor the head's sojourn so we arm even if
				// nothing completes. Idempotent while a timer is already pending.
				q.lockedScheduleMonitor()
			}
		}
	}
}

// lockedComplete removes a granted (undroppable) request from the queue on
// Release. The CoDel health check happens at grant (see lockedOnGrant), so
// this only unlinks the request.
func (q *CoDelQueue) lockedComplete(r *Request) {
	q.queue.Remove(r.codelqElem)
	r.codelqElem = nil
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
			q.droppable.remove(req)
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
		q.droppable.remove(req)
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
		q.droppable.remove(r)
		if q.droppableLen == 0 && q.dropping {
			q.dropping = false
		}
	}
}

func (q *CoDelQueue) lockedOnGrant(r *Request) {
	// CoDel health check, measured at grant: if this request's queue-wait
	// (now - enqueue) was under target, the system is healthy — leave the
	// dropping state. Separate from the droppableLen==0 clear below.
	if q.nowNs()-r.codelqEnqueuedAtNs < q.cfg.TargetNs() {
		q.dropping = false
	}
	if r.isDroppable() {
		q.droppable.remove(r)
		r.priority = PriorityUndroppable
		q.droppableLen--
		if q.droppableLen == 0 {
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
// element in the queue — the oldest one at the lowest priority present — or nil
// if none exists. O(1) via the droppable priority index (see droppableIndex).
func (q *CoDelQueue) lockedFindLowestPriorityDroppable() *list.Element {
	req := q.droppable.min()
	if req == nil {
		return nil
	}
	return req.codelqElem
}

// lockedRunTimer runs the CoDel drop logic. It is invoked both by the backstop
// timer and synchronously from the release/dequeue path, so shedding is driven
// as slots free rather than waiting for the (possibly late) timer to fire. The
// jump/monitor trigger logic (a head-sojourn crossing) is time-based and
// independent of drop pacing, so it runs on every call; the paced drop/ease/
// re-arm work runs only when a drop is actually due (now >= dropNextNs).
func (q *CoDelQueue) lockedRunTimer(dropFn func() bool) {
	now := q.nowNs()

	// Jump-start: while count==1 the timer is disarmed and only monitors the
	// head's sojourn. (DropBoth is armed at count==1 and handled inline below.)
	if q.dropMode() == DropJumpStart && q.count == 1 {
		q.lockedRunMonitor()
		return
	}

	// Both: a trigger crossing can jump at any time while the jump window is
	// open, independent of the ramp's pacing — so check it every call.
	if q.dropMode() == DropBoth && (q.count == 1 || q.count < q.graceCount()) && q.lockedTryJump(now) {
		return
	}

	// Paced work: only advance the drop/ease control law and re-arm when a drop
	// is actually due. dropNextNs==0 means no episode is armed and none is due.
	if q.dropNextNs == 0 || now < q.dropNextNs {
		return
	}

	q.lockedAdvance(now, dropFn)

	// Jump-start: after easing back to 1, hand off to the monitor rather than
	// re-arming. In jump-start the monitor is the sole arming authority (it arms
	// on a trigger crossing), so the episode must disarm here instead of calling
	// lockedArmDropTimer, which would re-assert dropping while a backlog remains.
	if q.dropMode() == DropJumpStart && q.count == 1 {
		q.lockedRunMonitor()
		return
	}

	if q.droppableLen > 0 || q.count > 1 {
		q.lockedArmDropTimer()
	} else {
		q.dropNextNs = 0
	}
}

// lockedAdvance runs the clock-driven core of the CoDel control law: it sheds
// every request that is due (now >= dropNextNs) while dropping, ramping count,
// then eases count back down while healthy. Every action is gated on the fresh
// `now`, so calling it is idempotent and safe outside the timer — the release
// (dequeue) path invokes it to shed stale requests in real time rather than
// waiting on the possibly-late backstop timer. It does NOT arm/disarm the timer
// or run mode-specific jump/monitor logic; those remain the timer's job.
func (q *CoDelQueue) lockedAdvance(now int64, dropFn func() bool) {
	// Step the control law per interval while a drop is due AND there is still
	// work to do: either a droppable backlog to shed, or an elevated count that
	// must ease back to 1. The count>1 term is essential for recovery — once the
	// queue drains (droppableLen==0) the ease branch still needs to run to decay
	// count and end the episode, otherwise the queue never returns to healthy.
	for now >= q.dropNextNs && (q.droppableLen > 0 || q.count > 1) {
		// Dropping: actively shed load.
		dropped := q.dropping
		if q.dropping {
			// Grace period: while count < GraceCount the head is not actually
			// dropped, but the count ramp and timer pacing proceed as usual.
			if q.count >= q.graceCount() {
				dropped = dropFn()
			}
			if dropped {
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
		// candidate for dropping. While count>1 this is identical in all modes.
		// But once count has eased to 1, jump-start mode forbids the head-check
		// from re-arming: only the monitor (on a trigger crossing) may take count
		// off 1 in jump-start. In arm-on-enqueue modes the head-check re-arms at
		// count==1 as usual.
		//
		// Judge staleness by the oldest WAITING request (lockedFirstWaiting),
		// not the raw list head (lockedPeek): a granted request can stay
		// resident in the list as UNDROPPABLE until Release, and it can never
		// be a drop candidate, so its age must not drive this decision. We
		// still call lockedPeek for its defensive cleanup side effect (it
		// purges a done-with-error request left resident by a bypassed
		// signal path) and to keep firstWaiting itself in sync.
		if q.droppableLen > 0 && (q.count > 1 || q.armsOnEnqueue()) {
			q.lockedPeek()
			if first := q.lockedFirstWaiting(); first != nil && first.codelqEnqueuedAtNs < q.dropNextNs {
				q.dropping = true
			}
		}
	}
}

// lockedTryJump escalates count when the head's sojourn has crossed the
// trigger, jumping to max(count, log2(droppableLen)). Returns true if it jumped
// (and re-armed); the caller should then return, as the jumped drop rate takes
// effect on the next fire (per the slow-start-style "arm now, drop next fire"
// semantics). Called at count==1, or anywhere in the grace window (count <
// graceCount), where count may already have ramped above 1 — hence the max.
func (q *CoDelQueue) lockedTryJump(now int64) bool {
	if q.firstWaiting == nil || q.droppableLen == 0 {
		return false
	}
	head := q.firstWaiting.Value.(*Request)
	if now-head.codelqEnqueuedAtNs < q.triggerNs() {
		return false
	}
	q.count = max(q.count, max(int(math.Log2(float64(q.droppableLen))), 1))
	// Anchor the first paced drop at the head's trigger-crossing deadline (the
	// logical moment the jump arms), not the possibly-late fire time.
	// lockedArmDropTimer sets dropping (droppableLen>0 here).
	q.dropNextNs = q.lockedControlLaw(head.codelqEnqueuedAtNs + q.triggerNs())
	q.lockedArmDropTimer()
	return true
}

// lockedScheduleMonitor arms the drop timer to fire when the current head
// (oldest not-yet-granted request) reaches the trigger threshold. No-op when
// not gated or when there is no waiting head. scheduleDropTimer is idempotent,
// so this is safe to call when a timer may already be pending.
func (q *CoDelQueue) lockedScheduleMonitor() {
	if q.dropMode() != DropJumpStart || q.firstWaiting == nil {
		return
	}
	head := q.firstWaiting.Value.(*Request)
	q.dropNextNs = head.codelqEnqueuedAtNs + q.triggerNs()
	delay := q.dropNextNs - q.nowNs()
	q.scheduleDropTimer(max(delay, q.cfg.MinDropDelayNs()))
}

// lockedRunMonitor runs on a disarmed (gated) timer fire: if the head's sojourn
// has reached the trigger, arm a dropping episode seeded from the droppable
// backlog; otherwise reschedule for the (possibly newer) head's deadline, or
// stop if nothing droppable remains.
func (q *CoDelQueue) lockedRunMonitor() {
	if q.firstWaiting == nil || q.droppableLen == 0 {
		q.dropNextNs = 0
		return
	}
	now := q.nowNs()
	head := q.firstWaiting.Value.(*Request)
	if now-head.codelqEnqueuedAtNs >= q.triggerNs() {
		q.count = max(int(math.Log2(float64(q.droppableLen))), 1)
		// Anchor the first paced drop at the head's trigger-crossing deadline
		// (the logical moment the episode arms), not the possibly-late fire
		// time. Consistent with lockedTryJump. lockedArmDropTimer sets dropping
		// (droppableLen>0 here).
		q.dropNextNs = q.lockedControlLaw(head.codelqEnqueuedAtNs + q.triggerNs())
		q.lockedArmDropTimer()
		return
	}
	// Head advanced to a younger request that has not yet crossed the trigger.
	q.lockedScheduleMonitor()
}

// lockedEaseCount returns the next drop count during easing:
// count -= floor(log_base(count) / base), floored at 1. A larger base yields a
// smaller step (gentler ease-out); base defaults to 3 when unset or <= 1.
func (q *CoDelQueue) lockedEaseCount() int {
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

// triggerNs is the sojourn threshold that arms a dropping episode. Defaults to
// the interval when TriggerNs is unset or non-positive.
func (q *CoDelQueue) triggerNs() int64 {
	if q.cfg.TriggerNs != nil {
		if t := q.cfg.TriggerNs(); t > 0 {
			return t
		}
	}
	return q.cfg.IntervalNs()
}

// dropMode reports the configured drop mode. Nil-safe; defaults to
// DropSlowStart (the original arm-on-enqueue behavior).
func (q *CoDelQueue) dropMode() CoDelDropMode {
	if q.cfg.DropMode == nil {
		return DropSlowStart
	}
	return q.cfg.DropMode()
}

// graceCount is the count threshold below which the head drop is suppressed.
// Nil-safe; defaults to 1, which never suppresses (count is always >= 1).
func (q *CoDelQueue) graceCount() int {
	if q.cfg.GraceCount == nil {
		return 1
	}
	return q.cfg.GraceCount()
}

// armsOnEnqueue reports whether a droppable enqueue arms the timer immediately
// (slow-start ramp present). True for DropSlowStart and DropBoth.
func (q *CoDelQueue) armsOnEnqueue() bool {
	return q.dropMode() != DropJumpStart
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
	// Mark the episode active for this armed interval; the next timer fire
	// re-evaluates health. Called when an episode begins (slow-start enqueue, or
	// the jump-start monitor arming in lockedRunMonitor) and on each mid-episode
	// re-arm in lockedRunTimer.
	q.dropping = q.droppableLen > 0
	wake := q.dropNextNs

	// Both mode while the jump window is open (count==1, or anywhere in the
	// grace window): the episode can escalate via the ramp (at dropNextNs) OR a
	// trigger crossing (at the head's deadline). Wake at whichever comes first
	// so neither escalation is slept through.
	if q.dropMode() == DropBoth && (q.count == 1 || q.count < q.graceCount()) && q.firstWaiting != nil {
		head := q.firstWaiting.Value.(*Request)
		if headDeadline := head.codelqEnqueuedAtNs + q.triggerNs(); headDeadline < wake {
			wake = headDeadline
		}
	}

	delay := max(wake-q.nowNs(), q.cfg.MinDropDelayNs())
	q.scheduleDropTimer(delay)
}
