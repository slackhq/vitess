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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func defaultTestConfig() CoDelConfig {
	return CoDelConfig{
		IntervalNs:     func() int64 { return int64(1e9) },
		TargetNs:       func() int64 { return int64(50e6) },
		Exponent:       func() float64 { return 1.0 },
		MinDropDelayNs: func() int64 { return 100 },
		EasingLogBase:  func() float64 { return 2.0 },
	}
}

func newTestClock() *testClock {
	return &testClock{now: 0}
}

type testClock struct {
	now int64
}

func (c *testClock) advance(ns int64) {
	c.now += ns
}

func (c *testClock) nowFunc() int64 {
	return c.now
}

type testDropTimerRecorder struct {
	armed     bool
	scheduled bool
	delayNs   int64
}

// schedule is idempotent: a no-op when already armed, matching production
// scheduleDropTimer behavior.
func (r *testDropTimerRecorder) schedule(delayNs int64) {
	if r.armed {
		return
	}
	r.armed = true
	r.scheduled = true
	r.delayNs = delayNs
}

func (r *testDropTimerRecorder) stop() {
	r.armed = false
	r.scheduled = false
}

// reset models a timer fire in tests: clears both the armed flag and the
// scheduled observability flag, so the next schedule() call is detected.
func (r *testDropTimerRecorder) reset() {
	r.armed = false
	r.scheduled = false
}

func newTestQueue(cfg CoDelConfig, clock *testClock) (*CoDelQueue, *testDropTimerRecorder) {
	rec := &testDropTimerRecorder{}
	q := newCoDelQueue(cfg, clock.nowFunc, rec.schedule, rec.stop, nil)
	return q, rec
}

func testEnqueue(q *CoDelQueue, priority float64) *Request {
	req := newRequest(priority)
	q.lockedEnqueue(req)
	return req
}

// --- Enqueue tests ---

func TestCoDelQueue_Enqueue_Basic(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	assert.Equal(t, 0, q.lockedLen())

	clock.now = 1000
	req := testEnqueue(q, 0)

	assert.Equal(t, 1, q.lockedLen())
	assert.NotNil(t, req)
	assert.Equal(t, int64(1000), req.codelqEnqueuedAtNs)
	assert.NotNil(t, req.codelqElem)
}

func TestCoDelQueue_Enqueue_RecordsEnqueueTime(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	clock.now = 42_000_000
	req := testEnqueue(q, 0)

	assert.Equal(t, int64(42_000_000), req.codelqEnqueuedAtNs)
}

func TestCoDelQueue_Enqueue_DroppableLen(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, 0)
	assert.Equal(t, 1, q.droppableLen)

	testEnqueue(q, PriorityUndroppable)
	assert.Equal(t, 1, q.droppableLen)
	assert.Equal(t, 2, q.lockedLen())
}

func TestCoDelQueue_Enqueue_UndroppableNoSchedule(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, PriorityUndroppable)
	assert.False(t, rec.scheduled)
	assert.Equal(t, 0, q.droppableLen)
}

// testDequeue grants the oldest waiting request.
func testDequeue(q *CoDelQueue) *Request {
	req := q.lockedFirstWaiting()
	if req == nil {
		return nil
	}
	q.lockedOnGrant(req)
	req.signal(grantSentinel)
	return req
}

// --- FirstWaiting tests ---

func TestCoDelQueue_FirstWaiting_FIFO(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)
	r2 := testEnqueue(q, 0)
	r3 := testEnqueue(q, 0)

	d1 := testDequeue(q)
	d2 := testDequeue(q)
	d3 := testDequeue(q)

	assert.Same(t, r1, d1)
	assert.Same(t, r2, d2)
	assert.Same(t, r3, d3)
}

func TestCoDelQueue_OnGrant_DecrementsDroppableLen(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, 0)
	testEnqueue(q, 0)
	assert.Equal(t, 2, q.droppableLen)

	testDequeue(q)
	assert.Equal(t, 1, q.droppableLen)
}

func TestCoDelQueue_Grant_ExitsDroppingOnTarget(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 }
	q, _ := newTestQueue(cfg, clock)

	q.dropping = true
	q.count = 5

	clock.now = 0
	testEnqueue(q, 0)
	clock.now = 100

	testDequeue(q)

	assert.False(t, q.dropping)
}

func TestCoDelQueue_FirstWaiting_Empty(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	req := q.lockedFirstWaiting()
	assert.Nil(t, req)
}

// --- Peek tests ---

func TestCoDelQueue_Peek_Empty(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	q.dropping = true
	req := q.lockedPeek()

	assert.Nil(t, req)
	assert.False(t, q.dropping)
}

func TestCoDelQueue_Peek_ReturnsHead(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)
	testEnqueue(q, 0)

	peeked := q.lockedPeek()
	assert.Same(t, r1, peeked)
	assert.Equal(t, 2, q.lockedLen())
}

func TestCoDelQueue_Peek_CleansHeadCancelled(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)
	r2 := testEnqueue(q, 0)

	r1.signal(&DroppedRequestError{})
	q.lockedOnGrant(r1)

	peeked := q.lockedPeek()
	assert.Same(t, r2, peeked)
	assert.Equal(t, 1, q.lockedLen())
}

func TestCoDelQueue_OnGrant_EvictsFromListImmediately(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)
	require.NotNil(t, r1.codelqElem)

	r1.signal(grantSentinel)
	q.lockedOnGrant(r1)

	assert.Nil(t, r1.codelqElem)
	assert.Nil(t, q.lockedPeek())
	assert.Equal(t, 0, q.lockedLen())
}

// --- Drop tests ---

func TestCoDelQueue_FindLowestPriorityDroppable_Basic(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, 10)
	testEnqueue(q, 1)
	testEnqueue(q, 5)

	elem := q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, elem)
	dropped := q.lockedPopElem(elem, &DroppedRequestError{})
	assert.Equal(t, float64(1), dropped.priority)
	assert.Equal(t, 2, q.lockedLen())

	err := <-dropped.signalChan
	assert.IsType(t, &DroppedRequestError{}, err)
}

func TestCoDelQueue_FindLowestPriorityDroppable_ZeroInstantPick(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, 10)
	r2 := testEnqueue(q, 0)
	testEnqueue(q, 5)

	elem := q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, elem)
	assert.Same(t, r2, elem.Value.(*Request))
}

func TestCoDelQueue_DropSkipsUndroppable(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, PriorityUndroppable)
	droppable := testEnqueue(q, 5)

	elem := q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, elem)
	assert.Same(t, droppable, elem.Value.(*Request))
	q.lockedPopElem(elem, &DroppedRequestError{})
	assert.Equal(t, 1, q.lockedLen())
}

func TestCoDelQueue_DropSkipsDone(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)
	r2 := testEnqueue(q, 5)

	r1.signal(grantSentinel)
	q.lockedOnGrant(r1)

	elem := q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, elem)
	assert.Same(t, r2, elem.Value.(*Request))
}

func TestCoDelQueue_DropAllUndroppable_ReturnsNil(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, PriorityUndroppable)
	testEnqueue(q, PriorityUndroppable)

	elem := q.lockedFindLowestPriorityDroppable()
	assert.Nil(t, elem)
}

func TestCoDelQueue_DropUndroppableVsInf(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, PriorityUndroppable)
	inf := testEnqueue(q, math.Inf(1)) //nolint:modernize

	elem := q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, elem)
	assert.Same(t, inf, elem.Value.(*Request))
}

func TestCoDelQueue_DropAllInf_NoPanic(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, math.Inf(1)) //nolint:modernize
	testEnqueue(q, math.Inf(1)) //nolint:modernize

	elem := q.lockedFindLowestPriorityDroppable()
	assert.NotNil(t, elem)
}

// --- CoDel state machine tests ---

func TestCoDelQueue_IsHealthy(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	assert.True(t, q.lockedIsHealthy())

	q.dropping = true
	assert.False(t, q.lockedIsHealthy())
}

func TestCoDelQueue_ControlLaw(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	result := q.lockedControlLaw(1000)
	assert.Equal(t, int64(1000+1e9), result)
}

func TestCoDelQueue_CurrentInterval_Dropping(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	q.dropping = true
	q.count = 4

	interval := q.lockedCurrentInterval()
	assert.Equal(t, int64(250_000_000), interval)
}

// --- Scheduled drop tests ---

func TestCoDelQueue_RunScheduledDrop_EntersDropping(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000 }
	cfg.TargetNs = func() int64 { return 100_000 }
	q, rec := newTestQueue(cfg, clock)

	testEnqueue(q, 0)
	testEnqueue(q, 0)

	// Manually arm an episode (enqueue no longer arms): dropping with count>1
	// and a seeded dropNextNs. Advance just past dropNextNs so exactly one drop
	// fires before the next scheduled drop time (interval/count=500µs).
	clock.now = 1_000_000_000
	q.dropping = true
	q.count = 2
	q.dropNextNs = clock.now
	clock.advance(1)

	dropFn := func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}
	rec.reset()
	q.lockedRunTimer(dropFn)
	assert.True(t, q.lockedLen() == 1)
	assert.True(t, rec.scheduled, "should reschedule via callback")
}

func TestCoDelQueue_RunScheduledDrop_NothingDroppable(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, PriorityUndroppable)
	clock.advance(2_000_000_000)

	rec.reset()
	dropFn := func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}
	q.lockedRunTimer(dropFn)
	assert.False(t, rec.scheduled)
	assert.Equal(t, 1, q.lockedLen())
}

// --- Remove tests ---

func TestCoDelQueue_Remove_RemovesRequest(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)
	testEnqueue(q, 0)

	q.lockedRemove(r1)

	assert.Equal(t, 1, q.lockedLen())
	assert.Equal(t, 1, q.droppableLen)
}

func TestCoDelQueue_Remove_AlreadyDone(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)

	r1.signal(grantSentinel)
	q.lockedOnGrant(r1)

	q.lockedRemove(r1)
	assert.Equal(t, 0, q.lockedLen())
}

// --- OnGrant tests ---

func TestCoDelQueue_OnGrant(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)
	assert.Equal(t, 1, q.droppableLen)

	q.lockedOnGrant(r1)
	assert.Equal(t, 0, q.droppableLen)
	assert.False(t, r1.isDroppable())
}

func TestCoDelQueue_OnGrant_AlreadyNotDroppable(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, PriorityUndroppable)
	assert.Equal(t, 0, q.droppableLen)

	q.lockedOnGrant(r1)
	assert.Equal(t, 0, q.droppableLen)
}

// --- Advance head-check: resident holder vs. real backlog staleness ---

func TestCoDelQueue_Advance_ResidentGrantedStragglerDoesNotCauseExtraDrops(t *testing.T) {
	clock := newTestClock()
	cfg := CoDelConfig{
		IntervalNs:     func() int64 { return 100_000_000 }, // 100ms
		TargetNs:       func() int64 { return 20_000_000 },  // 20ms
		Exponent:       func() float64 { return 1.0 },
		MinDropDelayNs: func() int64 { return 100 },
		EasingLogBase:  func() float64 { return 3.0 },
	}
	q, _ := newTestQueue(cfg, clock)

	clock.now = 100_000_000
	straggler := testEnqueue(q, 0)
	q.lockedOnGrant(straggler)
	require.NotNil(t, straggler.codelqElem, "precondition")

	// Six fresh, healthy waiters arrive together, well after the straggler.
	clock.now = 995_000_000
	for range 6 {
		testEnqueue(q, 0)
	}
	require.Equal(t, 6, q.droppableLen)

	// Seed a catch-up scenario: dropNextNs is 3 intervals stale relative to
	// "now" (simulating a delayed backstop timer fire), dropping already
	// armed from whenever the episode originally started.
	q.dropping = true
	q.count = 1
	q.dropNextNs = 700_000_000
	clock.now = 1_000_000_000

	drops := 0
	q.lockedRunTimer(countingDropFn(q, &drops))

	assert.Equal(t, 1, drops)
	assert.Equal(t, 1, q.count)
	assert.Equal(t, int64(1_050_000_000), q.dropNextNs)
	assert.Equal(t, 5, q.droppableLen)
	assert.True(t, q.dropping, "tautology: last iteration always re-marks true")
}

// --- Integration: fast vs slow moving ---

func TestCoDelQueue_FastMoving_NoDrop(t *testing.T) {
	clock := newTestClock()
	cfg := CoDelConfig{
		IntervalNs:     func() int64 { return 100_000_000 },
		TargetNs:       func() int64 { return 5_000_000 },
		Exponent:       func() float64 { return 1.0 },
		MinDropDelayNs: func() int64 { return 100 },
		EasingLogBase:  func() float64 { return 2.0 },
	}
	q, _ := newTestQueue(cfg, clock)

	enqueued := 0
	dequeued := 0
	for range 40 {
		clock.advance(5_000_000)
		testEnqueue(q, 0)
		enqueued++

		clock.advance(4_000_000)
		if req := testDequeue(q); req != nil {
			dequeued++
		}
	}

	assert.Equal(t, enqueued, dequeued, "fast-moving queue should not drop")
}

func TestCoDelQueue_Grant_TransitionsToEasing(t *testing.T) {
	clock := newTestClock()
	cfg := CoDelConfig{
		IntervalNs:     func() int64 { return 1_000_000 },
		TargetNs:       func() int64 { return 500_000 },
		Exponent:       func() float64 { return 1.0 },
		MinDropDelayNs: func() int64 { return 100 },
		EasingLogBase:  func() float64 { return 2.0 },
	}
	q, _ := newTestQueue(cfg, clock)

	clock.now = 0
	r1 := testEnqueue(q, 1)
	testEnqueue(q, 2)
	testEnqueue(q, 3)

	q.dropping = true
	q.count = 4
	q.dropNextNs = clock.now + cfg.IntervalNs()

	// Grant r1 with a fast sojourn.
	q.lockedOnGrant(r1)

	assert.False(t, q.dropping, "should exit dropping state")
	assert.Equal(t, 4, q.count, "count preserved for easing — timer will halve it when it fires")
}

// --- Sojourn measurement tests ---
//
// Sojourn is always measured at grant (dispatch): pure queue-wait time, not
// including resource hold time. Completion (Release) never records sojourn.

func TestCoDelQueue_Sojourn_FastGrantClearsDropping(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock) // TargetNs = 50ms

	clock.now = 0
	r := testEnqueue(q, 0)
	testEnqueue(q, 0) // second droppable keeps droppableLen > 0 after the grant
	q.dropping = true

	// Grant after a short queue-wait (< target) clears dropping at grant.
	// droppableLen stays > 0, so the clear must come from the sojourn check.
	clock.now = 10 * 1_000_000 // 10ms < 50ms target
	q.lockedOnGrant(r)
	assert.False(t, q.dropping, "fast queue-wait clears dropping at grant")
}

func TestCoDelQueue_Sojourn_SlowGrantKeepsDropping(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock) // TargetNs = 50ms

	clock.now = 0
	r := testEnqueue(q, 0)
	testEnqueue(q, 0) // second droppable keeps droppableLen > 0 after the grant
	q.dropping = true

	// Grant after a long queue-wait (> target) must NOT clear dropping.
	clock.now = 100 * 1_000_000 // 100ms > 50ms target
	q.lockedOnGrant(r)
	assert.True(t, q.dropping, "slow queue-wait keeps dropping")
}

// --- Easing tests ---

func TestCoDelQueue_Easing_TimerDecaysCount(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock) // default base 2

	// Put queue into an easing state: !dropping with count > 1, armed and due.
	// step = floor(log2(100)/2) = floor(3.32) = 3 → 100 - 3 = 97.
	clock.now = 1_000_000_000
	q.dropping = false
	q.count = 100
	q.dropNextNs = clock.now // armed, due now

	dropFn := func() bool { return false }

	rec.scheduled = false
	q.lockedRunTimer(dropFn)

	assert.False(t, q.dropping, "empty droppable queue: nothing to drop, stays healthy while easing")
	assert.Equal(t, 97, q.count, "count should decay by floor(log2(100)/2) = 3")
	assert.True(t, rec.scheduled, "timer should re-arm to continue easing")
}

func TestCoDelQueue_Easing_LogBase(t *testing.T) {
	// Easing decays count by floor(log_base(count)/base) each fire; a larger
	// base yields a smaller step.
	run := func(base float64, count int) int {
		clock := newTestClock()
		cfg := defaultTestConfig()
		cfg.EasingLogBase = func() float64 { return base }
		q, _ := newTestQueue(cfg, clock)
		clock.now = 1_000_000_000
		q.dropping = false
		q.count = count
		q.dropNextNs = clock.now // armed, due now
		q.lockedRunTimer(func() bool { return false })
		return q.count
	}

	// base 2:  log2(100)=6.64 / 2 = 3.32 → floor 3 → 97.
	assert.Equal(t, 97, run(2, 100), "base 2 → floor(log2(100)/2) = 3")
	// base 10: log10(100)=2 / 10 = 0.2 → floor 0 → step floored to 1 → 99.
	assert.Equal(t, 99, run(10, 100), "base 10 → floor(log10(100)/10) = 0 → step 1")
	// base 2, larger count: log2(10000)=13.29 / 2 = 6.64 → floor 6 → 9994.
	assert.Equal(t, 9994, run(2, 10000), "base 2 → floor(log2(10000)/2) = 6")
}

func TestCoDelQueue_Easing_DefaultBase(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.EasingLogBase = nil // unset → defaults to 3
	q, _ := newTestQueue(cfg, clock)

	clock.now = 1_000_000_000
	q.dropping = false
	q.count = 100
	q.dropNextNs = clock.now // armed, due now

	q.lockedRunTimer(func() bool { return false })

	assert.Equal(t, 99, q.count, "default base 3: floor(log3(100)/3) = floor(1.40) = 1")
}

func TestCoDelQueue_Easing_FloorsAtOne(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.EasingLogBase = func() float64 { return 2 }
	q, rec := newTestQueue(cfg, clock)

	// log2(2)=1 / 2 = 0.5 → floor 0 → step floored to 1 → 2 - 1 = 1.
	clock.now = 1_000_000_000
	q.dropping = false
	q.count = 2
	q.dropNextNs = clock.now // armed, due now

	rec.scheduled = false
	q.lockedRunTimer(func() bool { return false })

	assert.Equal(t, 1, q.count, "count should reach the floor of 1")
	assert.False(t, rec.scheduled, "timer should NOT re-arm once count reaches 1")
}

func TestCoDelQueue_Easing_TimerStopsAtCountOne(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock)

	clock.now = 1_000_000_000
	q.dropping = false
	q.count = 2
	q.dropNextNs = clock.now // armed, due now

	dropFn := func() bool { return false }

	rec.scheduled = false
	q.lockedRunTimer(dropFn)

	assert.Equal(t, 1, q.count, "count should decay from 2 to 1")
	assert.False(t, rec.scheduled, "timer should NOT re-arm once count reaches 1")
}

func TestCoDelQueue_Easing_TimerDelayShrinkWithCount(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	// interval = 1s, exponent = 1 → delay should be interval/count
	q, rec := newTestQueue(cfg, clock)

	clock.now = 1_000_000_000

	// Easing with count=8 → step floor(log2(8)/2)=floor(1.5)=1 → count 7.
	// delay should be interval/7, well under the full interval.
	q.dropping = false
	q.count = 8
	q.dropNextNs = clock.now

	dropFn := func() bool { return false }
	q.lockedRunTimer(dropFn)

	assert.Equal(t, 7, q.count, "count should decay by floor(log2(8)/2) = 1")
	// The timer delay should reflect interval/count, not the full interval (1s)
	assert.Less(t, rec.delayNs, int64(1_000_000_000), "easing delay should be less than full interval")
	assert.Equal(t, int64(1_000_000_000/7), rec.delayNs, "easing delay should be interval/count = 1s/7")
}

func TestCoDelQueue_Easing_DroppableLen_ReentersDroppingWithCurrentCount(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000 }
	q, rec := newTestQueue(cfg, clock)

	clock.now = 5_000_000
	testEnqueue(q, 0)
	testEnqueue(q, 0)

	// Easing state with droppable entries present; dropNextNs = now so the
	// easing loop fires exactly once before falling behind schedule.
	q.dropping = false
	q.count = 6
	q.dropNextNs = clock.now

	dropFn := func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}

	rec.reset()
	q.lockedRunTimer(dropFn)

	assert.True(t, q.dropping, "should re-enter dropping")
	assert.GreaterOrEqual(t, q.count, 5, "count should be close to easing start (decremented by one step)")
	assert.True(t, rec.scheduled, "timer should re-arm for continued dropping")
}

func TestCoDelQueue_Easing_GrantDoesNotResetCount(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 }
	q, _ := newTestQueue(cfg, clock)

	// In dropping state with count=10
	q.dropping = true
	q.count = 10

	clock.now = 0
	req := testEnqueue(q, 0)
	q.lockedOnGrant(req)
	req.signal(grantSentinel)

	assert.False(t, q.dropping, "should exit dropping")
	assert.Equal(t, 10, q.count, "count should NOT be reset on transition to healthy")
}

func TestCoDelQueue_Easing_DroppingToHealthy_TimerStillFires(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000 }
	cfg.TargetNs = func() int64 { return 500_000 }
	q, rec := newTestQueue(cfg, clock)

	// Easing with a high count and no droppable entries, armed and due. dropping
	// is false: a prior grant met target (or the queue drained), so this interval
	// is presumed healthy and only decays count.
	clock.now = 1_000_000_000
	q.dropping = false
	q.count = 16
	q.dropNextNs = clock.now // armed, due now

	// Timer fires with droppableLen==0 → nothing to drop, so it eases.
	dropFn := func() bool { return false }
	rec.scheduled = false
	q.lockedRunTimer(dropFn)

	assert.False(t, q.dropping, "empty droppable queue: nothing to drop, eases toward healthy")
	assert.Equal(t, 14, q.count, "should decay count by floor(log2(16)/2) = 2")
	assert.True(t, rec.scheduled, "timer should re-arm for easing continuation")
}

func TestCoDelQueue_Easing_FullSequence(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000_000 }
	q, rec := newTestQueue(cfg, clock)

	// Easing from count=16, no droppable entries. dropping is false (presumed
	// healthy: a grant met target or the queue drained), so each fire only
	// decays count. dropNextNs is seeded to now so the first fire is on time;
	// each iteration then advances by exactly the scheduled delay.
	q.dropping = false
	q.count = 16
	clock.now = 1_000_000_000
	q.dropNextNs = clock.now

	dropFn := func() bool { return false }

	// Each timer firing decays count via floor(log2(count)/2) and re-arms until
	// count reaches 1, at which point the timer stops (→ idle). The exact step
	// sequence depends on the log decay; assert the invariants rather than a
	// fixed schedule.
	prev := q.count
	for i := 0; i < 100; i++ {
		rec.reset()
		clock.advance(rec.delayNs)
		q.lockedRunTimer(dropFn)

		assert.Less(t, q.count, prev, "iteration %d: count should strictly decrease", i)
		prev = q.count

		if q.count > 1 {
			assert.False(t, q.dropping, "iteration %d: empty queue eases, stays healthy", i)
			assert.True(t, rec.scheduled, "iteration %d: timer should re-arm", i)
		} else {
			assert.False(t, q.dropping, "final: count==1, not re-armed, stays healthy")
			assert.False(t, rec.scheduled, "final: timer should stop")
			break
		}
	}
	assert.Equal(t, 1, q.count, "easing should fully relax to count=1")
}

func TestCoDelQueue_TriggerNs_DefaultsToInterval(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig() // IntervalNs = 1e9, no TriggerNs
	q, _ := newTestQueue(cfg, clock)
	assert.Equal(t, int64(1e9), q.triggerNs(), "nil TriggerNs falls back to IntervalNs")

	cfg.TriggerNs = func() int64 { return 250_000_000 }
	q2, _ := newTestQueue(cfg, clock)
	assert.Equal(t, int64(250_000_000), q2.triggerNs(), "set TriggerNs is used")

	cfg.TriggerNs = func() int64 { return 0 }
	q3, _ := newTestQueue(cfg, clock)
	assert.Equal(t, int64(1e9), q3.triggerNs(), "non-positive TriggerNs falls back to IntervalNs")
}

func TestCoDelQueue_SlowMoving_Drops(t *testing.T) {
	clock := newTestClock()
	cfg := CoDelConfig{
		IntervalNs:     func() int64 { return 100_000_000 },
		TargetNs:       func() int64 { return 5_000_000 },
		Exponent:       func() float64 { return 1.0 },
		MinDropDelayNs: func() int64 { return 100 },
		EasingLogBase:  func() float64 { return 2.0 },
	}
	q, _ := newTestQueue(cfg, clock)

	enqueued := 0
	for range 20 {
		clock.advance(2_000_000)
		testEnqueue(q, 0)
		enqueued++
	}

	// Manually arm an episode (enqueue no longer arms): set dropping=true with
	// count seeded from the backlog depth so lockedRunTimer can shed load.
	clock.advance(200_000_000)
	q.dropping = true
	q.count = max(int(math.Log2(float64(q.droppableLen))), 1)
	q.dropNextNs = clock.now

	dropFn := func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}
	q.lockedRunTimer(dropFn)
	assert.True(t, q.dropping, "should remain in dropping state with backlog")

	clock.advance(200_000_000)
	q.lockedRunTimer(dropFn)

	dropped := enqueued - q.lockedLen()
	assert.Greater(t, dropped, 0, "slow-moving queue should drop some requests")
}

func TestCoDelQueue_Enqueue_DoesNotArm(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTriggerGatedConfig(), clock)

	testEnqueue(q, 0)
	testEnqueue(q, 0)
	assert.False(t, q.dropping, "gated enqueue must not enter dropping")
	assert.Equal(t, 1, q.count, "gated enqueue must not raise count")
}

func TestCoDelQueue_Grant_DoesNotArmInGatedMode(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTriggerGatedConfig(), clock)
	clock.now = 0
	r := testEnqueue(q, 0)
	testEnqueue(q, 0)
	q.lockedOnGrant(r)
	r.signal(grantSentinel)
	clock.now = 2_000_000_000 // slow sojourn > trigger
	assert.False(t, q.dropping, "grant does not arm an episode; the monitor does")
}

func TestCoDelQueue_Easing_DisarmsAtCountOneWithBacklog(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTriggerGatedConfig(), clock)

	// Easing state at count 2 with a droppable backlog whose head has NOT yet
	// crossed the trigger (enqueued at the current time), so the monitor will
	// watch rather than immediately re-arm when count reaches 1.
	clock.now = 1_000_000_000
	testEnqueue(q, 0)
	testEnqueue(q, 0)
	assert.Equal(t, 2, q.droppableLen)

	// Transition to easing: set dropping=false, count=2, armed and due.
	q.dropping = false
	q.count = 2
	q.dropNextNs = clock.now // armed, due now

	rec.reset()
	q.lockedRunTimer(func() bool { return false })

	// count eases to 1; drop episode ends despite the backlog.
	// In gated mode the monitor re-arms to watch the head, but the drop
	// episode itself is over (dropping=false, count=1) — the head has not
	// crossed the trigger yet.
	assert.Equal(t, 1, q.count)
	assert.False(t, q.dropping, "drop episode must end at count==1 even with a backlog")
}

func TestCoDelQueue_Trigger_EpisodeRunsThenDisarms(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTriggerGatedConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000_000 } // trigger = 1s
	q, rec := newTestQueue(cfg, clock)

	// Arm via the monitor: enqueue backlog, advance past the head's trigger
	// deadline, then fire the monitor timer.
	clock.now = 0
	for range 16 {
		testEnqueue(q, 0)
	}
	clock.now = 1_000_000_000 // head enqueued at 0; trigger deadline = 0 + 1s
	rec.reset()
	q.lockedRunTimer(func() bool { return false })
	require.True(t, q.dropping)
	require.Greater(t, q.count, 1)

	// Drive the real drop loop: each fire drops the lowest-priority droppable
	// until the backlog drains (droppableLen==0 → dropping=false), then easing
	// walks count back down to 1 and the episode disarms.
	dropFn := func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}
	for i := 0; i < 1000 && q.count > 1; i++ {
		rec.reset()
		clock.advance(rec.delayNs + 1_000_000_000)
		q.lockedRunTimer(dropFn)
	}
	assert.Equal(t, 1, q.count, "episode eases to count 1 after the backlog drains")
	assert.False(t, q.dropping, "episode disarms once relaxed")
}

func TestCoDelQueue_Trigger_RearmsAfterDisarm(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTriggerGatedConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000_000 }
	q, rec := newTestQueue(cfg, clock)

	// Start in disarmed resting state with a fresh droppable backlog.
	// Enqueue so the monitor is scheduled, then fire it past the trigger
	// deadline to arm a new episode.
	clock.now = 0
	testEnqueue(q, 0)
	testEnqueue(q, 0)
	clock.now = 1_000_000_000 // past the trigger deadline (0 + 1s)
	rec.reset()
	q.lockedRunTimer(func() bool { return false })
	assert.True(t, q.dropping, "a fresh monitor fire re-arms a new episode")
	assert.True(t, rec.scheduled)
}

func TestCoDelQueue_DropMode_DefaultsSlowStart(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig() // no DropMode
	q, _ := newTestQueue(cfg, clock)
	assert.Equal(t, DropSlowStart, q.dropMode(), "nil DropMode defaults to slow-start")
	assert.True(t, q.armsOnEnqueue(), "slow-start arms on enqueue")

	cfg.DropMode = func() CoDelDropMode { return DropJumpStart }
	q2, _ := newTestQueue(cfg, clock)
	assert.Equal(t, DropJumpStart, q2.dropMode())
	assert.False(t, q2.armsOnEnqueue(), "jump-start does not arm on enqueue")

	cfg.DropMode = func() CoDelDropMode { return DropBoth }
	q3, _ := newTestQueue(cfg, clock)
	assert.Equal(t, DropBoth, q3.dropMode())
	assert.True(t, q3.armsOnEnqueue(), "both arms on enqueue")
}

func TestCoDelQueue_SlowStart_EnqueueArms(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock) // slow-start (default)
	clock.now = 5_000_000_000

	testEnqueue(q, 0)
	assert.True(t, rec.scheduled, "slow-start: droppable enqueue arms the timer")
	assert.Equal(t, int64(6_000_000_000), q.dropNextNs, "slow-start: first enqueue seeds dropNextNs = now + interval")
}

func defaultTriggerGatedConfig() CoDelConfig {
	cfg := defaultTestConfig()
	cfg.DropMode = func() CoDelDropMode { return DropJumpStart }
	return cfg
}

func TestCoDelQueue_Monitor_ArmsWhenHeadCrossesTrigger(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTriggerGatedConfig(), clock) // interval=1e9 → trigger=1e9
	clock.now = 0
	for range 16 {
		testEnqueue(q, 0)
	}
	assert.True(t, rec.scheduled, "gated enqueue schedules the monitor")
	assert.False(t, q.dropping, "monitor pending, not yet armed")

	// Advance to the head's trigger deadline and fire.
	clock.now = 1_000_000_000
	rec.reset()
	dropFn := func() bool { return false }
	q.lockedRunTimer(dropFn)

	assert.True(t, q.dropping, "head past trigger arms an episode")
	assert.Equal(t, 4, q.count, "count seeded to floor(log2(16)) = 4")
	assert.True(t, rec.scheduled, "armed episode keeps a timer")
}

func TestCoDelQueue_Monitor_StuckQueueArms(t *testing.T) {
	// No completion ever happens; the monitor timer alone must arm the episode.
	clock := newTestClock()
	q, _ := newTestQueue(defaultTriggerGatedConfig(), clock)
	clock.now = 0
	for range 8 {
		testEnqueue(q, 0)
	}
	clock.now = 1_500_000_000 // well past the 1s trigger
	q.lockedRunTimer(func() bool { return false })
	assert.True(t, q.dropping, "stuck queue arms via the monitor, not completion")
	assert.Equal(t, 3, q.count, "floor(log2(8)) = 3")
}

func TestCoDelQueue_Monitor_ReschedulesWhenHeadNotRipe(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTriggerGatedConfig(), clock)
	clock.now = 0
	r1 := testEnqueue(q, 0) // head, deadline = 1e9
	clock.now = 500_000_000
	testEnqueue(q, 0) // younger, deadline = 1.5e9

	// Grant the original head so firstWaiting advances to the younger request.
	q.lockedOnGrant(r1)

	// Fire at the original head's deadline; the new head (enqueued at 0.5e9) is
	// not yet ripe at now=1e9, so the monitor must reschedule, not arm.
	clock.now = 1_000_000_000
	rec.reset()
	q.lockedRunTimer(func() bool { return false })

	assert.False(t, q.dropping, "younger head not past trigger → no arm")
	assert.True(t, rec.scheduled, "monitor reschedules for the new head's deadline")
	assert.Equal(t, int64(1_500_000_000), q.dropNextNs, "rescheduled to new head deadline")
}

func TestCoDelQueue_Monitor_StopsWhenNoDroppable(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTriggerGatedConfig(), clock)
	// Disarmed, gated, count==1, no droppable items.
	q.dropping = false
	q.count = 1
	q.dropNextNs = 12345
	rec.reset()
	q.lockedRunTimer(func() bool { return false })
	assert.False(t, q.dropping)
	assert.False(t, rec.scheduled, "nothing to monitor → no reschedule")
	assert.Equal(t, int64(0), q.dropNextNs, "dropNextNs cleared")
}

func TestCoDelQueue_Monitor_SeedFloorsAtOneForSingleItem(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTriggerGatedConfig(), clock)
	clock.now = 0
	testEnqueue(q, 0) // single droppable item

	// Advance past the trigger and fire the monitor: log2(1)=0 → count floored to 1.
	clock.now = 1_000_000_000
	q.lockedRunTimer(func() bool { return false })

	assert.True(t, q.dropping, "single over-trigger item still arms")
	assert.Equal(t, 1, q.count, "count = max(floor(log2(1)), 1) = 1")
}

func TestCoDelQueue_DropMode_FlipToSlowStartMidEpisode(t *testing.T) {
	clock := newTestClock()
	mode := DropJumpStart
	cfg := defaultTestConfig()
	cfg.DropMode = func() CoDelDropMode { return mode }
	q, rec := newTestQueue(cfg, clock)

	// Arm a jump-start episode via the monitor.
	clock.now = 0
	for range 16 {
		testEnqueue(q, 0)
	}
	clock.now = 1_000_000_000
	q.lockedRunTimer(func() bool { return false })
	require.True(t, q.dropping)
	require.Greater(t, q.count, 1)

	// Flip the mode to slow-start mid-episode and advance to the next due drop.
	// The fire must not get stuck: the re-arm tail (droppableLen>0 || count>1)
	// keeps the timer armed while a backlog remains, so dropping continues
	// coherently.
	mode = DropSlowStart
	clock.now = q.dropNextNs // advance to when the next drop is due
	rec.reset()
	q.lockedRunTimer(func() bool { return false })
	assert.True(t, rec.scheduled, "re-arm tail keeps the timer armed with a backlog")
}

// --- DropBoth tests ---

func defaultBothConfig(triggerNs int64) CoDelConfig {
	cfg := defaultTestConfig() // interval=1e9, target=50e6
	cfg.DropMode = func() CoDelDropMode { return DropBoth }
	cfg.TriggerNs = func() int64 { return triggerNs }
	return cfg
}

func TestCoDelQueue_Both_WakesAtEarlierDeadline(t *testing.T) {
	clock := newTestClock()
	// Trigger (25ms) well below interval (1s): the head's trigger deadline is
	// earlier than the ramp's next-drop deadline, so the timer must wake at it.
	q, rec := newTestQueue(defaultBothConfig(25_000_000), clock)
	clock.now = 0
	for range 16 {
		testEnqueue(q, 0)
	}
	assert.True(t, q.dropping, "both arms on enqueue like slow-start")
	assert.Equal(t, int64(1_000_000_000), q.dropNextNs, "ramp deadline = now + interval")
	assert.Equal(t, int64(25_000_000), rec.delayNs, "wake at the earlier (head trigger) deadline")
}

func TestCoDelQueue_Both_JumpsWhenTriggerFiresFirst(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultBothConfig(25_000_000), clock)
	clock.now = 0
	for range 16 {
		testEnqueue(q, 0)
	}

	// Fire at the head's trigger deadline (before any ramp drop). The episode
	// leaves count==1 via the jump, not the ramp.
	clock.now = 25_000_000
	rec.reset()
	q.lockedRunTimer(func() bool { return true })

	assert.Equal(t, 4, q.count, "jump seeds count = floor(log2(16)) = 4")
	assert.True(t, q.dropping, "still dropping after the jump")
	assert.True(t, rec.scheduled, "re-armed after the jump")
}

func TestCoDelQueue_Both_RampsWhenNoTriggerCrossing(t *testing.T) {
	clock := newTestClock()
	// Trigger (2s) above interval (1s): the ramp's next-drop deadline is earlier,
	// so the episode leaves count==1 by the ramp (count++), not the jump.
	q, _ := newTestQueue(defaultBothConfig(2_000_000_000), clock)
	clock.now = 0
	for range 16 {
		testEnqueue(q, 0)
	}

	clock.now = 1_000_000_000 // ramp deadline; head sojourn (1s) < trigger (2s)
	q.lockedRunTimer(func() bool { return true })

	assert.Equal(t, 2, q.count, "ramp escalates count 1 -> 2 (not a jump to log2)")
	assert.True(t, q.dropping)
}

func TestCoDelQueue_Both_JumpWindowClosesAfterEscalation(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultBothConfig(25_000_000), clock)
	clock.now = 0
	r := testEnqueue(q, 0) // head waiting, would be ripe at 25ms
	_ = r

	// Simulate an already-escalated episode (count > 1): the jump window is
	// closed, so arming must use the ramp deadline, ignoring the head trigger.
	q.count = 3
	q.dropNextNs = 1_000_000_000
	rec.reset()
	q.lockedArmDropTimer()
	assert.Equal(t, int64(1_000_000_000), rec.delayNs, "count>1: wake at ramp deadline, head trigger ignored")
}

// --- Grace-period tests ---

// countingDropFn returns a dropFn that records how many times it's called and
// pops the lowest-priority droppable each time.
func countingDropFn(q *CoDelQueue, calls *int) func() bool {
	return func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		*calls++
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}
}

func TestCoDelQueue_Grace_SuppressesDropWhileBelowThreshold(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig() // interval=1e9, exp=1
	cfg.GraceCount = func() int { return 3 }
	q, _ := newTestQueue(cfg, clock)

	clock.now = 1_000_000_000
	for range 8 {
		testEnqueue(q, 0)
	}
	// Armed episode at count==1; ripe to drop now.
	q.dropping = true
	q.count = 1
	q.dropNextNs = clock.now
	clock.advance(1)

	calls := 0
	q.lockedRunTimer(countingDropFn(q, &calls))

	// count(1) < grace(3): the head is NOT dropped, but the ramp still advances.
	assert.Equal(t, 0, calls, "count below grace: no actual drop")
	assert.Equal(t, 2, q.count, "count still ramps during grace")
	assert.Equal(t, 8, q.droppableLen, "nothing removed from the queue")
	assert.True(t, q.dropping, "still dropping")
}

func TestCoDelQueue_Grace_DropsOnceCountReachesThreshold(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.GraceCount = func() int { return 3 }
	q, _ := newTestQueue(cfg, clock)

	clock.now = 1_000_000_000
	for range 8 {
		testEnqueue(q, 0)
	}
	// Already ramped to the grace threshold.
	q.dropping = true
	q.count = 3
	q.dropNextNs = clock.now
	clock.advance(1)

	calls := 0
	q.lockedRunTimer(countingDropFn(q, &calls))

	assert.Equal(t, 1, calls, "count >= grace: head is dropped")
	assert.Equal(t, 4, q.count, "count ramps after the drop")
	assert.Equal(t, 7, q.droppableLen, "one request removed")
}

func TestCoDelQueue_Grace_JumpFiresDuringGraceWindow(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig() // interval=1e9, exp=1
	cfg.DropMode = func() CoDelDropMode { return DropBoth }
	cfg.GraceCount = func() int { return 10 }
	cfg.TriggerNs = func() int64 { return 25_000_000 } // 25ms
	q, _ := newTestQueue(cfg, clock)

	clock.now = 0
	for range 256 {
		testEnqueue(q, 0) // log2(256) = 8, chosen > count so the jump is observable
	}
	// Mid-grace: ramped to count=4 (< grace 10), so the jump window is open.
	q.dropping = true
	q.count = 4
	q.dropNextNs = 1_000_000_000 // ramp deadline far off (would be the wake without a jump)
	clock.now = 25_000_000       // head sojourn == trigger

	calls := 0
	q.lockedRunTimer(countingDropFn(q, &calls))

	// Jump fires during the grace window: count = max(4, floor(log2(256))=8) = 8.
	// The count change (4 -> 8) proves the jump branch ran, not the ramp.
	assert.Equal(t, 8, q.count, "jump fired mid-grace; max(count=4, log2(256)=8) = 8")
	assert.Equal(t, 0, calls, "jump re-arms; the jumped rate drops on the next fire")
	// dropNextNs is re-anchored at the head's trigger deadline (0 + 25ms) plus
	// the post-jump interval (interval/count = 1e9/8 = 125ms): 25ms + 125ms.
	assert.Equal(t, int64(150_000_000), q.dropNextNs, "next drop anchored at trigger deadline + post-jump interval")
}

func TestCoDelQueue_Grace_JumpTakesMaxOfCountAndLog(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.DropMode = func() CoDelDropMode { return DropBoth }
	cfg.GraceCount = func() int { return 20 }
	cfg.TriggerNs = func() int64 { return 25_000_000 }
	q, _ := newTestQueue(cfg, clock)

	clock.now = 0
	for range 8 {
		testEnqueue(q, 0) // log2(8) = 3
	}
	// Ramped to count=7 (> log2(8)=3) but still below grace(20): jump must keep
	// the larger current count, not reduce it to log2.
	q.dropping = true
	q.count = 7
	q.dropNextNs = 1_000_000_000
	clock.now = 25_000_000

	q.lockedRunTimer(countingDropFn(q, new(int)))
	assert.Equal(t, 7, q.count, "max(count=7, log2(8)=3) = 7 (jump never lowers count)")
}

func TestCoDelQueue_Grace_JumpBlowsThroughGraceWindow(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.DropMode = func() CoDelDropMode { return DropBoth }
	cfg.GraceCount = func() int { return 5 } // small grace
	cfg.TriggerNs = func() int64 { return 25_000_000 }
	q, _ := newTestQueue(cfg, clock)

	clock.now = 0
	for range 256 {
		testEnqueue(q, 0) // log2(256) = 8, exceeds grace(5)
	}
	// At count==1 in the grace window; a trigger crossing on a deep backlog
	// jumps count to log2(256)=8, which is ABOVE grace(5). This intentionally
	// ends suppression mid-grace: a trigger crossing is proof the queue is
	// genuinely bad, so the next fire drops at the jumped rate rather than
	// continuing to suppress.
	q.dropping = true
	q.count = 1
	q.dropNextNs = 1_000_000_000
	clock.now = 25_000_000

	calls := 0
	q.lockedRunTimer(countingDropFn(q, &calls))
	require.Equal(t, 8, q.count, "jump to log2(256)=8, above grace=5")
	assert.Equal(t, 0, calls, "jump fire re-arms; no drop on this fire")

	// Next fire: count(8) >= grace(5), so the head IS dropped (grace no longer
	// suppresses).
	clock.now = q.dropNextNs
	q.lockedRunTimer(countingDropFn(q, &calls))
	assert.Equal(t, 1, calls, "post-jump count exceeds grace, so dropping resumes")
}
