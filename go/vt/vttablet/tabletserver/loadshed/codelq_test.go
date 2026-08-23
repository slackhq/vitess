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

type testRequest = Request[struct{}]
type testCoDelQueue = CoDelQueue[struct{}]

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

// reset models a timer fire in tests: clears both the armed flag and the
// scheduled observability flag, so the next schedule() call is detected.
func (r *testDropTimerRecorder) reset() {
	r.armed = false
	r.scheduled = false
}

func newTestQueue(cfg CoDelConfig, clock *testClock) (*testCoDelQueue, *testDropTimerRecorder) {
	rec := &testDropTimerRecorder{}
	q := newCoDelQueue[struct{}](cfg, clock.nowFunc, rec.schedule)
	return q, rec
}

func testEnqueue(q *testCoDelQueue, priority float64) *testRequest {
	req := newRequest[struct{}](priority)
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

// testDequeue removes the oldest waiting request.
func testDequeue(q *testCoDelQueue) *testRequest {
	req := q.lockedFirstWaiting()
	if req == nil {
		return nil
	}
	q.lockedDequeue(req)
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

func TestCoDelQueue_Dequeue_DecrementsDroppableLen(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, 0)
	testEnqueue(q, 0)
	assert.Equal(t, 2, q.droppableLen)

	testDequeue(q)
	assert.Equal(t, 1, q.droppableLen)
}

func TestCoDelQueue_Dequeue_ExitsDroppingOnTarget(t *testing.T) {
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

func TestCoDelQueue_Dequeue_EvictsFromListImmediately(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)
	require.NotNil(t, r1.codelqElem)

	q.lockedDequeue(r1)

	assert.Nil(t, r1.codelqElem)
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
	dropped := elem.Value.(*testRequest)
	q.lockedRemove(dropped)
	assert.Equal(t, float64(1), dropped.priority)
	assert.Equal(t, 2, q.lockedLen())
	assert.False(t, dropped.queued)
}

func TestCoDelQueue_FindLowestPriorityDroppable_ZeroInstantPick(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, 10)
	r2 := testEnqueue(q, 0)
	testEnqueue(q, 5)

	elem := q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, elem)
	assert.Same(t, r2, elem.Value.(*testRequest))
}

func TestCoDelQueue_DropSkipsUndroppable(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, PriorityUndroppable)
	droppable := testEnqueue(q, 5)

	elem := q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, elem)
	assert.Same(t, droppable, elem.Value.(*testRequest))
	q.lockedRemove(droppable)
	assert.Equal(t, 1, q.lockedLen())
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
	assert.Same(t, inf, elem.Value.(*testRequest))
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

	// Seed a dropping episode with count>1 and a due dropNextNs. Advance just
	// past dropNextNs so exactly one drop fires before the next scheduled drop
	// time (interval/count=500µs).
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
		q.lockedRemove(elem.Value.(*testRequest))
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
		q.lockedRemove(elem.Value.(*testRequest))
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

	q.lockedDequeue(r1)

	q.lockedRemove(r1)
	assert.Equal(t, 0, q.lockedLen())
}

// --- Dequeue tests ---

func TestCoDelQueue_Dequeue(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)
	assert.Equal(t, 1, q.droppableLen)

	q.lockedDequeue(r1)
	assert.Equal(t, 0, q.droppableLen)
}

func TestCoDelQueue_Dequeue_AlreadyNotDroppable(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, PriorityUndroppable)
	assert.Equal(t, 0, q.droppableLen)

	q.lockedDequeue(r1)
	assert.Equal(t, 0, q.droppableLen)
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

func TestCoDelQueue_Dequeue_TransitionsToEasing(t *testing.T) {
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

	// Dequeue r1 with a fast sojourn.
	q.lockedDequeue(r1)

	assert.False(t, q.dropping, "should exit dropping state")
	assert.Equal(t, 4, q.count, "count preserved for easing — timer will halve it when it fires")
}

// --- Sojourn measurement tests ---
//
// Sojourn is always measured at dequeue: pure queue-wait time.

func TestCoDelQueue_Sojourn_FastDequeueClearsDropping(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock) // TargetNs = 50ms

	clock.now = 0
	r := testEnqueue(q, 0)
	testEnqueue(q, 0) // second droppable keeps droppableLen > 0 after dequeue
	q.dropping = true

	// Dequeue after a short queue-wait (< target) clears dropping.
	// droppableLen stays > 0, so the clear must come from the sojourn check.
	clock.now = 10 * 1_000_000 // 10ms < 50ms target
	q.lockedDequeue(r)
	assert.False(t, q.dropping, "fast queue-wait clears dropping at dequeue")
}

func TestCoDelQueue_Sojourn_SlowDequeueKeepsDropping(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock) // TargetNs = 50ms

	clock.now = 0
	r := testEnqueue(q, 0)
	testEnqueue(q, 0) // second droppable keeps droppableLen > 0 after dequeue
	q.dropping = true

	// Dequeue after a long queue-wait (> target) must NOT clear dropping.
	clock.now = 100 * 1_000_000 // 100ms > 50ms target
	q.lockedDequeue(r)
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
		q.lockedRemove(elem.Value.(*testRequest))
		return true
	}

	rec.reset()
	q.lockedRunTimer(dropFn)

	assert.True(t, q.dropping, "should re-enter dropping")
	assert.GreaterOrEqual(t, q.count, 5, "count should be close to easing start (decremented by one step)")
	assert.True(t, rec.scheduled, "timer should re-arm for continued dropping")
}

func TestCoDelQueue_Easing_DequeueDoesNotResetCount(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 }
	q, _ := newTestQueue(cfg, clock)

	// In dropping state with count=10
	q.dropping = true
	q.count = 10

	clock.now = 0
	req := testEnqueue(q, 0)
	q.lockedDequeue(req)

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
	// is false: a prior dequeue met target (or the queue drained), so this interval
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
	// healthy: a dequeue met target or the queue drained), so each fire only
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

	// Manually seed a dropping episode with the backlog depth so
	// lockedRunTimer can shed load.
	clock.advance(200_000_000)
	q.dropping = true
	q.count = max(int(math.Log2(float64(q.droppableLen))), 1)
	q.dropNextNs = clock.now

	dropFn := func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedRemove(elem.Value.(*testRequest))
		return true
	}
	q.lockedRunTimer(dropFn)
	assert.True(t, q.dropping, "should remain in dropping state with backlog")

	clock.advance(200_000_000)
	q.lockedRunTimer(dropFn)

	dropped := enqueued - q.lockedLen()
	assert.Greater(t, dropped, 0, "slow-moving queue should drop some requests")
}

func TestCoDelQueue_SlowStart_EnqueueArms(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock)
	clock.now = 5_000_000_000

	testEnqueue(q, 0)
	assert.True(t, rec.scheduled, "slow-start: droppable enqueue arms the timer")
	assert.Equal(t, int64(6_000_000_000), q.dropNextNs, "slow-start: first enqueue seeds dropNextNs = now + interval")
}

func TestSnakeQueue_DequeueRemovesRequest(t *testing.T) {
	s := NewSnake[string](SnakeConfig{CoDel: defaultTestConfig()})

	req, dropped := s.Enqueue("value", "", 0)
	require.Empty(t, dropped)
	dequeued, ok, dropped := s.Dequeue()
	require.True(t, ok)
	require.Equal(t, "value", dequeued)
	require.Empty(t, dropped)
	require.Equal(t, 0, s.q.lockedLen())
	require.False(t, req.queued)
}

func TestSnakeQueue_CancelRemovesRequest(t *testing.T) {
	s := NewSnake[string](SnakeConfig{CoDel: defaultTestConfig()})
	req, dropped := s.Enqueue("value", "", 0)
	require.Empty(t, dropped)

	cancelled, dropped := s.Cancel(req)
	require.True(t, cancelled)
	require.Empty(t, dropped)
	require.Equal(t, 0, s.q.lockedLen())
	cancelled, dropped = s.Cancel(req)
	require.False(t, cancelled)
	require.Empty(t, dropped)
}

func TestSnakeQueue_CancelRemovesValveWaiter(t *testing.T) {
	s := NewSnake[string](SnakeConfig{CoDel: defaultTestConfig()})
	first, dropped := s.Enqueue("first", "valve", 0)
	require.Empty(t, dropped)
	second, dropped := s.Enqueue("second", "valve", 0)
	require.Empty(t, dropped)

	cancelled, dropped := s.Cancel(second)
	require.True(t, cancelled)
	require.Empty(t, dropped)

	dequeued, ok, dropped := s.Dequeue()
	require.True(t, ok)
	require.Equal(t, "first", dequeued)
	require.Empty(t, dropped)
	dequeued, ok, dropped = s.Dequeue()
	require.False(t, ok)
	require.Empty(t, dequeued)
	require.Empty(t, dropped)
	require.False(t, first.queued)
}

func TestSnakeQueue_DisabledDoesNotDrop(t *testing.T) {
	config := SnakeConfig{
		CoDel:               defaultTestConfig(),
		LoadsheddingAllowed: func() bool { return false },
	}
	s := NewSnake[struct{}](config)
	for range 6 {
		_, dropped := s.Enqueue(struct{}{}, "", 0)
		require.Empty(t, dropped)
	}
	s.q.codelq.dropNextNs = 1

	_, _, dropped := s.Dequeue()
	require.Empty(t, dropped)
}
