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
	scheduled bool
	delayNs   int64
}

func (r *testDropTimerRecorder) schedule(delayNs int64) {
	r.scheduled = true
	r.delayNs = delayNs
}

func (r *testDropTimerRecorder) stop() {
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

	testEnqueue(q, priorityUndroppable)
	assert.Equal(t, 1, q.droppableLen)
	assert.Equal(t, 2, q.lockedLen())
}

func TestCoDelQueue_Enqueue_SchedulesTimer(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, 0)
	assert.True(t, rec.scheduled)

	rec.scheduled = false
	testEnqueue(q, 0)
	assert.True(t, rec.scheduled, "callback is always called; idempotency is the caller's concern")
}

func TestCoDelQueue_Enqueue_SeedsDropNextNs(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock) // interval = 1e9, count = 1

	clock.now = 5_000_000_000
	testEnqueue(q, 0)

	// First droppable enqueue seeds dropNextNs = now + interval so the first
	// timer fire paces drops rather than draining the backlog at once.
	assert.Equal(t, int64(6_000_000_000), q.dropNextNs)
}

func TestCoDelQueue_Enqueue_SeedsDropNextNsOnce(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock) // interval = 1e9, count = 1

	clock.now = 5_000_000_000
	testEnqueue(q, 0)
	seeded := q.dropNextNs

	// A second enqueue mid-episode must not re-seed dropNextNs; the first
	// fire's pacing schedule is preserved.
	clock.now = 5_500_000_000
	testEnqueue(q, 0)
	assert.Equal(t, seeded, q.dropNextNs, "dropNextNs should only be seeded on the first droppable enqueue")
}

func TestCoDelQueue_Enqueue_UndroppableNoSchedule(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, priorityUndroppable)
	assert.False(t, rec.scheduled)
	assert.Equal(t, 0, q.droppableLen)
}

// testDequeue simulates the old lockedDequeue behavior using the new primitives:
// lockedFirstWaiting + lockedOnGrant + signal + lockedComplete.
func testDequeue(q *CoDelQueue) *Request {
	req := q.lockedFirstWaiting()
	if req == nil {
		return nil
	}
	q.lockedOnGrant(req)
	req.signal(grantSentinel)
	q.lockedComplete(req)
	return req
}

// --- FirstWaiting / Complete tests (replaces old Dequeue tests) ---

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

func TestCoDelQueue_Complete_ExitsDroppingOnTarget(t *testing.T) {
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

func TestCoDelQueue_Peek_KeepsDoneNotCancelled(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)

	r1.signal(grantSentinel)
	q.lockedOnGrant(r1)

	peeked := q.lockedPeek()
	assert.Same(t, r1, peeked)
	assert.Equal(t, 1, q.lockedLen())
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

	testEnqueue(q, priorityUndroppable)
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

	testEnqueue(q, priorityUndroppable)
	testEnqueue(q, priorityUndroppable)

	elem := q.lockedFindLowestPriorityDroppable()
	assert.Nil(t, elem)
}

func TestCoDelQueue_DropUndroppableVsInf(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, priorityUndroppable)
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

	clock.advance(1_100_000)

	dropFn := func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}
	rec.scheduled = false
	q.lockedRunTimer(dropFn)
	assert.True(t, q.lockedLen() == 1)
	assert.True(t, rec.scheduled, "should reschedule via callback")
}

func TestCoDelQueue_RunScheduledDrop_NothingDroppable(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, priorityUndroppable)
	clock.advance(2_000_000_000)

	rec.scheduled = false
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

	r1 := testEnqueue(q, priorityUndroppable)
	assert.Equal(t, 0, q.droppableLen)

	q.lockedOnGrant(r1)
	assert.Equal(t, 0, q.droppableLen)
}

func TestCoDelQueue_OnGrant_Idempotent(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)
	assert.Equal(t, 1, q.droppableLen)

	q.lockedOnGrant(r1)
	q.lockedOnGrant(r1)
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

func TestCoDelQueue_Complete_TransitionsToEasing(t *testing.T) {
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

	// Grant r1 (mark not droppable) and then complete it with a fast sojourn
	q.lockedOnGrant(r1)
	clock.now = 100
	q.lockedComplete(r1)

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

func TestCoDelQueue_Sojourn_CompletionDoesNotRecord(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock) // TargetNs = 50ms

	clock.now = 0
	r := testEnqueue(q, 0)
	testEnqueue(q, 0) // second droppable keeps droppableLen > 0
	q.dropping = true

	// Completion never records sojourn, even a fast one that would otherwise
	// clear the dropping state.
	r.codelqEnqueuedAtNs = q.nowNs() - 10*1_000_000 // 10ms < 50ms target
	q.lockedComplete(r)
	assert.True(t, q.dropping, "completion does not record sojourn")
}

// --- Easing tests ---

func TestCoDelQueue_Easing_TimerDecaysCount(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock) // default base 2

	// Put queue into an easing state: !dropping with count > 1.
	// step = floor(log2(100)/2) = floor(3.32) = 3 → 100 - 3 = 97.
	q.dropping = false
	q.count = 100
	q.dropNextNs = 0 // past due

	dropFn := func() bool { return false }

	rec.scheduled = false
	q.lockedRunTimer(dropFn)

	assert.True(t, q.dropping, "re-arm re-marks dropping (guilty until proven innocent)")
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
		q.dropping = false
		q.count = count
		q.dropNextNs = 0
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

	q.dropping = false
	q.count = 100
	q.dropNextNs = 0

	q.lockedRunTimer(func() bool { return false })

	assert.Equal(t, 99, q.count, "default base 3: floor(log3(100)/3) = floor(1.40) = 1")
}

func TestCoDelQueue_Easing_FloorsAtOne(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.EasingLogBase = func() float64 { return 2 }
	q, rec := newTestQueue(cfg, clock)

	// log2(2)=1 / 2 = 0.5 → floor 0 → step floored to 1 → 2 - 1 = 1.
	q.dropping = false
	q.count = 2
	q.dropNextNs = 0

	rec.scheduled = false
	q.lockedRunTimer(func() bool { return false })

	assert.Equal(t, 1, q.count, "count should reach the floor of 1")
	assert.False(t, rec.scheduled, "timer should NOT re-arm once count reaches 1")
}

func TestCoDelQueue_Easing_TimerStopsAtCountOne(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock)

	q.dropping = false
	q.count = 2
	q.dropNextNs = 0

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

	// Easing state with droppable entries present
	q.dropping = false
	q.count = 6
	q.dropNextNs = 0

	clock.now = 5_000_000
	testEnqueue(q, 0)
	testEnqueue(q, 0)

	dropFn := func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}

	rec.scheduled = false
	q.lockedRunTimer(dropFn)

	assert.True(t, q.dropping, "should re-enter dropping")
	assert.GreaterOrEqual(t, q.count, 6, "count should be preserved from easing, then incremented by drops")
	assert.True(t, rec.scheduled, "timer should re-arm for continued dropping")
}

func TestCoDelQueue_Easing_CompleteDoesNotResetCount(t *testing.T) {
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

	// Complete with sojourn < target → transitions to !dropping
	clock.now = 500_000
	q.lockedComplete(req)

	assert.False(t, q.dropping, "should exit dropping")
	assert.Equal(t, 10, q.count, "count should NOT be reset on transition to healthy")
}

func TestCoDelQueue_Easing_DroppingToHealthy_TimerStillFires(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000 }
	cfg.TargetNs = func() int64 { return 500_000 }
	q, rec := newTestQueue(cfg, clock)

	// Start in dropping with high count, no droppable entries
	q.dropping = true
	q.count = 16
	q.dropNextNs = 0

	// Timer fires with droppableLen==0 → should start easing
	dropFn := func() bool { return false }
	rec.scheduled = false
	q.lockedRunTimer(dropFn)

	assert.True(t, q.dropping, "re-arm re-marks dropping; next fire re-evaluates health")
	assert.Equal(t, 14, q.count, "should decay count by floor(log2(16)/2) = 2")
	assert.True(t, rec.scheduled, "timer should re-arm for easing continuation")
}

func TestCoDelQueue_Easing_FullSequence(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000_000 }
	q, rec := newTestQueue(cfg, clock)

	// Start dropping with count=16, no droppable entries. dropNextNs is seeded
	// to now so the first fire is on time; each iteration then advances by
	// exactly the scheduled delay so the timer fires on schedule.
	q.dropping = true
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
		rec.scheduled = false
		clock.advance(rec.delayNs)
		q.lockedRunTimer(dropFn)

		assert.Less(t, q.count, prev, "iteration %d: count should strictly decrease", i)
		prev = q.count

		if q.count > 1 {
			assert.True(t, q.dropping, "iteration %d: re-arm re-marks dropping", i)
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

	clock.advance(200_000_000)
	dropFn := func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}
	q.lockedRunTimer(dropFn)
	assert.True(t, q.dropping, "should enter dropping state")

	clock.advance(200_000_000)
	q.lockedRunTimer(dropFn)

	dropped := enqueued - q.lockedLen()
	assert.Greater(t, dropped, 0, "slow-moving queue should drop some requests")
}
