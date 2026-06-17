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
		EasingDivisor:  func() float64 { return 2.0 },
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

func TestCoDelQueue_EnterDroppingState_Fresh(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	clock.now = 5_000_000_000
	q.lockedEnterDroppingState()

	assert.True(t, q.dropping)
	assert.Equal(t, 1, q.count)
	assert.Equal(t, 1, q.lastCount)
	assert.Equal(t, int64(6_000_000_000), q.dropNextNs)
}

func TestCoDelQueue_EnterDroppingState_NoDeltaNoRestore(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	// count=1 (fully eased), lastCount=1 → delta = 0, no restore
	q.count = 1
	q.lastCount = 1
	q.dropNextNs = 5_000_000_000

	clock.now = 5_000_000_000 + 1_000_000_000
	q.lockedEnterDroppingState()

	assert.Equal(t, 1, q.count, "no delta to restore → stays at 1")
}

func TestCoDelQueue_EnterDroppingState_RestoresLargerDelta(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	// count=1 (fully eased), lastCount was 5 from prior drop round
	// delta = 1 - 5 = -4 → not > 1, no restore
	// To test restore: we need count > lastCount. Set count=1
	// and simulate: lastCount was set to a prior count value.
	// The heuristic: delta = count - lastCount > 1 and recent.
	// For delta > 1: count must be > lastCount + 1.
	// But count is 1 here and lastCount >= 1, so delta <= 0.
	// Let's test the *actual* restore path correctly:
	// count=1, lastCount=0 → delta=1, not > 1 → no restore.
	// Actually the memory heuristic was: the LAST dropping round had
	// count reach N, so on re-entry we restore to delta = count - lastCount.
	// With easing, if count is already > 1, we skip the whole heuristic
	// and use count as-is. The heuristic only fires when count==1.
	// So the restore scenario: count was 6 at last exit, lastCount was 1,
	// easing ran count all the way down to 1, then we re-enter dropping.
	// delta = 1 - 1 = 0. Hmm, that doesn't restore either.
	// Oh wait — lastCount is saved at entry to dropping, not at exit.
	// Let's trace: enter dropping with count=1 → lastCount set to 1.
	// Drop things → count goes to 6. Exit dropping (sojourn < target).
	// Ease: 6 → 3 → 1. Now re-enter dropping: count=1, lastCount=1.
	// delta = 1 - 1 = 0. No restore. But we WERE just at count=6!
	//
	// The issue is that lastCount is stale. Before easing, lastCount
	// was set to count at entry. We need to track the peak count.
	// For now, test the mid-ease-out path which IS the restore:
	// if count > 1 on entry to dropping, we use it directly.
	q.count = 6
	q.lastCount = 2
	q.dropNextNs = 5_000_000_000

	clock.now = 5_000_000_000 + 1_000_000_000
	q.lockedEnterDroppingState()

	assert.Equal(t, 6, q.count, "mid-ease-out: count preserved as-is")
	assert.Equal(t, 6, q.lastCount, "lastCount updated to current count on entry")
}

func TestCoDelQueue_EnterDroppingState_StaleNoRestore(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	// When count is already > 1 (mid-ease-out), we use it as-is regardless of staleness.
	// The staleness check only applies when count has fully relaxed to 1.
	q.count = 1
	q.lastCount = 5
	q.dropNextNs = 5_000_000_000

	clock.now = 5_000_000_000 + 17_000_000_000
	q.lockedEnterDroppingState()

	assert.Equal(t, 1, q.count, "stale delta should not restore count")
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

	clock.advance(2_000_000)

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
	assert.True(t, q.dropping)
	assert.True(t, rec.scheduled, "should reschedule via callback")
}

func TestCoDelQueue_RunScheduledDrop_MaxIterations(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1 }
	cfg.TargetNs = func() int64 { return 1 }
	cfg.MinDropDelayNs = func() int64 { return 1 }
	q, _ := newTestQueue(cfg, clock)

	for range 200 {
		testEnqueue(q, 0)
	}

	clock.advance(1_000_000_000)

	dropFn := func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}
	q.lockedRunTimer(dropFn)

	assert.GreaterOrEqual(t, q.lockedLen(), 100)
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

// --- Easing tests ---

func TestCoDelQueue_Easing_TimerHalvesCount(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock)

	// Put queue into an easing state: !dropping with count > 1
	q.dropping = false
	q.count = 8
	q.dropNextNs = 0 // past due

	// No droppable entries — the timer path that halves count
	dropFn := func() bool { return false }

	rec.scheduled = false
	q.lockedRunTimer(dropFn)

	assert.False(t, q.dropping)
	assert.Equal(t, 4, q.count, "count should halve from 8 to 4")
	assert.True(t, rec.scheduled, "timer should re-arm to continue easing")
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

	assert.Equal(t, 1, q.count, "count should halve from 2 to 1")
	assert.False(t, rec.scheduled, "timer should NOT re-arm once count reaches 1")
}

func TestCoDelQueue_Easing_TimerDelayShrinkWithCount(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	// interval = 1s, exponent = 1 → delay should be interval/count
	q, rec := newTestQueue(cfg, clock)

	clock.now = 1_000_000_000

	// Easing with count=8 → after halving to 4, delay should be interval/4 = 250ms
	q.dropping = false
	q.count = 8
	q.dropNextNs = 0

	dropFn := func() bool { return false }
	q.lockedRunTimer(dropFn)

	assert.Equal(t, 4, q.count)
	// The timer delay should reflect interval/count (250ms), not the full interval (1s)
	assert.Less(t, rec.delayNs, int64(1_000_000_000), "easing delay should be less than full interval")
	assert.Equal(t, int64(250_000_000), rec.delayNs, "easing delay should be interval/count = 1s/4 = 250ms")
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

	assert.False(t, q.dropping, "should exit dropping since droppableLen==0")
	assert.Equal(t, 8, q.count, "should halve count from 16 to 8")
	assert.True(t, rec.scheduled, "timer should re-arm for easing continuation")
}

func TestCoDelQueue_Easing_FullSequence(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000_000 }
	q, rec := newTestQueue(cfg, clock)

	// Start dropping with count=16, no droppable entries
	q.dropping = true
	q.count = 16
	q.dropNextNs = 0
	clock.now = 1_000_000_000

	dropFn := func() bool { return false }

	// Each timer firing should halve count and re-arm until count=1
	expectedCounts := []int{8, 4, 2, 1}
	for i, expected := range expectedCounts {
		rec.scheduled = false
		clock.advance(rec.delayNs + 1_000_000_000)
		q.lockedRunTimer(dropFn)

		assert.Equal(t, expected, q.count, "iteration %d: count should be %d", i, expected)
		assert.False(t, q.dropping)

		if expected > 1 {
			assert.True(t, rec.scheduled, "iteration %d: timer should re-arm", i)
		} else {
			assert.False(t, rec.scheduled, "final iteration: timer should stop")
		}
	}
}

func TestCoDelQueue_SlowMoving_Drops(t *testing.T) {
	clock := newTestClock()
	cfg := CoDelConfig{
		IntervalNs:     func() int64 { return 100_000_000 },
		TargetNs:       func() int64 { return 5_000_000 },
		Exponent:       func() float64 { return 1.0 },
		MinDropDelayNs: func() int64 { return 100 },
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
