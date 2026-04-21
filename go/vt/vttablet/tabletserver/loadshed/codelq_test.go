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

func newTestQueue(cfg CoDelConfig, clock *testClock) *CoDelQueue {
	return newCoDelQueue(cfg, clock.nowFunc)
}

// --- Enqueue tests ---

func TestCoDelQueue_Enqueue_Basic(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	assert.Equal(t, 0, q.lockedLen())

	clock.now = 1000
	req, _, _ := q.lockedEnqueue(NewPriority(0))

	assert.Equal(t, 1, q.lockedLen())
	assert.NotNil(t, req)
	assert.Equal(t, int64(1000), req.enqueuedAt)
	assert.NotNil(t, req.elem)
}

func TestCoDelQueue_Enqueue_RecordsEnqueueTime(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	clock.now = 42_000_000
	req, _, _ := q.lockedEnqueue(NewPriority(0))

	assert.Equal(t, int64(42_000_000), req.enqueuedAt)
}

func TestCoDelQueue_Enqueue_DroppableLen(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	// droppable request increments
	q.lockedEnqueue(NewPriority(0))
	assert.Equal(t, 1, q.droppableLen)

	// undroppable does not
	q.lockedEnqueue(nil)
	assert.Equal(t, 1, q.droppableLen)
	assert.Equal(t, 2, q.lockedLen())
}

func TestCoDelQueue_Enqueue_NeedSchedule(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	// first droppable request: needs schedule
	_, needSchedule, _ := q.lockedEnqueue(NewPriority(0))
	assert.True(t, needSchedule)

	// second droppable: no schedule needed (already scheduled)
	_, needSchedule, _ = q.lockedEnqueue(NewPriority(0))
	assert.False(t, needSchedule)
}

func TestCoDelQueue_Enqueue_UndroppableNoSchedule(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	// undroppable request: no schedule needed
	_, needSchedule, _ := q.lockedEnqueue(nil)
	assert.False(t, needSchedule)
	assert.Equal(t, 0, q.droppableLen)
}

// --- Dequeue tests ---

func TestCoDelQueue_Dequeue_FIFO(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	r1, _, _ := q.lockedEnqueue(NewPriority(0))
	r2, _, _ := q.lockedEnqueue(NewPriority(0))
	r3, _, _ := q.lockedEnqueue(NewPriority(0))

	d1 := q.lockedDequeue()
	d2 := q.lockedDequeue()
	d3 := q.lockedDequeue()

	assert.Same(t, r1, d1)
	assert.Same(t, r2, d2)
	assert.Same(t, r3, d3)
}

func TestCoDelQueue_Dequeue_SignalsNil(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	q.lockedEnqueue(NewPriority(0))
	req := q.lockedDequeue()

	require.True(t, req.isDone())
	err := <-req.done
	assert.NoError(t, err)
}

func TestCoDelQueue_Dequeue_DecrementsDroppableLen(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	q.lockedEnqueue(NewPriority(0))
	q.lockedEnqueue(NewPriority(0))
	assert.Equal(t, 2, q.droppableLen)

	q.lockedDequeue()
	assert.Equal(t, 1, q.droppableLen)
}

func TestCoDelQueue_Dequeue_ExitsDroppingOnTarget(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 } // 1ms target
	q := newTestQueue(cfg, clock)

	// force into dropping state
	q.dropping = true
	q.count = 5

	// enqueue at time 0, dequeue at time 100ns (well below 1ms target)
	clock.now = 0
	q.lockedEnqueue(NewPriority(0))
	clock.now = 100

	q.lockedDequeue()

	assert.False(t, q.dropping)
}

func TestCoDelQueue_Dequeue_Empty(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	req := q.lockedDequeue()
	assert.Nil(t, req)
}

// --- Peek tests ---

func TestCoDelQueue_Peek_Empty(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	// set dropping to verify empty queue resets it
	q.dropping = true
	req := q.lockedPeek()

	assert.Nil(t, req)
	assert.False(t, q.dropping)
}

func TestCoDelQueue_Peek_ReturnsHead(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	r1, _, _ := q.lockedEnqueue(NewPriority(0))
	q.lockedEnqueue(NewPriority(0))

	peeked := q.lockedPeek()
	assert.Same(t, r1, peeked)
	assert.Equal(t, 2, q.lockedLen()) // peek doesn't remove
}

func TestCoDelQueue_Peek_CleansHeadCancelled(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	r1, _, _ := q.lockedEnqueue(NewPriority(0))
	r2, _, _ := q.lockedEnqueue(NewPriority(0))

	// simulate r1 being cancelled
	r1.signal(&DroppedRequestError{})
	r1.droppable = false
	q.droppableLen--

	peeked := q.lockedPeek()
	assert.Same(t, r2, peeked)
	assert.Equal(t, 1, q.lockedLen()) // r1 was removed
}

func TestCoDelQueue_Peek_KeepsDoneNotCancelled(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	r1, _, _ := q.lockedEnqueue(NewPriority(0))

	// r1 is done (granted) but not cancelled — peek should keep it.
	// In the lock flow, the holder's request is at the head with done written.
	r1.signal(nil)
	r1.droppable = false
	q.droppableLen--

	peeked := q.lockedPeek()
	assert.Same(t, r1, peeked)
	assert.Equal(t, 1, q.lockedLen())
}

// --- Drop tests ---

func TestCoDelQueue_DropLowestPriority_Basic(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	q.lockedEnqueue(NewPriority(10))
	q.lockedEnqueue(NewPriority(1))
	q.lockedEnqueue(NewPriority(5))

	dropped := q.lockedDropLowestPriority()
	require.NotNil(t, dropped)
	assert.Equal(t, float64(1), *dropped.priority)
	assert.Equal(t, 2, q.lockedLen())

	err := <-dropped.done
	assert.IsType(t, &DroppedRequestError{}, err)
}

func TestCoDelQueue_DropLowestPriority_ZeroInstantPick(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	q.lockedEnqueue(NewPriority(10))
	r2, _, _ := q.lockedEnqueue(NewPriority(0))
	q.lockedEnqueue(NewPriority(5))

	dropped := q.lockedDropLowestPriority()
	assert.Same(t, r2, dropped)
}

func TestCoDelQueue_DropSkipsUndroppable(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	q.lockedEnqueue(nil) // undroppable
	droppable, _, _ := q.lockedEnqueue(NewPriority(5))

	dropped := q.lockedDropLowestPriority()
	assert.Same(t, droppable, dropped)
	assert.Equal(t, 1, q.lockedLen()) // undroppable remains
}

func TestCoDelQueue_DropSkipsDone(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	r1, _, _ := q.lockedEnqueue(NewPriority(0))
	r2, _, _ := q.lockedEnqueue(NewPriority(5))

	// mark r1 as done (already granted, still in queue)
	r1.signal(nil)
	r1.droppable = false
	q.droppableLen--

	dropped := q.lockedDropLowestPriority()
	assert.Same(t, r2, dropped)
}

func TestCoDelQueue_DropAllUndroppable_ReturnsNil(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	q.lockedEnqueue(nil)
	q.lockedEnqueue(nil)

	dropped := q.lockedDropLowestPriority()
	assert.Nil(t, dropped)
}

func TestCoDelQueue_DropUndroppableVsInf(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	q.lockedEnqueue(nil)                                   // undroppable
	inf, _, _ := q.lockedEnqueue(NewPriority(math.Inf(1))) //nolint:modernize // droppable, high priority

	dropped := q.lockedDropLowestPriority()
	assert.Same(t, inf, dropped)
}

func TestCoDelQueue_DropAllInf_NoPanic(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	q.lockedEnqueue(NewPriority(math.Inf(1))) //nolint:modernize
	q.lockedEnqueue(NewPriority(math.Inf(1))) //nolint:modernize

	dropped := q.lockedDropLowestPriority()
	assert.NotNil(t, dropped)
}

// --- CoDel state machine tests ---

func TestCoDelQueue_IsHealthy(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	assert.True(t, q.lockedIsHealthy())

	q.dropping = true
	assert.False(t, q.lockedIsHealthy())
}

func TestCoDelQueue_ControlLaw(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	// not dropping: currentInterval = interval = 1e9
	result := q.lockedControlLaw(1000)
	assert.Equal(t, int64(1000+1e9), result)
}

func TestCoDelQueue_CurrentInterval_Dropping(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	q.dropping = true
	q.count = 4

	// interval / count^exponent = 1e9 / 4^1 = 250_000_000
	interval := q.lockedCurrentInterval()
	assert.Equal(t, int64(250_000_000), interval)
}

func TestCoDelQueue_CurrentInterval_MinFloor(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 10 }
	q := newTestQueue(cfg, clock)

	q.dropping = true
	q.count = 1000 // would give interval/1000 = 0.01 -> floor at 100

	interval := q.lockedCurrentInterval()
	assert.Equal(t, int64(100), interval)
}

func TestCoDelQueue_EnterDroppingState_Fresh(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	clock.now = 5_000_000_000
	q.lockedEnterDroppingState()

	assert.True(t, q.dropping)
	assert.Equal(t, 1, q.count)
	assert.Equal(t, 1, q.lastCount)
	// dropNextNs = controlLaw(now) = now + interval = 5e9 + 1e9
	assert.Equal(t, int64(6_000_000_000), q.dropNextNs)
}

func TestCoDelQueue_EnterDroppingState_RestoresRecentCount(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	// simulate prior dropping state
	q.count = 10
	q.lastCount = 10
	q.dropNextNs = 5_000_000_000

	// re-enter within 16 * interval (16 * 1e9 = 16e9)
	clock.now = 5_000_000_000 + 8_000_000_000 // 13e9, within 16e9 of dropNextNs
	q.lockedEnterDroppingState()

	// delta = count - lastCount = 10 - 10 = 0, so delta > 1 is false
	// count should be 1 (not restored since delta <= 1)
	assert.Equal(t, 1, q.count)
}

func TestCoDelQueue_EnterDroppingState_RestoresLargerDelta(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	// prior state: count advanced beyond lastCount
	q.count = 10
	q.lastCount = 5
	q.dropNextNs = 5_000_000_000

	// re-enter within 16 * interval
	clock.now = 5_000_000_000 + 1_000_000_000
	q.lockedEnterDroppingState()

	// delta = 10 - 5 = 5, > 1, and within 16*interval
	// count = delta = 5
	assert.Equal(t, 5, q.count)
	assert.Equal(t, 5, q.lastCount)
}

func TestCoDelQueue_EnterDroppingState_StaleNoRestore(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	q.count = 10
	q.lastCount = 5
	q.dropNextNs = 5_000_000_000

	// re-enter AFTER 16 * interval (too old)
	clock.now = 5_000_000_000 + 17_000_000_000
	q.lockedEnterDroppingState()

	// stale: count = 1
	assert.Equal(t, 1, q.count)
}

// --- Scheduled drop tests ---

func TestCoDelQueue_RunScheduledDrop_EntersDropping(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000 } // 1ms
	cfg.TargetNs = func() int64 { return 100_000 }     // 0.1ms
	q := newTestQueue(cfg, clock)

	// enqueue at time 0
	q.lockedEnqueue(NewPriority(0))
	q.lockedEnqueue(NewPriority(0))

	// advance past interval
	clock.advance(2_000_000) // 2ms

	// first call enters dropping state and sets dropNextNs
	dropFn := func() bool { return q.lockedDropLowestPriority() != nil }
	reschedule, _ := q.lockedRunScheduledDrop(dropFn)
	assert.True(t, q.dropping)
	assert.True(t, reschedule, "should reschedule to actually drop later")
}

func TestCoDelQueue_RunScheduledDrop_MaxIterations(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1 } // very small
	cfg.TargetNs = func() int64 { return 1 }   // very small
	cfg.MinDropDelayNs = func() int64 { return 1 }
	q := newTestQueue(cfg, clock)

	// enqueue 200 droppable requests
	for range 200 {
		q.lockedEnqueue(NewPriority(0))
	}

	clock.advance(1_000_000_000) // way past any target

	dropFn := func() bool { return q.lockedDropLowestPriority() != nil }
	q.lockedRunScheduledDrop(dropFn)

	// at most 100 should be dropped per invocation
	assert.GreaterOrEqual(t, q.lockedLen(), 100)
}

func TestCoDelQueue_RunScheduledDrop_NothingDroppable(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	// only undroppable
	q.lockedEnqueue(nil)
	clock.advance(2_000_000_000)

	dropFn := func() bool { return q.lockedDropLowestPriority() != nil }
	reschedule, _ := q.lockedRunScheduledDrop(dropFn)
	assert.False(t, reschedule)
	assert.Equal(t, 1, q.lockedLen()) // undroppable stays
}

// --- Cancel tests ---

func TestCoDelQueue_Cancel_RemovesRequest(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	r1, _, _ := q.lockedEnqueue(NewPriority(0))
	q.lockedEnqueue(NewPriority(0))

	q.lockedCancel(r1)

	assert.Equal(t, 1, q.lockedLen())
	assert.Equal(t, 1, q.droppableLen)
}

func TestCoDelQueue_Cancel_AlreadyDone(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	r1, _, _ := q.lockedEnqueue(NewPriority(0))

	// mark as done
	r1.signal(nil)
	r1.droppable = false
	q.droppableLen--

	// cancel should still remove from list
	q.lockedCancel(r1)
	assert.Equal(t, 0, q.lockedLen())
}

// --- MarkNotDroppable tests ---

func TestCoDelQueue_MarkNotDroppable(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	r1, _, _ := q.lockedEnqueue(NewPriority(0))
	assert.Equal(t, 1, q.droppableLen)

	q.lockedMarkNotDroppable(r1)
	assert.Equal(t, 0, q.droppableLen)
	assert.False(t, r1.droppable)
}

func TestCoDelQueue_MarkNotDroppable_AlreadyNotDroppable(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	r1, _, _ := q.lockedEnqueue(nil) // undroppable
	assert.Equal(t, 0, q.droppableLen)

	q.lockedMarkNotDroppable(r1) // no-op
	assert.Equal(t, 0, q.droppableLen)
}

func TestCoDelQueue_MarkNotDroppable_Idempotent(t *testing.T) {
	clock := newTestClock()
	q := newTestQueue(defaultTestConfig(), clock)

	r1, _, _ := q.lockedEnqueue(NewPriority(0))
	assert.Equal(t, 1, q.droppableLen)

	q.lockedMarkNotDroppable(r1)
	q.lockedMarkNotDroppable(r1) // second call is no-op
	assert.Equal(t, 0, q.droppableLen)
}

// --- Integration: fast vs slow moving ---

func TestCoDelQueue_FastMoving_NoDrop(t *testing.T) {
	clock := newTestClock()
	cfg := CoDelConfig{
		IntervalNs:     func() int64 { return 100_000_000 }, // 100ms
		TargetNs:       func() int64 { return 5_000_000 },   // 5ms
		Exponent:       func() float64 { return 1.0 },
		MinDropDelayNs: func() int64 { return 100 },
	}
	q := newTestQueue(cfg, clock)

	// enqueue and dequeue quickly (sojourn < target)
	enqueued := 0
	dequeued := 0
	for range 40 {
		clock.advance(5_000_000) // 5ms between enqueues
		q.lockedEnqueue(NewPriority(0))
		enqueued++

		clock.advance(4_000_000) // 4ms processing (sojourn ~4ms < 5ms target)
		if req := q.lockedDequeue(); req != nil {
			dequeued++
		}
	}

	assert.Equal(t, enqueued, dequeued, "fast-moving queue should not drop")
}

func TestCoDelQueue_SlowMoving_Drops(t *testing.T) {
	clock := newTestClock()
	cfg := CoDelConfig{
		IntervalNs:     func() int64 { return 100_000_000 }, // 100ms
		TargetNs:       func() int64 { return 5_000_000 },   // 5ms
		Exponent:       func() float64 { return 1.0 },
		MinDropDelayNs: func() int64 { return 100 },
	}
	q := newTestQueue(cfg, clock)

	// Enqueue many items so the queue builds up.
	enqueued := 0
	for range 20 {
		clock.advance(2_000_000) // 2ms between enqueues
		q.lockedEnqueue(NewPriority(0))
		enqueued++
	}

	// Advance past one interval. First scheduled drop enters the dropping
	// state and sets dropNextNs = now + interval.
	clock.advance(200_000_000) // 200ms
	dropFn := func() bool { return q.lockedDropLowestPriority() != nil }
	reschedule, _ := q.lockedRunScheduledDrop(dropFn)
	assert.True(t, reschedule || q.dropping, "should enter dropping state")

	// Advance past dropNextNs so the second invocation actually drops.
	clock.advance(200_000_000) // another 200ms
	q.timerScheduled = false   // reset so we can track return value
	q.lockedRunScheduledDrop(dropFn)

	dropped := enqueued - q.lockedLen()
	assert.Greater(t, dropped, 0, "slow-moving queue should drop some requests")
}
