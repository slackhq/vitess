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

// --- Dequeue tests ---

func TestCoDelQueue_Dequeue_FIFO(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, 0)
	r2 := testEnqueue(q, 0)
	r3 := testEnqueue(q, 0)

	d1 := q.lockedDequeue()
	d2 := q.lockedDequeue()
	d3 := q.lockedDequeue()

	assert.Same(t, r1, d1)
	assert.Same(t, r2, d2)
	assert.Same(t, r3, d3)
}

func TestCoDelQueue_Dequeue_SignalsNil(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, 0)
	req := q.lockedDequeue()

	require.NotNil(t, req.signaledValue)
	val := <-req.signalChan
	assert.Equal(t, grantSentinel, val)
}

func TestCoDelQueue_Dequeue_DecrementsDroppableLen(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, 0)
	testEnqueue(q, 0)
	assert.Equal(t, 2, q.droppableLen)

	q.lockedDequeue()
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

	q.lockedDequeue()

	assert.False(t, q.dropping)
}

func TestCoDelQueue_Dequeue_Empty(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	req := q.lockedDequeue()
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

func TestCoDelQueue_CurrentInterval_MinFloor(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 10 }
	q, _ := newTestQueue(cfg, clock)

	q.dropping = true
	q.count = 1000

	interval := q.lockedCurrentInterval()
	assert.Equal(t, int64(100), interval)
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

func TestCoDelQueue_EnterDroppingState_RestoresRecentCount(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	q.count = 10
	q.lastCount = 10
	q.dropNextNs = 5_000_000_000

	clock.now = 5_000_000_000 + 8_000_000_000
	q.lockedEnterDroppingState()

	assert.Equal(t, 1, q.count)
}

func TestCoDelQueue_EnterDroppingState_RestoresLargerDelta(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	q.count = 10
	q.lastCount = 5
	q.dropNextNs = 5_000_000_000

	clock.now = 5_000_000_000 + 1_000_000_000
	q.lockedEnterDroppingState()

	assert.Equal(t, 5, q.count)
	assert.Equal(t, 5, q.lastCount)
}

func TestCoDelQueue_EnterDroppingState_StaleNoRestore(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	q.count = 10
	q.lastCount = 5
	q.dropNextNs = 5_000_000_000

	clock.now = 5_000_000_000 + 17_000_000_000
	q.lockedEnterDroppingState()

	assert.Equal(t, 1, q.count)
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
	q.lockedRunScheduledDrop(dropFn)
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
	q.lockedRunScheduledDrop(dropFn)

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
	q.lockedRunScheduledDrop(dropFn)
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
		if req := q.lockedDequeue(); req != nil {
			dequeued++
		}
	}

	assert.Equal(t, enqueued, dequeued, "fast-moving queue should not drop")
}

func TestCoDelQueue_Dequeue_ReschedulesTimerAfterHealthyExit(t *testing.T) {
	clock := newTestClock()
	cfg := CoDelConfig{
		IntervalNs:     func() int64 { return 1_000_000 },
		TargetNs:       func() int64 { return 500_000 },
		Exponent:       func() float64 { return 1.0 },
		MinDropDelayNs: func() int64 { return 100 },
	}
	q, rec := newTestQueue(cfg, clock)

	clock.now = 0
	testEnqueue(q, 1)
	testEnqueue(q, 2)
	testEnqueue(q, 3)

	q.dropping = true
	q.count = 2
	q.dropNextNs = clock.now + cfg.IntervalNs()

	clock.now = 100
	rec.scheduled = false
	req := q.lockedDequeue()

	require.NotNil(t, req)
	assert.False(t, q.dropping, "should exit dropping state")
	assert.True(t, rec.scheduled, "should reschedule via callback when droppable items remain")
	assert.Greater(t, rec.delayNs, int64(0), "delay should be positive")
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
	q.lockedRunScheduledDrop(dropFn)
	assert.True(t, q.dropping, "should enter dropping state")

	clock.advance(200_000_000)
	q.lockedRunScheduledDrop(dropFn)

	dropped := enqueued - q.lockedLen()
	assert.Greater(t, dropped, 0, "slow-moving queue should drop some requests")
}
