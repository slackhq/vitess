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

type (
	testRequest    = Request[struct{}]
	testCoDelQueue = CoDelQueue[struct{}]
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

// Match production timer scheduling: re-arming an armed timer is a no-op.
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

// A fired timer must be observable as a fresh schedule on the next arm.
func (r *testDropTimerRecorder) reset() {
	r.armed = false
	r.scheduled = false
}

func newTestQueue(cfg CoDelConfig, clock *testClock) (*testCoDelQueue, *testDropTimerRecorder) {
	rec := &testDropTimerRecorder{}
	q := newCoDelQueue[struct{}](cfg, clock.nowFunc, rec.schedule, rec.stop)
	return q, rec
}

func testEnqueue(q *testCoDelQueue, droppable bool) *testRequest {
	req := newRequest(struct{}{}, droppable)
	q.lockedEnqueue(req)
	return req
}

func TestCoDelQueue_Enqueue_Basic(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	assert.Equal(t, 0, q.lockedLen())

	clock.now = 1000
	req := testEnqueue(q, true)

	assert.Equal(t, 1, q.lockedLen())
	assert.NotNil(t, req)
	assert.Equal(t, int64(1000), req.codelqEnqueuedAtNs)
	assert.NotNil(t, req.codelqElem)
}

func TestCoDelQueue_Enqueue_RecordsEnqueueTime(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	clock.now = 42_000_000
	req := testEnqueue(q, true)

	assert.Equal(t, int64(42_000_000), req.codelqEnqueuedAtNs)
}

func TestCoDelQueue_Enqueue_DroppableLen(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, true)
	assert.Equal(t, 1, q.droppableLen)

	testEnqueue(q, false)
	assert.Equal(t, 1, q.droppableLen)
	assert.Equal(t, 2, q.lockedLen())
}

func TestCoDelQueue_Enqueue_UndroppableNoSchedule(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, false)
	assert.False(t, rec.scheduled)
	assert.Equal(t, 0, q.droppableLen)
}

func testDequeue(q *testCoDelQueue) *testRequest {
	req := q.lockedPeek()
	if req == nil {
		return nil
	}
	q.lockedDequeue(req)
	return req
}

func TestCoDelQueue_FirstWaiting_FIFO(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, true)
	r2 := testEnqueue(q, true)
	r3 := testEnqueue(q, true)

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

	testEnqueue(q, true)
	testEnqueue(q, true)
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
	testEnqueue(q, true)
	clock.now = 100

	testDequeue(q)

	assert.False(t, q.dropping)
}

func TestCoDelQueue_FirstWaiting_Empty(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	req := q.lockedPeek()
	assert.Nil(t, req)
}

func TestCoDelQueue_Peek_ReturnsHead(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, true)
	testEnqueue(q, true)

	peeked := q.lockedPeek()
	assert.Same(t, r1, peeked)
	assert.Equal(t, 2, q.lockedLen())
}

func TestCoDelQueue_Dequeue_EvictsFromListImmediately(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, true)
	require.NotNil(t, r1.codelqElem)

	q.lockedDequeue(r1)

	assert.Nil(t, r1.codelqElem)
	assert.Equal(t, 0, q.lockedLen())
}

func TestCoDelQueue_FindDroppable_FIFO(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	first := testEnqueue(q, true)
	testEnqueue(q, true)
	testEnqueue(q, true)

	elem := q.lockedFindDroppable()
	require.NotNil(t, elem)
	dropped := elem.Value
	q.lockedRemove(dropped)
	assert.Same(t, first, dropped)
	assert.Equal(t, 2, q.lockedLen())
}

func TestCoDelQueue_DropSkipsUndroppable(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, false)
	droppable := testEnqueue(q, true)

	elem := q.lockedFindDroppable()
	require.NotNil(t, elem)
	assert.Same(t, droppable, elem.Value)
	q.lockedRemove(droppable)
	assert.Equal(t, 1, q.lockedLen())
}

func TestCoDelQueue_DropAllUndroppable_ReturnsNil(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	testEnqueue(q, false)
	testEnqueue(q, false)

	elem := q.lockedFindDroppable()
	assert.Nil(t, elem)
}

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

func TestCoDelQueue_InitialTargetOnlyAppliesAtCountOne(t *testing.T) {
	for _, tc := range []struct {
		name         string
		count        int
		wantDropping bool
	}{
		{name: "initial", count: 1, wantDropping: false},
		{name: "normal", count: 2, wantDropping: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newTestClock()
			cfg := defaultTestConfig()
			cfg.TargetNs = func() int64 { return 50_000_000 }
			cfg.InitialTargetNs = func() int64 { return 200_000_000 }
			q, _ := newTestQueue(cfg, clock)
			q.count = tc.count

			r := testEnqueue(q, true)
			testEnqueue(q, true)
			q.dropping = true
			clock.now = 100_000_000

			q.lockedDequeue(r)

			assert.Equal(t, tc.wantDropping, q.dropping)
		})
	}
}

func TestCoDelQueue_InitialIntervalOnlyAppliesAtCountOne(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000_000 }
	cfg.InitialIntervalNs = func() int64 { return 4_000_000_000 }
	q, _ := newTestQueue(cfg, clock)

	assert.Equal(t, int64(4_000_000_000), q.lockedCurrentInterval())

	q.count = 2
	assert.Equal(t, int64(500_000_000), q.lockedCurrentInterval())

	q.count = 1
	assert.Equal(t, int64(4_000_000_000), q.lockedCurrentInterval())
}

func TestCoDelQueue_FirstDropSwitchesToNormalInterval(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 100 }
	cfg.InitialIntervalNs = func() int64 { return 1_000 }
	cfg.MinDropDelayNs = func() int64 { return 1 }
	q, rec := newTestQueue(cfg, clock)

	testEnqueue(q, true)
	testEnqueue(q, true)
	assert.Equal(t, int64(1_000), q.dropNextNs)

	clock.now = q.dropNextNs
	rec.reset()
	q.lockedRunTimer(func() bool {
		elem := q.lockedFindDroppable()
		require.NotNil(t, elem)
		q.lockedRemove(elem.Value)
		return true
	})

	assert.Equal(t, 2, q.count)
	assert.Equal(t, int64(1_050), q.dropNextNs)
}

func TestCoDelQueue_InitialConfigRestoredAfterEasingToOne(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 10 }
	cfg.InitialTargetNs = func() int64 { return 100 }
	cfg.IntervalNs = func() int64 { return 100 }
	cfg.InitialIntervalNs = func() int64 { return 1_000 }
	cfg.MinDropDelayNs = func() int64 { return 1 }
	cfg.EasingLogBase = func() float64 { return 2 }
	q, rec := newTestQueue(cfg, clock)

	testEnqueue(q, true)
	remaining := testEnqueue(q, true)
	assert.Equal(t, int64(100), q.lockedTargetNs())
	assert.Equal(t, int64(1_000), q.dropNextNs)

	clock.now = q.dropNextNs
	rec.reset()
	q.lockedRunTimer(func() bool {
		elem := q.lockedFindDroppable()
		require.NotNil(t, elem)
		q.lockedRemove(elem.Value)
		return true
	})
	assert.Equal(t, 2, q.count)
	assert.Equal(t, int64(10), q.lockedTargetNs())
	assert.Equal(t, int64(1_050), q.dropNextNs)

	q.lockedDequeue(remaining)
	clock.now = q.dropNextNs
	rec.reset()
	q.lockedRunTimer(func() bool { return false })
	assert.Equal(t, 1, q.count)
	assert.Equal(t, int64(100), q.lockedTargetNs())
	assert.Equal(t, int64(1_000), q.lockedCurrentInterval())
	assert.False(t, rec.scheduled)

	clock.now = 2_000
	testEnqueue(q, true)
	assert.Equal(t, int64(3_000), q.dropNextNs)
}

func TestCoDelQueue_InitialConfigFallsBackToNormal(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.InitialTargetNs = func() int64 { return 0 }
	cfg.InitialIntervalNs = func() int64 { return 0 }
	q, _ := newTestQueue(cfg, clock)

	assert.Equal(t, cfg.TargetNs(), q.lockedTargetNs())
	assert.Equal(t, cfg.IntervalNs(), q.lockedCurrentInterval())
}

func TestCoDelQueue_RunScheduledDrop_EntersDropping(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000 }
	cfg.TargetNs = func() int64 { return 100_000 }
	q, rec := newTestQueue(cfg, clock)

	testEnqueue(q, true)
	testEnqueue(q, true)

	// Advance far enough for exactly one drop before the next control-law deadline.
	clock.now = 1_000_000_000
	q.dropping = true
	q.count = 2
	q.dropNextNs = clock.now
	clock.advance(1)

	dropFn := func() bool {
		elem := q.lockedFindDroppable()
		if elem == nil {
			return false
		}
		q.lockedRemove(elem.Value)
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

	testEnqueue(q, false)
	clock.advance(2_000_000_000)

	rec.reset()
	dropFn := func() bool {
		elem := q.lockedFindDroppable()
		if elem == nil {
			return false
		}
		q.lockedRemove(elem.Value)
		return true
	}
	q.lockedRunTimer(dropFn)
	assert.False(t, rec.scheduled)
	assert.Equal(t, 1, q.lockedLen())
}

func TestCoDelQueue_Remove_RemovesRequest(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, true)
	testEnqueue(q, true)

	q.lockedRemove(r1)

	assert.Equal(t, 1, q.lockedLen())
	assert.Equal(t, 1, q.droppableLen)
}

func TestCoDelQueue_Remove_AlreadyDone(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, true)

	q.lockedDequeue(r1)

	q.lockedRemove(r1)
	assert.Equal(t, 0, q.lockedLen())
}

func TestCoDelQueue_Dequeue(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, true)
	assert.Equal(t, 1, q.droppableLen)

	q.lockedDequeue(r1)
	assert.Equal(t, 0, q.droppableLen)
}

func TestCoDelQueue_Dequeue_AlreadyNotDroppable(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	r1 := testEnqueue(q, false)
	assert.Equal(t, 0, q.droppableLen)

	q.lockedDequeue(r1)
	assert.Equal(t, 0, q.droppableLen)
}

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
		testEnqueue(q, true)
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
	r1 := testEnqueue(q, true)
	testEnqueue(q, true)
	testEnqueue(q, true)

	q.dropping = true
	q.count = 4
	q.dropNextNs = clock.now + cfg.IntervalNs()

	q.lockedDequeue(r1)

	assert.False(t, q.dropping, "should exit dropping state")
	assert.Equal(t, 4, q.count, "count preserved for easing — timer will halve it when it fires")
}

func TestCoDelQueue_Sojourn_FastDequeueClearsDropping(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	clock.now = 0
	r := testEnqueue(q, true)
	// Keep droppableLen nonzero so only the sojourn check can clear dropping.
	testEnqueue(q, true)
	q.dropping = true

	clock.now = 10 * 1_000_000
	q.lockedDequeue(r)
	assert.False(t, q.dropping, "fast queue-wait clears dropping at dequeue")
}

func TestCoDelQueue_Sojourn_SlowDequeueKeepsDropping(t *testing.T) {
	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	clock.now = 0
	r := testEnqueue(q, true)
	// Keep droppableLen nonzero so only the sojourn check can clear dropping.
	testEnqueue(q, true)
	q.dropping = true

	clock.now = 100 * 1_000_000
	q.lockedDequeue(r)
	assert.True(t, q.dropping, "slow queue-wait keeps dropping")
}

func TestCoDelQueue_Easing_TimerDecaysCount(t *testing.T) {
	clock := newTestClock()
	q, rec := newTestQueue(defaultTestConfig(), clock)

	clock.now = 1_000_000_000
	q.dropping = false
	q.count = 100
	q.dropNextNs = clock.now

	dropFn := func() bool { return false }

	rec.scheduled = false
	q.lockedRunTimer(dropFn)

	assert.False(t, q.dropping, "empty droppable queue: nothing to drop, stays healthy while easing")
	assert.Equal(t, 97, q.count, "count should decay by floor(log2(100)/2) = 3")
	assert.True(t, rec.scheduled, "timer should re-arm to continue easing")
}

func TestCoDelQueue_Easing_LogBase(t *testing.T) {
	run := func(base float64, count int) int {
		clock := newTestClock()
		cfg := defaultTestConfig()
		cfg.EasingLogBase = func() float64 { return base }
		q, _ := newTestQueue(cfg, clock)
		clock.now = 1_000_000_000
		q.dropping = false
		q.count = count
		q.dropNextNs = clock.now
		q.lockedRunTimer(func() bool { return false })
		return q.count
	}

	assert.Equal(t, 97, run(2, 100), "base 2 → floor(log2(100)/2) = 3")
	assert.Equal(t, 99, run(10, 100), "base 10 → floor(log10(100)/10) = 0 → step 1")
	assert.Equal(t, 9994, run(2, 10000), "base 2 → floor(log2(10000)/2) = 6")
}

func TestCoDelQueue_Easing_DefaultBase(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.EasingLogBase = nil
	q, _ := newTestQueue(cfg, clock)

	clock.now = 1_000_000_000
	q.dropping = false
	q.count = 100
	q.dropNextNs = clock.now

	q.lockedRunTimer(func() bool { return false })

	assert.Equal(t, 99, q.count, "default base 3: floor(log3(100)/3) = floor(1.40) = 1")
}

func TestCoDelQueue_Easing_FloorsAtOne(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.EasingLogBase = func() float64 { return 2 }
	q, rec := newTestQueue(cfg, clock)

	clock.now = 1_000_000_000
	q.dropping = false
	q.count = 2
	q.dropNextNs = clock.now

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
	q.dropNextNs = clock.now

	dropFn := func() bool { return false }

	rec.scheduled = false
	q.lockedRunTimer(dropFn)

	assert.Equal(t, 1, q.count, "count should decay from 2 to 1")
	assert.False(t, rec.scheduled, "timer should NOT re-arm once count reaches 1")
}

func TestCoDelQueue_Easing_TimerDelayShrinkWithCount(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	q, rec := newTestQueue(cfg, clock)

	clock.now = 1_000_000_000

	q.dropping = false
	q.count = 8
	q.dropNextNs = clock.now

	dropFn := func() bool { return false }
	q.lockedRunTimer(dropFn)

	assert.Equal(t, 7, q.count, "count should decay by floor(log2(8)/2) = 1")
	assert.Less(t, rec.delayNs, int64(1_000_000_000), "easing delay should be less than full interval")
	assert.Equal(t, int64(1_000_000_000/7), rec.delayNs, "easing delay should be interval/count = 1s/7")
}

func TestCoDelQueue_Easing_DroppableLen_ReentersDroppingWithCurrentCount(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 1_000_000 }
	q, rec := newTestQueue(cfg, clock)

	clock.now = 5_000_000
	testEnqueue(q, true)
	testEnqueue(q, true)

	// Keep the next deadline at now so this call performs one easing step.
	q.dropping = false
	q.count = 6
	q.dropNextNs = clock.now

	dropFn := func() bool {
		elem := q.lockedFindDroppable()
		if elem == nil {
			return false
		}
		q.lockedRemove(elem.Value)
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

	q.dropping = true
	q.count = 10

	clock.now = 0
	req := testEnqueue(q, true)
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

	clock.now = 1_000_000_000
	q.dropping = false
	q.count = 16
	q.dropNextNs = clock.now

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

	q.dropping = false
	q.count = 16
	clock.now = 1_000_000_000
	q.dropNextNs = clock.now

	dropFn := func() bool { return false }

	// Assert invariants because the exact sequence depends on logarithmic decay.
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
		testEnqueue(q, true)
		enqueued++
	}

	// The test bypasses normal enqueue-driven episode setup.
	clock.advance(200_000_000)
	q.dropping = true
	q.count = max(int(math.Log2(float64(q.droppableLen))), 1)
	q.dropNextNs = clock.now

	dropFn := func() bool {
		elem := q.lockedFindDroppable()
		if elem == nil {
			return false
		}
		q.lockedRemove(elem.Value)
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

	testEnqueue(q, true)
	assert.True(t, rec.scheduled, "slow-start: droppable enqueue arms the timer")
	assert.Equal(t, int64(6_000_000_000), q.dropNextNs, "slow-start: first enqueue seeds dropNextNs = now + interval")
}

func TestSnakeQueue_DequeueRemovesRequest(t *testing.T) {
	s := NewSnake[string](SnakeConfig{CoDel: defaultTestConfig()})

	_, dropped := s.Enqueue("value")
	require.Empty(t, dropped)
	dequeued, ok, dropped := s.Dequeue()
	require.True(t, ok)
	require.Equal(t, "value", dequeued)
	require.Empty(t, dropped)
	require.Equal(t, 0, s.q.lockedLen())
}

func TestSnakeQueue_CancelRemovesRequest(t *testing.T) {
	s := NewSnake[string](SnakeConfig{CoDel: defaultTestConfig()})
	req, dropped := s.Enqueue("value")
	require.Empty(t, dropped)

	cancelled := s.Cancel(req)
	require.True(t, cancelled)
	require.Equal(t, 0, s.q.lockedLen())
	cancelled = s.Cancel(req)
	require.False(t, cancelled)
}

func TestSnakeQueue_DisabledDoesNotDrop(t *testing.T) {
	config := SnakeConfig{
		CoDel: defaultTestConfig(),
		Mode:  func() Mode { return ModeOff },
	}
	s := NewSnake[struct{}](config)
	for range 6 {
		_, dropped := s.Enqueue(struct{}{})
		require.Empty(t, dropped)
	}
	s.q.dropNextNs = 1

	_, _, dropped := s.Dequeue()
	require.Empty(t, dropped)
}
