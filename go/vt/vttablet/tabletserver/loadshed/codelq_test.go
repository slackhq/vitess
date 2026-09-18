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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	codelQueueTestClock struct {
		now int64
	}

	codelQueueTimerRecorder struct {
		scheduled bool
		stopped   bool
		delayNs   int64
	}
)

func (c *codelQueueTestClock) advance(d time.Duration) {
	c.now += d.Nanoseconds()
}

func newTestCoDelQueue[T any](clock *codelQueueTestClock) *CoDelQueue[T] {
	q, _ := newTestCoDelQueueWithTimer[T](clock)
	return q
}

func newTestCoDelQueueWithTimer[T any](clock *codelQueueTestClock) (*CoDelQueue[T], *codelQueueTimerRecorder) {
	return newConfiguredCoDelQueueWithTimer[T](clock, testCoDelConfig())
}

func newConfiguredCoDelQueueWithTimer[T any](clock *codelQueueTestClock, cfg CoDelConfig) (*CoDelQueue[T], *codelQueueTimerRecorder) {
	recorder := &codelQueueTimerRecorder{}
	q := newCoDelQueue[T](
		cfg,
		func() int64 { return clock.now },
		func(delayNs int64) {
			recorder.scheduled = true
			recorder.stopped = false
			recorder.delayNs = delayNs
		},
		func() {
			recorder.scheduled = false
			recorder.stopped = true
		},
	)
	return q, recorder
}

func testCoDelConfig() CoDelConfig {
	return CoDelConfig{
		IntervalNs:        func() int64 { return (10 * time.Millisecond).Nanoseconds() },
		InitialIntervalNs: func() int64 { return (20 * time.Millisecond).Nanoseconds() },
		TargetNs:          func() int64 { return (2 * time.Millisecond).Nanoseconds() },
		InitialTargetNs:   func() int64 { return time.Millisecond.Nanoseconds() },
		Exponent:          func() float64 { return 1 },
		MinDropDelayNs:    func() int64 { return 1 },
	}
}

func TestCoDelQueueFindFallsBackToHead(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)
	first := newRequest(1, 0)
	q.lockedEnqueueIf(first, false)
	q.lockedEnqueueIf(newRequest(2, 0), false)

	assert.Same(t, first, q.lockedFind(func(value int) bool {
		return value == 3
	}))
}

func TestCoDelQueueRunTimerLimited(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)
	for i := range 6 {
		q.lockedEnqueueIf(newRequest(i, 0), true)
	}
	q.dropNextNs = 1
	q.dropping = true
	clock.advance(time.Second)

	drops := 0
	q.lockedRunTimerLimited(func() bool {
		req := q.lockedFindLowestPriorityDroppable()
		require.NotNil(t, req)
		q.lockedRemove(req)
		drops++
		return true
	}, 2)

	assert.Equal(t, 2, drops)
	assert.Equal(t, 4, q.droppableLen)
}

func TestCoDelQueueRunTimerUnlimited(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)
	for i := range 3 {
		q.lockedEnqueueIf(newRequest(i, 0), true)
	}
	q.dropNextNs = 1
	q.dropping = true
	clock.advance(time.Second)

	drops := 0
	q.lockedRunTimer(func() bool {
		req := q.lockedFindLowestPriorityDroppable()
		if req == nil {
			return false
		}
		q.lockedRemove(req)
		drops++
		return true
	})

	assert.Equal(t, 3, drops)
	assert.Zero(t, q.droppableLen)
}

func TestCoDelQueueInitialConfigOnlyAppliesAtCountOne(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)

	assert.Equal(t, time.Millisecond.Nanoseconds(), q.lockedTargetNs())
	assert.Equal(t, (20 * time.Millisecond).Nanoseconds(), q.lockedCurrentInterval())

	q.count = 2
	assert.Equal(t, (2 * time.Millisecond).Nanoseconds(), q.lockedTargetNs())
	assert.Equal(t, (5 * time.Millisecond).Nanoseconds(), q.lockedCurrentInterval())
}

func TestCoDelQueueDisableResetsController(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)
	q.lockedEnqueueIf(newRequest(1, 0), true)
	q.count = 8
	q.dropping = true

	q.lockedDisable()

	assert.False(t, q.dropping)
	assert.Zero(t, q.dropNextNs)
	assert.Equal(t, 1, q.count)
	assert.Equal(t, 1, q.lockedLen())
}

func TestCoDelQueueDropsLowestPriorityFIFO(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[string](clock)
	firstLow := newRequest("first-low", 0)
	secondLow := newRequest("second-low", 0)
	q.lockedEnqueueIf(newRequest("high", 100), false)
	q.lockedEnqueueIf(firstLow, false)
	q.lockedEnqueueIf(secondLow, false)

	require.Same(t, firstLow, q.lockedFindLowestPriorityDroppable())
	q.lockedRemove(firstLow)
	assert.Same(t, secondLow, q.lockedFindLowestPriorityDroppable())
}

func TestCoDelQueueUndroppableDoesNotArmController(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)

	q.lockedEnqueueIf(newRequest(1, PriorityUndroppable), true)

	assert.Zero(t, q.droppableLen)
	assert.Zero(t, q.dropNextNs)
	assert.False(t, q.dropping)
}

func TestCoDelQueueFastDequeueStartsEasing(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)
	req := newRequest(1, 0)
	q.lockedEnqueueIf(req, true)
	q.count = 4
	q.dropping = true

	clock.advance(500 * time.Microsecond)
	q.lockedDequeue(req)

	assert.False(t, q.dropping)
	assert.Equal(t, 4, q.count)
}

func TestCoDelQueueEnqueueRecordsStateAndArmsTimer(t *testing.T) {
	clock := &codelQueueTestClock{now: int64(42 * time.Millisecond)}
	q, recorder := newTestCoDelQueueWithTimer[int](clock)
	req := newRequest(1, 0)

	q.lockedEnqueueIf(req, true)

	assert.Equal(t, 1, q.lockedLen())
	assert.Equal(t, 1, q.droppableLen)
	assert.Equal(t, clock.now, req.codelqEnqueuedAtNs)
	assert.NotNil(t, req.codelqElem)
	assert.True(t, recorder.scheduled)
	assert.Equal(t, (20 * time.Millisecond).Nanoseconds(), recorder.delayNs)
}

func TestCoDelQueueDisabledEnqueueLeavesControllerIdle(t *testing.T) {
	clock := &codelQueueTestClock{}
	q, recorder := newTestCoDelQueueWithTimer[int](clock)

	q.lockedEnqueueIf(newRequest(1, 0), false)

	assert.Equal(t, 1, q.droppableLen)
	assert.False(t, q.dropping)
	assert.Zero(t, q.dropNextNs)
	assert.Equal(t, 1, q.count)
	assert.False(t, recorder.scheduled)
	assert.True(t, recorder.stopped)
}

func TestCoDelQueueUndroppableEnqueueDoesNotScheduleTimer(t *testing.T) {
	clock := &codelQueueTestClock{}
	q, recorder := newTestCoDelQueueWithTimer[int](clock)

	q.lockedEnqueueIf(newRequest(1, PriorityUndroppable), true)

	assert.Equal(t, 1, q.lockedLen())
	assert.Zero(t, q.droppableLen)
	assert.False(t, recorder.scheduled)
}

func TestCoDelQueuePeekAndDequeuePreserveFIFO(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)
	first := newRequest(1, 0)
	second := newRequest(2, 0)
	third := newRequest(3, 0)
	q.lockedEnqueueIf(first, false)
	q.lockedEnqueueIf(second, false)
	q.lockedEnqueueIf(third, false)

	assert.Same(t, first, q.lockedPeek())
	q.lockedDequeue(first)
	assert.Same(t, second, q.lockedPeek())
	q.lockedDequeue(second)
	assert.Same(t, third, q.lockedPeek())
	q.lockedDequeue(third)
	assert.Nil(t, q.lockedPeek())
}

func TestCoDelQueueRemoveIsIdempotent(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)
	req := newRequest(1, 0)
	q.lockedEnqueueIf(req, false)

	q.lockedRemove(req)
	q.lockedRemove(req)

	assert.Zero(t, q.lockedLen())
	assert.Zero(t, q.droppableLen)
	assert.Nil(t, req.codelqElem)
}

func TestCoDelQueueFindLowestPrioritySkipsUndroppable(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[string](clock)
	q.lockedEnqueueIf(newRequest("undroppable", PriorityUndroppable), false)
	infinite := newRequest("infinite", math.Inf(1))
	q.lockedEnqueueIf(infinite, false)

	assert.Same(t, infinite, q.lockedFindLowestPriorityDroppable())
	q.lockedRemove(infinite)
	assert.Nil(t, q.lockedFindLowestPriorityDroppable())
	assert.Equal(t, 1, q.lockedLen())
}

func TestCoDelQueueHealthReflectsDroppingState(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)

	assert.True(t, q.lockedIsHealthy())
	q.dropping = true
	assert.False(t, q.lockedIsHealthy())
}

func TestCoDelQueueControlLawUsesCurrentInterval(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)

	assert.Equal(t, int64(21*time.Millisecond), q.lockedControlLaw(time.Millisecond.Nanoseconds()))
	q.count = 4
	assert.Equal(t, int64(3500*time.Microsecond), q.lockedControlLaw(time.Millisecond.Nanoseconds()))
}

func TestCoDelQueueInitialConfigFallsBackToNormal(t *testing.T) {
	clock := &codelQueueTestClock{}
	cfg := testCoDelConfig()
	cfg.InitialTargetNs = func() int64 { return 0 }
	cfg.InitialIntervalNs = func() int64 { return 0 }
	q, _ := newConfiguredCoDelQueueWithTimer[int](clock, cfg)

	assert.Equal(t, cfg.TargetNs(), q.lockedTargetNs())
	assert.Equal(t, cfg.IntervalNs(), q.lockedCurrentInterval())
}

func TestCoDelQueueFirstDropUsesNormalInterval(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)
	first := newRequest(1, 0)
	q.lockedEnqueueIf(first, true)
	q.lockedEnqueueIf(newRequest(2, 0), true)
	require.Equal(t, (20 * time.Millisecond).Nanoseconds(), q.dropNextNs)
	clock.now = q.dropNextNs

	q.lockedRunTimer(func() bool {
		q.lockedRemove(first)
		return true
	})

	assert.Equal(t, 2, q.count)
	assert.Equal(t, (25 * time.Millisecond).Nanoseconds(), q.dropNextNs)
}

func TestCoDelQueueInitialConfigReturnsAfterEasing(t *testing.T) {
	clock := &codelQueueTestClock{}
	q, recorder := newTestCoDelQueueWithTimer[int](clock)
	first := newRequest(1, 0)
	remaining := newRequest(2, 0)
	q.lockedEnqueueIf(first, true)
	q.lockedEnqueueIf(remaining, true)
	clock.now = q.dropNextNs
	q.lockedRunTimer(func() bool {
		q.lockedRemove(first)
		return true
	})
	require.Equal(t, 2, q.count)

	q.lockedDequeue(remaining)
	clock.now = q.dropNextNs
	recorder.scheduled = false
	q.lockedRunTimer(func() bool { return false })

	assert.Equal(t, 1, q.count)
	assert.Equal(t, time.Millisecond.Nanoseconds(), q.lockedTargetNs())
	assert.Equal(t, (20 * time.Millisecond).Nanoseconds(), q.lockedCurrentInterval())
	assert.Zero(t, q.dropNextNs)
	assert.False(t, recorder.scheduled)
}

func TestCoDelQueueEnableArmsExistingBacklog(t *testing.T) {
	clock := &codelQueueTestClock{}
	q, recorder := newTestCoDelQueueWithTimer[int](clock)
	q.lockedEnqueueIf(newRequest(1, 0), false)
	recorder.stopped = false

	q.lockedEnable()

	assert.True(t, q.dropping)
	assert.Equal(t, (20 * time.Millisecond).Nanoseconds(), q.dropNextNs)
	assert.True(t, recorder.scheduled)
}

func TestCoDelQueueRunTimerWithNoDroppableRequestsStops(t *testing.T) {
	clock := &codelQueueTestClock{}
	q, recorder := newTestCoDelQueueWithTimer[int](clock)
	q.lockedEnqueueIf(newRequest(1, PriorityUndroppable), true)
	q.count = 2
	q.dropNextNs = 1
	clock.now = time.Second.Nanoseconds()

	q.lockedRunTimer(func() bool {
		require.Fail(t, "drop callback called")
		return false
	})

	assert.Equal(t, 1, q.count)
	assert.Zero(t, q.dropNextNs)
	assert.False(t, recorder.scheduled)
}

func TestCoDelQueueSlowDequeueKeepsDropping(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)
	req := newRequest(1, 0)
	q.lockedEnqueueIf(req, true)
	q.lockedEnqueueIf(newRequest(2, 0), true)
	q.dropping = true
	clock.advance(2 * time.Millisecond)

	q.lockedDequeue(req)

	assert.True(t, q.dropping)
}

func TestCoDelQueueEasingFormula(t *testing.T) {
	tests := []struct {
		name  string
		base  func() float64
		count int
		want  int
	}{
		{name: "base two", base: func() float64 { return 2 }, count: 100, want: 97},
		{name: "base ten", base: func() float64 { return 10 }, count: 100, want: 99},
		{name: "default base", count: 100, want: 99},
		{name: "invalid base", base: func() float64 { return 1 }, count: 100, want: 99},
		{name: "floor", base: func() float64 { return 2 }, count: 2, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := &codelQueueTestClock{}
			cfg := testCoDelConfig()
			cfg.EasingLogBase = tt.base
			q, _ := newConfiguredCoDelQueueWithTimer[int](clock, cfg)
			q.count = tt.count

			assert.Equal(t, tt.want, q.lockedEaseCount())
		})
	}
}

func TestCoDelQueueEasingRearmsUntilCountOne(t *testing.T) {
	clock := &codelQueueTestClock{now: time.Second.Nanoseconds()}
	q, recorder := newTestCoDelQueueWithTimer[int](clock)
	q.count = 4
	q.dropNextNs = clock.now

	q.lockedRunTimer(func() bool { return false })

	assert.Equal(t, 3, q.count)
	assert.True(t, recorder.scheduled)
	assert.Equal(t, (10 * time.Millisecond / 3).Nanoseconds(), recorder.delayNs)

	clock.now = q.dropNextNs
	recorder.scheduled = false
	q.lockedRunTimer(func() bool { return false })
	clock.now = q.dropNextNs
	recorder.scheduled = false
	q.lockedRunTimer(func() bool { return false })

	assert.Equal(t, 1, q.count)
	assert.Zero(t, q.dropNextNs)
	assert.False(t, recorder.scheduled)
}

func TestCoDelQueueTimerReentersDroppingWithBacklog(t *testing.T) {
	clock := &codelQueueTestClock{}
	q, recorder := newTestCoDelQueueWithTimer[int](clock)
	q.lockedEnqueueIf(newRequest(1, 0), true)
	q.lockedEnqueueIf(newRequest(2, 0), true)
	q.dropping = false
	q.count = 6
	clock.now = q.dropNextNs
	recorder.scheduled = false

	q.lockedRunTimer(func() bool {
		require.Fail(t, "drop callback called while easing")
		return false
	})

	assert.Equal(t, 5, q.count)
	assert.True(t, q.dropping)
	assert.True(t, recorder.scheduled)
}

func TestCoDelQueueRecoversDroppingEpisodeAfterTeardown(t *testing.T) {
	clock := &codelQueueTestClock{}
	q := newTestCoDelQueue[int](clock)
	for i := range 6 {
		q.lockedEnqueueIf(newRequest(i, 0), true)
	}
	q.dropping = false
	clock.advance(5 * time.Second)
	before := q.droppableLen

	q.lockedRunTimer(func() bool {
		req := q.lockedFindLowestPriorityDroppable()
		if req == nil {
			return false
		}
		q.lockedRemove(req)
		return true
	})

	assert.Less(t, q.droppableLen, before)
	assert.Zero(t, q.droppableLen)
}
