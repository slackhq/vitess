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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type codelQueueTestClock struct {
	now int64
}

func (c *codelQueueTestClock) advance(d time.Duration) {
	c.now += d.Nanoseconds()
}

func newTestCoDelQueue[T any](clock *codelQueueTestClock) *CoDelQueue[T] {
	return newCoDelQueue[T](
		CoDelConfig{
			IntervalNs:        func() int64 { return (10 * time.Millisecond).Nanoseconds() },
			InitialIntervalNs: func() int64 { return (20 * time.Millisecond).Nanoseconds() },
			TargetNs:          func() int64 { return (2 * time.Millisecond).Nanoseconds() },
			InitialTargetNs:   func() int64 { return time.Millisecond.Nanoseconds() },
			Exponent:          func() float64 { return 1 },
			MinDropDelayNs:    func() int64 { return 1 },
		},
		func() int64 { return clock.now },
		func(int64) {},
		func() {},
	)
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
