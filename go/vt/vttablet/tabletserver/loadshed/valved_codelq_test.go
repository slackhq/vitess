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

func newTestValvedCoDelQueue() *ValvedCoDelQueue[string] {
	return newValvedCoDelQueue[string](
		CoDelConfig{
			IntervalNs:     func() int64 { return (10 * time.Millisecond).Nanoseconds() },
			TargetNs:       func() int64 { return time.Millisecond.Nanoseconds() },
			Exponent:       func() float64 { return 1 },
			MinDropDelayNs: func() int64 { return 1 },
		},
		func() int64 { return 0 },
		func(int64) {},
		func() {},
		func() Mode { return ModeEnabled },
	)
}

func TestValvedCoDelQueueSerializesSameValve(t *testing.T) {
	q := newTestValvedCoDelQueue()

	first := q.lockedEnqueue("valve", 0)
	second := q.lockedEnqueue("valve", 0)
	other := q.lockedEnqueue("other", 0)

	require.NotNil(t, first.codelqElem)
	assert.Nil(t, second.codelqElem)
	require.NotNil(t, other.codelqElem)
	assert.Equal(t, 2, q.lockedLen())

	q.lockedDequeue(first)

	require.NotNil(t, second.codelqElem)
	assert.Equal(t, 2, q.lockedLen())
}

func TestValvedCoDelQueueEmptyValveIDBypassesValve(t *testing.T) {
	q := newTestValvedCoDelQueue()

	first := q.lockedEnqueue("", 0)
	second := q.lockedEnqueue("", 0)

	require.NotNil(t, first.codelqElem)
	require.NotNil(t, second.codelqElem)
	assert.Equal(t, 2, q.lockedLen())
}

func TestValvedCoDelQueueCancelPendingDoesNotBypassActive(t *testing.T) {
	q := newTestValvedCoDelQueue()

	active := q.lockedEnqueue("valve", 0)
	cancelled := q.lockedEnqueue("valve", 0)
	q.lockedCancel(cancelled)
	next := q.lockedEnqueue("valve", 0)

	assert.Nil(t, next.codelqElem)
	q.lockedDequeue(active)
	require.NotNil(t, next.codelqElem)
	assert.Equal(t, 1, q.lockedLen())
}

func TestValvedCoDelQueueCancelActivePromotesNext(t *testing.T) {
	q := newTestValvedCoDelQueue()

	active := q.lockedEnqueue("valve", 0)
	next := q.lockedEnqueue("valve", 0)

	q.lockedCancel(active)

	require.NotNil(t, next.codelqElem)
	assert.Equal(t, 1, q.lockedLen())
}

func TestValvedCoDelQueueDropPromotesNext(t *testing.T) {
	q := newTestValvedCoDelQueue()

	active := q.lockedEnqueue("valve", 0)
	next := q.lockedEnqueue("valve", 0)

	q.lockedDrop(active)

	require.NotNil(t, next.codelqElem)
	assert.Equal(t, 1, q.lockedLen())
	assert.Equal(t, []*Request[string]{active}, q.lockedTakePendingDrops())
}

func TestValvedCoDelQueueDifferentValvesAreIndependent(t *testing.T) {
	q := newTestValvedCoDelQueue()

	firstA := q.lockedEnqueue("a", 0)
	secondA := q.lockedEnqueue("a", 0)
	firstB := q.lockedEnqueue("b", 0)
	secondB := q.lockedEnqueue("b", 0)

	require.NotNil(t, firstA.codelqElem)
	assert.Nil(t, secondA.codelqElem)
	require.NotNil(t, firstB.codelqElem)
	assert.Nil(t, secondB.codelqElem)
	assert.Equal(t, 2, q.lockedLen())

	q.lockedDequeue(firstA)
	require.NotNil(t, secondA.codelqElem)
	assert.Nil(t, secondB.codelqElem)
}

func TestValvedCoDelQueuePreservesFIFOWithinValve(t *testing.T) {
	q := newTestValvedCoDelQueue()
	requests := make([]*Request[string], 4)
	for i := range requests {
		requests[i] = q.lockedEnqueue("valve", 0)
	}

	for i, req := range requests {
		require.Same(t, req, q.lockedPeek())
		q.lockedDequeue(req)
		if i+1 < len(requests) {
			require.NotNil(t, requests[i+1].codelqElem)
		}
	}

	assert.Zero(t, q.lockedLen())
	assert.Empty(t, q.valves)
	assert.Empty(t, q.droppablePerValve)
}

func TestValvedCoDelQueueCancelMiddlePromotesRemainingFIFO(t *testing.T) {
	q := newTestValvedCoDelQueue()
	active := q.lockedEnqueue("valve", 0)
	first := q.lockedEnqueue("valve", 0)
	middle := q.lockedEnqueue("valve", 0)
	last := q.lockedEnqueue("valve", 0)

	q.lockedCancel(middle)
	q.lockedDequeue(active)
	require.Same(t, first, q.lockedPeek())

	q.lockedDequeue(first)
	require.Same(t, last, q.lockedPeek())
	assert.Nil(t, middle.codelqElem)
}

func TestValvedCoDelQueueSkipsConsecutiveCancelledWaiters(t *testing.T) {
	q := newTestValvedCoDelQueue()
	active := q.lockedEnqueue("valve", 0)
	cancelledOne := q.lockedEnqueue("valve", 0)
	cancelledTwo := q.lockedEnqueue("valve", 0)
	next := q.lockedEnqueue("valve", 0)

	q.lockedCancel(cancelledOne)
	q.lockedCancel(cancelledTwo)
	q.lockedDequeue(active)

	require.Same(t, next, q.lockedPeek())
	assert.Equal(t, 1, q.lockedLen())
}

func TestValvedCoDelQueueAllWaitersCancelledCleansMaps(t *testing.T) {
	q := newTestValvedCoDelQueue()
	active := q.lockedEnqueue("valve", 0)
	first := q.lockedEnqueue("valve", 0)
	second := q.lockedEnqueue("valve", 0)

	q.lockedCancel(first)
	q.lockedCancel(second)
	q.lockedDequeue(active)

	assert.Zero(t, q.lockedLen())
	assert.NotContains(t, q.valves, "valve")
	assert.NotContains(t, q.droppablePerValve, "valve")
}

func TestValvedCoDelQueueCancelAllThenNewArrivalWaitsForActive(t *testing.T) {
	q := newTestValvedCoDelQueue()
	active := q.lockedEnqueue("valve", 0)
	first := q.lockedEnqueue("valve", 0)
	second := q.lockedEnqueue("valve", 0)

	q.lockedCancel(first)
	q.lockedCancel(second)
	next := q.lockedEnqueue("valve", 0)

	assert.Nil(t, next.codelqElem)
	q.lockedDequeue(active)
	require.NotNil(t, next.codelqElem)
	assert.Same(t, next, q.lockedPeek())
}

func TestValvedCoDelQueueMassCancellation(t *testing.T) {
	q := newTestValvedCoDelQueue()
	active := q.lockedEnqueue("valve", 0)
	requests := make([]*Request[string], 100)
	for i := range requests {
		requests[i] = q.lockedEnqueue("valve", 0)
	}
	for i, req := range requests {
		if i%3 != 0 {
			q.lockedCancel(req)
		}
	}

	q.lockedDequeue(active)
	for i, req := range requests {
		if i%3 != 0 {
			continue
		}
		require.Same(t, req, q.lockedPeek())
		q.lockedDequeue(req)
	}

	assert.Zero(t, q.lockedLen())
	assert.Empty(t, q.valves)
	assert.Empty(t, q.droppablePerValve)
}

func TestValvedCoDelQueuePendingDropsTransferOwnership(t *testing.T) {
	q := newTestValvedCoDelQueue()
	first := q.lockedEnqueue("first", 0)
	second := q.lockedEnqueue("second", 0)

	q.lockedDrop(first)
	q.lockedDrop(second)

	assert.Equal(t, []*Request[string]{first, second}, q.lockedTakePendingDrops())
	assert.Nil(t, q.lockedTakePendingDrops())
}

func TestValvedCoDelQueueDisabledTearsDownController(t *testing.T) {
	q := newTestValvedCoDelQueue()
	for i := range 5 {
		q.lockedEnqueue(string(rune('a'+i)), 0)
	}
	q.codelq.count = 5
	q.codelq.dropNextNs = 1
	q.codelq.dropping = true

	q.lockedRunTimerIf(false)

	assert.False(t, q.codelq.dropping)
	assert.Equal(t, 1, q.codelq.count)
	assert.Zero(t, q.codelq.dropNextNs)
	assert.Equal(t, 5, q.lockedLen())
	assert.Nil(t, q.lockedTakePendingDrops())
}
