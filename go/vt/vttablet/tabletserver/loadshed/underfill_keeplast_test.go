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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnake_UnderfillCount_EmptyQueue: releasing a slot with no waiter to grant
// increments the underfill counter.
func TestSnake_UnderfillCount_EmptyQueue(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 1 }
	s := NewSnake(cfg)

	u, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	require.NoError(t, u.Release())
	assert.Equal(t, int64(1), s.UnderfillCount(), "release into empty queue underfills")

	// A second acquire/release cycle underfills again.
	u2, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	require.NoError(t, u2.Release())
	assert.Equal(t, int64(2), s.UnderfillCount())
}

// TestSnake_UnderfillCount_WaiterGranted: releasing while a waiter is queued
// grants the waiter and does NOT underfill.
func TestSnake_UnderfillCount_WaiterGranted(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 1 }
	s := NewSnake(cfg)

	held, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	granted := make(chan *SafeUnlock, 1)
	go func() {
		if wu, werr := s.Acquire(t.Context(), "", 0); werr == nil {
			granted <- wu
		}
	}()
	// Wait until the waiter has parked in the queue (held is granted; waiter
	// makes QueueLen == 2).
	assert.Eventually(t, func() bool {
		return s.Stats().QueueLen == 2
	}, 2*time.Second, time.Millisecond, "waiter should enqueue")

	require.NoError(t, held.Release()) // grants the waiter — no underfill
	wu := <-granted
	require.NoError(t, wu.Release()) // now empty — this release underfills

	// Exactly one underfill: the waiter's own final release. The held.Release
	// granted the waiter, so it did not underfill.
	assert.Equal(t, int64(1), s.UnderfillCount())
}

// TestSnake_ShedBelowCapacityCount: a release that sheds aged waiters while the
// semaphore has a free slot (holders < capacity) counts those drops as
// shed-below-capacity — the counterproductive drops the keep-droppable floor is
// meant to prevent.
func TestSnake_ShedBelowCapacityCount(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 2 }
	// Small target/interval so the drop pass is due immediately once armed. A
	// large MinDropDelay keeps the (wall-clock) backstop timer from firing during
	// the test, so the shed happens on the synchronous release path — which is
	// exactly what this test exercises (a drop while a freed slot is open).
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.IntervalNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return int64(time.Hour) }
	s := newTestSnake(cfg)

	// Injected clock is read by both the test goroutine and (potentially) timer
	// goroutines, so it must be race-safe.
	var now atomic.Int64
	s.clockFunc = now.Load
	s.q.codelq.nowNs = now.Load

	// Fill capacity with two real holders (granted inline).
	u1, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	_, err = s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	// Enqueue three droppable waiters directly (capacity is full, so they park).
	// Their buffered signal channels absorb the drop signal — no goroutines.
	s.mu.Lock()
	for range 3 {
		s.q.lockedEnqueue("", 0)
	}
	// Force an armed dropping episode that is due now.
	s.q.codelq.dropping = true
	s.q.codelq.count = s.q.codelq.graceCount()
	s.q.codelq.dropNextNs = 1
	s.mu.Unlock()

	// Advance the clock well past the drop deadline so the pass sheds.
	now.Store(1_000_000_000)

	assert.Equal(t, int64(0), s.ShedBelowCapacityCount(), "no below-capacity sheds before the release")

	// Releasing one holder frees a slot (holders drops to 1 < capacity 2), then
	// the shed pass runs while below capacity — its drops are counted.
	require.NoError(t, u1.Release())

	assert.Positive(t, s.ShedBelowCapacityCount(), "sheds while a slot was free must be counted as below-capacity")
}

// TestCoDelQueue_KeepDroppableFloor: with a floor of N, the drop loop refuses to
// shed once the droppable backlog is at or below N, leaving a warm reserve.
func TestCoDelQueue_KeepDroppableFloor(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 }
	cfg.IntervalNs = func() int64 { return 10_000_000 }
	floor := 1
	cfg.KeepDroppableFloor = func() int { return floor }
	q, _ := newTestQueue(cfg, clock)

	for range 3 {
		testEnqueue(q, 0)
	}
	q.dropping = true
	q.count = q.graceCount()
	q.dropNextNs = 1
	clock.advance(1_000_000_000)

	dropFn := func() bool {
		// Mirror ValvedCoDelQueue.lockedDropFn's keep-droppable-floor guard.
		if f := q.keepDroppableFloor(); f > 0 && q.droppableLen <= f {
			return false
		}
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}

	// Floor of 1: drain via the drop loop; it must stop at 1 remaining droppable.
	for q.droppableLen > 1 && dropFn() {
	}
	assert.Equal(t, 1, q.droppableLen, "floor=1 must leave exactly one droppable request")
	assert.False(t, dropFn(), "floor=1 refuses to drop the final droppable request")

	// Turning the floor off (0) allows the last one to drop.
	floor = 0
	assert.True(t, dropFn(), "with the floor off, the last droppable can be dropped")
	assert.Equal(t, 0, q.droppableLen)
}

// TestCoDelQueue_KeepDroppableFloor_N: a floor of N > 1 keeps N droppable
// requests as a warm reserve and only sheds the backlog above N.
func TestCoDelQueue_KeepDroppableFloor_N(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 }
	cfg.IntervalNs = func() int64 { return 10_000_000 }
	cfg.KeepDroppableFloor = func() int { return 3 }
	q, _ := newTestQueue(cfg, clock)

	for range 10 {
		testEnqueue(q, 0)
	}
	q.dropping = true
	q.count = q.graceCount()
	q.dropNextNs = 1
	clock.advance(1_000_000_000)

	dropFn := func() bool {
		if f := q.keepDroppableFloor(); f > 0 && q.droppableLen <= f {
			return false
		}
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}

	// Drain as far as the loop allows; the floor stops it at exactly 3.
	for dropFn() {
	}
	assert.Equal(t, 3, q.droppableLen, "floor=3 keeps a reserve of three droppable requests")
	assert.False(t, dropFn(), "floor=3 refuses to shed below the reserve")
}
