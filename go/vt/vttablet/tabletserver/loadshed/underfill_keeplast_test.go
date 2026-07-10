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

// TestCoDelQueue_KeepLastDroppable: with the option on, the drop loop refuses to
// shed the final droppable request.
func TestCoDelQueue_KeepLastDroppable(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 }
	cfg.IntervalNs = func() int64 { return 10_000_000 }
	keep := true
	cfg.KeepLastDroppable = func() bool { return keep }
	q, _ := newTestQueue(cfg, clock)

	for range 3 {
		testEnqueue(q, 0)
	}
	q.dropping = true
	q.count = q.graceCount()
	q.dropNextNs = 1
	clock.advance(1_000_000_000)

	dropFn := func() bool {
		// Mirror ValvedCoDelQueue.lockedDropFn's keep-last guard.
		if q.keepLastDroppable() && q.droppableLen <= 1 {
			return false
		}
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}

	// Drain via the drop loop; it must stop at 1 remaining droppable.
	for q.droppableLen > 1 && dropFn() {
	}
	assert.Equal(t, 1, q.droppableLen, "keep-last must leave exactly one droppable request")
	assert.False(t, dropFn(), "keep-last refuses to drop the final droppable request")

	// Turning the option off allows the last one to drop.
	keep = false
	assert.True(t, dropFn(), "with keep-last off, the last droppable can be dropped")
	assert.Equal(t, 0, q.droppableLen)
}
