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

	"github.com/stretchr/testify/assert"
)

func dropAllFn(q *testCoDelQueue) func() bool {
	return func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedRemove(elem.Value)
		return true
	}
}

// Dequeue must advance CoDel because MinDropDelay may postpone the backstop timer.
func TestCoDelQueue_DequeueSheds_AfterEpisodeTornDown(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 }
	cfg.IntervalNs = func() int64 { return 10_000_000 }
	cfg.MinDropDelayNs = func() int64 { return 1_000_000_000 } // 1s: backstop off
	q, rec := newTestQueue(cfg, clock)

	const backlog = 6
	for range backlog {
		testEnqueue(q, 1)
	}
	assert.True(t, q.dropping, "first droppable enqueue should arm an episode")

	// Reproduce the state after a healthy dequeue tears down the episode.
	q.dropping = false

	clock.advance(5_000_000_000)

	before := q.droppableLen
	for i := 0; i < backlog+2; i++ {
		rec.reset()
		q.lockedRunTimer(dropAllFn(q))
		clock.advance(1_000_000_000) // keep drops due each cycle
	}

	assert.Less(t, q.droppableLen, before, "dequeue path must shed stale waiters without a timer fire")
	assert.Zero(t, q.droppableLen, "sustained dequeue under overload should drain the stale backlog")
}

func TestSnake_KeepDroppableFloorConfigurable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		floor     func() int
		wantDrops int
	}{
		{name: "default", floor: nil, wantDrops: 0},
		{name: "zero", floor: func() int { return 0 }, wantDrops: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snake, clock, _ := newStatsTestSnake()
			snake.cfg.CoDel.KeepDroppableFloor = tc.floor

			snake.Enqueue("", 1)
			clock.advance(20)

			assert.Len(t, snake.LockedDropTimerFired(), tc.wantDrops)
		})
	}
}
