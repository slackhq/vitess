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
	"github.com/stretchr/testify/require"
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

func TestValved_DropReturnsDroppedRequests(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)
	// Fast target/interval so drops are due immediately once armed.
	sq.codelq.cfg.TargetNs = func() int64 { return 1_000_000 }
	sq.codelq.cfg.IntervalNs = func() int64 { return 10_000_000 }

	chained := make([]*testRequest, 3)
	for i := range chained {
		chained[i] = sq.lockedEnqueue("a", 100, "")
	}
	distinct := make([]*testRequest, 5)
	for i := range distinct {
		distinct[i] = sq.lockedEnqueue(string(rune('b'+i)), 1, "")
	}
	require.True(t, sq.codelq.dropping, "first droppable enqueue arms an episode")

	// Seed a due, ramped episode and advance well past the deadline so the pass
	// sheds multiple requests in one lockedRunTimer call.
	sq.codelq.count = 1
	sq.codelq.dropNextNs = 1
	clock.advance(1_000_000_000)

	dropped := sq.lockedRunTimer()
	require.Equal(t, []*testRequest{chained[0], chained[1], chained[2], distinct[0]}, dropped)
	for _, req := range dropped {
		assert.Equal(t, outcomeShed, req.outcome)
	}
	assert.Empty(t, sq.lockedRunTimer())
}

// TestValved_DisabledTearsDownEpisodeWithoutDropping verifies that running the
// timer while disabled tears the active CoDel episode down to idle instead of
// warming it: no drops, no count ramp, no armed timer. This is the "standard
// queue" contract for shadow/off modes.
func TestValved_DisabledTearsDownEpisodeWithoutDropping(t *testing.T) {
	clock := newTestClock()
	sq, rec := newValvedQueue(clock)
	sq.codelq.cfg.TargetNs = func() int64 { return 1_000_000 }
	sq.codelq.cfg.IntervalNs = func() int64 { return 10_000_000 }

	const backlog = 5
	reqs := make([]*testRequest, backlog)
	for i := range reqs {
		reqs[i] = sq.lockedEnqueue(string(rune('a'+i)), 100, "")
	}
	require.True(t, sq.codelq.dropping, "enabled enqueue arms an episode")
	sq.codelq.count = 5
	sq.codelq.dropNextNs = 1
	clock.advance(1_000_000_000)

	sq.lockedRunTimerIf(false)

	assert.False(t, sq.codelq.dropping, "disabled run leaves the dropping state")
	assert.Equal(t, 1, sq.codelq.count, "disabled run does not warm the count")
	assert.Zero(t, sq.codelq.dropNextNs, "disabled run clears the drop deadline")
	assert.False(t, rec.armed, "disabled run stops the drop timer")
	assert.Equal(t, backlog, sq.lockedLen(), "disabled run drops nothing")
	for _, req := range reqs {
		assert.False(t, req.done())
	}
}

// TestValved_ShadowModeEnqueueDoesNotArm verifies that when the queue is not in
// ModeEnabled, enqueuing a droppable backlog never arms a CoDel episode: the
// queue stays idle (count 1, no drop deadline, no timer) so it behaves as a
// plain FIFO.
func TestValved_ShadowModeEnqueueDoesNotArm(t *testing.T) {
	clock := newTestClock()
	sq, rec := newValvedQueueMode(clock, func() Mode { return ModeShadow })
	sq.codelq.cfg.TargetNs = func() int64 { return 1_000_000 }
	sq.codelq.cfg.IntervalNs = func() int64 { return 10_000_000 }

	const backlog = 10
	reqs := make([]*testRequest, backlog)
	for i := range reqs {
		reqs[i] = sq.lockedEnqueue(string(rune('a'+i)), 100, "")
	}

	assert.False(t, sq.codelq.dropping, "shadow enqueue never enters dropping")
	assert.Equal(t, 1, sq.codelq.count, "shadow enqueue never warms the count")
	assert.Zero(t, sq.codelq.dropNextNs, "shadow enqueue never seeds a drop deadline")
	assert.False(t, rec.armed, "shadow enqueue never arms the drop timer")
	assert.Equal(t, backlog, sq.lockedLen(), "all requests remain queued")
	for _, req := range reqs {
		assert.False(t, req.done())
	}
}
