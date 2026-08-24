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

// dropAll is the standard test dropFn: sheds the lowest-priority droppable head.
func dropAllFn(q *testCoDelQueue) func() bool {
	return func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedRemove(elem.Value.(*testRequest))
		return true
	}
}

// TestCoDelQueue_DequeueSheds_AfterEpisodeTornDown reproduces the bug where the
// dequeue path could not re-establish a dropping episode on its own: once
// lockedDequeue cleared `dropping` (a dequeue whose sojourn was under target), only
// the backstop timer re-armed it. With the timer effectively off (large
// MinDropDelay), the dequeue path must run the full CoDel logic and shed stale
// waiters without a timer fire.
func TestCoDelQueue_DequeueSheds_AfterEpisodeTornDown(t *testing.T) {
	clock := newTestClock()
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 }           // 1ms
	cfg.IntervalNs = func() int64 { return 10_000_000 }        // 10ms
	cfg.MinDropDelayNs = func() int64 { return 1_000_000_000 } // 1s: backstop off
	q, rec := newTestQueue(cfg, clock)

	// Build a droppable backlog. The first enqueue arms an episode (slow mode).
	const backlog = 6
	for range backlog {
		testEnqueue(q, 0)
	}
	assert.True(t, q.dropping, "first droppable enqueue should arm an episode")

	// Simulate the episode teardown that dequeue triggers: a request whose
	// sojourn is under target clears `dropping` in lockedDequeue. We reproduce the
	// cleared state directly (this is the state the dequeue path must recover from).
	q.dropping = false

	// Time passes well beyond target+interval: every remaining waiter is stale and
	// drops are due. The backstop timer never fires (MinDropDelay=1s).
	clock.advance(5_000_000_000) // 5s

	// Drive the dequeue path repeatedly. It must re-establish
	// the episode and shed the stale backlog with no timer fire.
	before := q.droppableLen
	for i := 0; i < backlog+2; i++ {
		rec.reset()
		q.lockedRunTimer(dropAllFn(q))
		clock.advance(1_000_000_000) // keep drops due each cycle
	}

	assert.Less(t, q.droppableLen, before, "dequeue path must shed stale waiters without a timer fire")
	assert.Zero(t, q.droppableLen, "sustained dequeue under overload should drain the stale backlog")
}

// TestValved_Drop_DefersSignalOutsideLock asserts the deferral contract: a batch
func TestValved_DropReturnsPendingRequests(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)
	// Fast target/interval so drops are due immediately once armed.
	sq.codelq.cfg.TargetNs = func() int64 { return 1_000_000 }
	sq.codelq.cfg.IntervalNs = func() int64 { return 10_000_000 }

	// A backlog of distinct-valve droppable requests (distinct valves so each is
	// its own droppable representative and all are eligible to shed).
	const backlog = 5
	for i := range backlog {
		sq.lockedEnqueue(string(rune('a'+i)), 0)
	}
	require.True(t, sq.codelq.dropping, "first droppable enqueue arms an episode")

	// Seed a due, ramped episode and advance well past the deadline so the pass
	// sheds the whole backlog in one lockedRunTimer call.
	sq.codelq.count = 1
	sq.codelq.dropNextNs = 1
	clock.advance(1_000_000_000)

	sq.lockedRunTimer()

	pending := sq.lockedTakePendingDrops()
	require.NotEmpty(t, pending, "the pass should have shed some requests")
	for _, req := range pending {
		assert.NotNil(t, req.signaledValue)
	}
	assert.Nil(t, sq.lockedTakePendingDrops(), "taking again yields nothing (ownership transferred)")
}

func TestValved_DisabledDropAdvancesCoDelWithoutDropping(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)
	sq.codelq.cfg.TargetNs = func() int64 { return 1_000_000 }
	sq.codelq.cfg.IntervalNs = func() int64 { return 10_000_000 }

	const backlog = 5
	reqs := make([]*testRequest, backlog)
	for i := range reqs {
		reqs[i] = sq.lockedEnqueue(string(rune('a'+i)), 0)
	}
	sq.codelq.count = 1
	sq.codelq.dropNextNs = 1
	clock.advance(1_000_000_000)

	initialCount := sq.codelq.count
	sq.lockedRunTimerIf(func() bool { return false })

	assert.Greater(t, sq.codelq.count, initialCount)
	assert.Equal(t, backlog, sq.lockedLen())
	for _, req := range reqs {
		assert.Nil(t, req.signaledValue)
	}
}

func TestValved_EnablementSnapshottedOncePerBatch(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)
	sq.codelq.cfg.TargetNs = func() int64 { return 1_000_000 }
	sq.codelq.cfg.IntervalNs = func() int64 { return 10_000_000 }

	const backlog = 10
	reqs := make([]*testRequest, backlog)
	for i := range reqs {
		reqs[i] = sq.lockedEnqueue(string(rune('a'+i)), 0)
	}
	sq.codelq.count = 1
	sq.codelq.dropNextNs = 1
	clock.advance(1_000_000_000)

	checks := 0
	sq.lockedRunTimerIf(func() bool {
		checks++
		return false
	})

	assert.Equal(t, 1, checks)
	for _, req := range reqs {
		assert.Nil(t, req.signaledValue)
	}
}
