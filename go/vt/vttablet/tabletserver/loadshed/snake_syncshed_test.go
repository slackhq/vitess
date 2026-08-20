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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnake_SyncShedOnRelease proves that a release sheds stale requests
// synchronously (at the dequeue point) using a fresh clock, without relying on
// the backstop drop timer firing. On the pre-synchronous-shedding code this
// backlog would only be shed when runDropTimer eventually fired; here the
// release itself must drive the CoDel drop.
func TestSnake_SyncShedOnRelease(t *testing.T) {
	var now atomic.Int64
	cfg := defaultSnakeConfig()
	// Tight target/interval so the queued waiters are "stale" almost immediately
	// once the clock advances.
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.IntervalNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	cfg.Capacity = func() int { return 1 }

	s := NewSnake(cfg)
	s.clockFunc = now.Load
	s.q.codelq.nowNs = now.Load // queue uses defaultClock otherwise — keep clocks consistent

	// Grant the single slot to a holder we control.
	holder, err := s.Acquire(t.Context(), 0)
	require.NoError(t, err)

	// Enqueue several droppable waiters directly into the queue and force the
	// dropping episode, mimicking the state after sustained overload. They block
	// on their signal channels; a shed signals them with a DroppedRequestError.
	// The backlog must exceed keepDroppableFloor so the drop pass actually sheds
	// (it refuses to shed at or below the floor).
	const backlog = keepDroppableFloor + 4
	waiters := make([]*Request, backlog)
	s.mu.Lock()
	for i := range backlog {
		waiters[i] = s.q.lockedEnqueue("", 1)
	}
	// Drive the queue into an armed dropping episode with drops already due. A
	// real armed episode always has dropNextNs > 0; seed it in the past so drops
	// are immediately due once the clock advances.
	s.q.codelq.dropping = true
	s.q.codelq.dropNextNs = 1
	s.q.codelq.count = s.q.codelq.graceCount() // past grace so drops actually fire
	s.mu.Unlock()

	// Advance the clock so every waiter's sojourn is over target.
	now.Store(1_000_000)

	// Release the holder. This is the dequeue point: it must synchronously shed
	// the stale backlog (draining all due drops) rather than wait for the timer.
	require.NoError(t, holder.Release())

	// The backlog should have been shed: each waiter received a drop signal.
	dropped := 0
	for _, w := range waiters {
		select {
		case v := <-w.signalChan:
			if v != grantSentinel {
				dropped++
			}
		default:
		}
	}
	assert.Positive(t, dropped, "release must synchronously shed stale queued requests without a timer fire")
}
