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

// armLateDroppingEpisode fills the CoDel queue with n droppable requests and
// arms a dropping episode whose deadline is far in the past, so the next
// lockedRunTimer is a late fire that would catch-up-loop through many intervals.
func armLateDroppingEpisode(q *ValvedCoDelQueue, clock *testClock, n int) {
	for range n {
		q.lockedEnqueue("", 0)
	}
	c := q.codelq
	c.dropping = true
	c.count = c.graceCount()
	c.dropNextNs = 1
	// Advance well past dropNextNs so the un-capped loop would drain the queue.
	clock.advance(1_000_000_000)
}

// TestMaxDropsPerFire_CapsBurst: a late fire that would otherwise drain the
// whole droppable backlog sheds at most MaxDropsPerFire in one advance, and
// lastDropsPerFire reports exactly that.
func TestMaxDropsPerFire_CapsBurst(t *testing.T) {
	clock := newTestClock()
	rec := &testDropTimerRecorder{}
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 }
	cfg.IntervalNs = func() int64 { return 10_000_000 }
	cfg.MaxDropsPerFire = func() int { return 3 }
	q := newValvedCoDelQueue(cfg, clock.nowFunc, rec.schedule, rec.stop)

	armLateDroppingEpisode(q, clock, 10)

	before := q.lockedDroppableLen()
	q.lockedRunTimer()

	shed := before - q.lockedDroppableLen()
	assert.Equal(t, 3, shed, "a late fire must shed at most MaxDropsPerFire")
	assert.Equal(t, 3, q.lockedLastDropsPerFire(), "lastDropsPerFire reports the capped burst size")
	assert.Equal(t, 7, q.lockedDroppableLen(), "the rest of the backlog remains for later advances")
}

// TestMaxDropsPerFire_UnlimitedDrains: with no cap, the same late fire drains
// the whole backlog in one advance (the burst the cap is meant to bound), and
// lastDropsPerFire reflects the full count.
func TestMaxDropsPerFire_UnlimitedDrains(t *testing.T) {
	clock := newTestClock()
	rec := &testDropTimerRecorder{}
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 }
	cfg.IntervalNs = func() int64 { return 10_000_000 }
	// MaxDropsPerFire nil => unlimited.
	q := newValvedCoDelQueue(cfg, clock.nowFunc, rec.schedule, rec.stop)

	armLateDroppingEpisode(q, clock, 10)

	q.lockedRunTimer()

	assert.Equal(t, 0, q.lockedDroppableLen(), "without a cap a late fire drains the backlog")
	assert.Greater(t, q.lockedLastDropsPerFire(), 3, "the uncapped burst is larger than the cap would allow")
}

// TestMaxDropsPerFire_ResumesAcrossAdvances: the capped remainder is deferred,
// not forgiven — successive advances keep shedding the cap each time until the
// backlog is gone.
func TestMaxDropsPerFire_ResumesAcrossAdvances(t *testing.T) {
	clock := newTestClock()
	rec := &testDropTimerRecorder{}
	cfg := defaultTestConfig()
	cfg.TargetNs = func() int64 { return 1_000_000 }
	cfg.IntervalNs = func() int64 { return 10_000_000 }
	cfg.MaxDropsPerFire = func() int { return 2 }
	q := newValvedCoDelQueue(cfg, clock.nowFunc, rec.schedule, rec.stop)

	armLateDroppingEpisode(q, clock, 6)

	total := 0
	for range 5 {
		before := q.lockedDroppableLen()
		q.lockedRunTimer()
		total += before - q.lockedDroppableLen()
		if q.lockedDroppableLen() == 0 {
			break
		}
	}
	assert.Equal(t, 6, total, "successive capped advances eventually shed the whole backlog")
}
