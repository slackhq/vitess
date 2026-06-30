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

package tabletserver

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// fakeGrantSnake is a controllable idleGrantingSnake for testing the granter
// loop in isolation. grantsRemaining sets how many TryGrantIdle calls return
// true before falling to false.
type fakeGrantSnake struct {
	grantsRemaining atomic.Int64
	calls           atomic.Int64
}

func (f *fakeGrantSnake) TryGrantIdle() bool {
	f.calls.Add(1)
	for {
		n := f.grantsRemaining.Load()
		if n <= 0 {
			return false
		}
		if f.grantsRemaining.CompareAndSwap(n, n-1) {
			return true
		}
	}
}

// newTestGranter builds a single-worker granter with a counting yield, so tests
// run deterministically without depending on real CPU scheduling.
func newTestGranter(snake idleGrantingSnake, yields *atomic.Int64) *idleGranter {
	g := newIdleGranter()
	g.workers = 1
	g.snake = snake
	g.yield = func() { yields.Add(1) }
	return g
}

// TestIdleGranter_NoSpinWhenNothingToGrant is the core no-spin requirement:
// when TryGrantIdle always returns false (empty queue or at capacity), a kick
// must produce exactly one TryGrantIdle call and then the granter must block —
// it must not loop.
func TestIdleGranter_NoSpinWhenNothingToGrant(t *testing.T) {
	snake := &fakeGrantSnake{} // grantsRemaining == 0, always false
	var yields atomic.Int64
	g := newTestGranter(snake, &yields)

	g.start()
	t.Cleanup(g.stop)

	g.kick()

	// Exactly one TryGrantIdle call results from the kick, and no yields (no
	// grant succeeded). The granter then blocks on <-wake.
	assert.Eventually(t, func() bool {
		return snake.calls.Load() == 1
	}, 30*time.Second, time.Millisecond, "kick should produce exactly one grant attempt")

	// Give any (incorrect) spin a chance to manifest, then confirm the count is
	// still exactly one and nothing yielded.
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(1), snake.calls.Load(), "granter must not spin when no grant is possible")
	assert.Equal(t, int64(0), yields.Load(), "no yield without a successful grant")
}

// TestIdleGranter_DrainsBacklogOneYieldPerGrant verifies that on a kick the
// granter drains all currently-grantable waiters, yielding between each, then
// makes one final (failing) call and parks. For N grants: N+1 calls, N yields.
func TestIdleGranter_DrainsBacklogOneYieldPerGrant(t *testing.T) {
	const backlog = 5
	snake := &fakeGrantSnake{}
	snake.grantsRemaining.Store(backlog)
	var yields atomic.Int64
	g := newTestGranter(snake, &yields)

	g.start()
	t.Cleanup(g.stop)

	g.kick()

	assert.Eventually(t, func() bool {
		return snake.calls.Load() == backlog+1 && yields.Load() == backlog
	}, 30*time.Second, time.Millisecond,
		"one kick should drain the backlog: N+1 grant attempts and N yields")

	// And then it parks — no further calls.
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(backlog+1), snake.calls.Load(), "granter must park after draining")
}

// TestIdleGranter_KickCoalesces verifies kick never blocks and the cap-1 wake
// channel coalesces redundant kicks (sending many kicks doesn't panic/block).
func TestIdleGranter_KickCoalesces(t *testing.T) {
	snake := &fakeGrantSnake{}
	var yields atomic.Int64
	g := newTestGranter(snake, &yields)

	// Not started: kicks must still not block thanks to the buffered channel
	// and non-blocking send.
	for range 100 {
		g.kick()
	}
}

// TestIdleGranter_CleanStartStop verifies start/stop does not deadlock even
// with pending kicks.
func TestIdleGranter_CleanStartStop(t *testing.T) {
	snake := &fakeGrantSnake{}
	var yields atomic.Int64
	g := newTestGranter(snake, &yields)

	g.start()
	g.kick()
	g.kick()
	g.stop() // should return promptly
}
