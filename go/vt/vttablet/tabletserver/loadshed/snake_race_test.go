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
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnake_CancelVsGrant_Race proves and verifies the fix for a race between
// context cancellation and lock grant in Acquire's double-select.
//
// The race window: the releaser sets holder=B and unlocks, then signals B.
// Between unlock and signal, B's cancel path can take the default branch
// (done channel empty) and acquire the mutex. Without the fix, B would call
// lockedCancel on itself (the holder), leaking the grant.
//
// With the fix, the default branch checks s.holder == req after acquiring the
// mutex. If true, it calls releaseOnCancel to hand the lock to the next waiter.
func TestSnake_CancelVsGrant_Race(t *testing.T) {
	const iterations = 50000

	cfg := defaultSnakeConfig()
	var leaked atomic.Int64

	for range iterations {
		s := newTestSnake(cfg)

		unlockA, err := s.Acquire(t.Context(), "")
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())

		var wg sync.WaitGroup
		wg.Add(1)

		var acquireErr error
		var unlockB *SafeUnlock

		go func() {
			defer wg.Done()
			unlockB, acquireErr = s.Acquire(ctx, "")
		}()

		runtime.Gosched()

		// Cancel and release concurrently to maximize interleaving.
		cancel()
		unlockA.Release()

		wg.Wait()

		if acquireErr != nil {
			// B was cancelled. Verify the lock isn't stuck.
			s.mu.Lock()
			holderAfterCancel := s.holder
			s.mu.Unlock()

			if holderAfterCancel != nil {
				leaked.Add(1)
			}

			// Verify lock is still usable.
			ctx2, cancel2 := context.WithTimeout(t.Context(), 10*time.Millisecond)
			unlockC, err2 := s.Acquire(ctx2, "")
			cancel2()
			if assert.NoError(t, err2, "lock must remain acquirable") {
				unlockC.Release()
			}
		} else {
			unlockB.Release()
		}
	}

	assert.Zero(t, leaked.Load(),
		"cancel-vs-grant race leaked %d/%d grants", leaked.Load(), iterations)
}
