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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Recovery from overload ---

func TestSnake_Overload_RecoveryToHealthy(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 5_000_000 }   // 5ms
	cfg.CoDel.TargetNs = func() int64 { return 500_000 }       // 0.5ms
	cfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 } // 0.1ms
	s := NewSnake(cfg)

	// Phase 1: overload — hold lock for a long time with waiters
	unlock, err := s.Acquire(t.Context(), "")
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), "")
			if err == nil {
				u.Release()
			}
		}()
	}

	// Hold long enough to trigger drops
	time.Sleep(100 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	// Phase 2: recovery — the queue should eventually return to healthy
	assert.Eventually(t, func() bool {
		return s.IsHealthy()
	}, 2*time.Second, 10*time.Millisecond, "queue should recover to healthy")

	// Phase 3: verify function — uncontended acquires should succeed immediately
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	u, err := s.Acquire(ctx, "")
	require.NoError(t, err, "post-recovery acquire should succeed quickly")
	u.Release()
}

// --- Holder keeps queue non-empty (preserves CoDel pressure signal) ---

func TestSnake_Overload_HolderKeepsQueueNonEmpty(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context(), "")
	require.NoError(t, err)

	// The holder stays at the head — queue length should be 1 even though
	// the lock is granted and no one else is waiting.
	s.mu.Lock()
	qLen := s.q.lockedLen()
	s.mu.Unlock()
	assert.Equal(t, 1, qLen, "holder should remain in queue to preserve pressure signal")

	unlock.Release()

	s.mu.Lock()
	qLen = s.q.lockedLen()
	s.mu.Unlock()
	assert.Equal(t, 0, qLen, "queue should be empty after release")
}

// --- Mass cancel at Snake layer ---

func TestSnake_Overload_MassCancel(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context(), "mass-cancel")
	require.NoError(t, err)

	const n = 30
	ctxs := make([]context.Context, n)
	cancels := make([]context.CancelFunc, n)
	errChs := make([]chan error, n)

	var wg sync.WaitGroup
	for i := range n {
		ctxs[i], cancels[i] = context.WithCancel(t.Context())
		errChs[i] = make(chan error, 1)
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(ctxs[idx], "mass-cancel")
			if err == nil {
				u.Release()
			}
			errChs[idx] <- err
		}()
	}

	time.Sleep(20 * time.Millisecond)

	// Cancel all waiters
	for _, cancel := range cancels {
		cancel()
	}

	wg.Wait()

	// All should have been cancelled
	for i := range n {
		err := <-errChs[i]
		assert.ErrorIs(t, err, context.Canceled, "waiter %d should be cancelled", i)
	}

	// Release original holder
	unlock.Release()
	assert.True(t, s.isIdle())

	// Verify the lock is still usable
	u, err := s.Acquire(t.Context(), "mass-cancel")
	require.NoError(t, err)
	u.Release()
}

// --- Drop timer delay decreases with successive drops ---

func TestCoDelQueue_DropDelay_Decreasing(t *testing.T) {
	clock := newTestClock()
	cfg := CoDelConfig{
		IntervalNs:     func() int64 { return 1_000_000_000 }, // 1s
		TargetNs:       func() int64 { return 1 },             // 1ns
		MinDropDelayNs: func() int64 { return 100 },
		Exponent:       func() float64 { return 1.0 },
		EasingLogBase:  func() float64 { return 2.0 },
	}
	q, _ := newTestQueue(cfg, clock)

	// Put queue in dropping state
	q.dropping = true
	q.count = 1
	q.dropNextNs = 0

	// Compute successive intervals: each should be <= the previous
	var intervals []int64
	for i := 1; i <= 10; i++ {
		q.count = i
		interval := q.lockedCurrentInterval()
		intervals = append(intervals, interval)
	}

	for i := 1; i < len(intervals); i++ {
		assert.LessOrEqual(t, intervals[i], intervals[i-1],
			"interval[%d]=%d should be <= interval[%d]=%d",
			i, intervals[i], i-1, intervals[i-1])
	}
}

// --- Drop ONLY droppable, leaving undroppable untouched ---

func TestSnake_Overload_DropOnlyDroppable(t *testing.T) {
	// Verify that CoDel drops only affect droppable requests (LoadsheddingAllowed=true)
	// and never undroppable requests (LoadsheddingAllowed=false). Uses two separate
	// Snakes to avoid a shared-atomic race that can flip all requests to one category.
	t.Run("droppable", func(t *testing.T) {
		cfg := defaultSnakeConfig()
		cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
		cfg.CoDel.TargetNs = func() int64 { return 1 }
		cfg.CoDel.MinDropDelayNs = func() int64 { return 1_000 }
		cfg.LoadsheddingAllowed = func() bool { return true }
		s := NewSnake(cfg)

		unlock, err := s.Acquire(t.Context(), "")
		require.NoError(t, err)

		var wg sync.WaitGroup
		var dropped atomic.Int64
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				u, err := s.Acquire(t.Context(), "")
				if err != nil {
					dropped.Add(1)
					return
				}
				u.Release()
			}()
		}

		time.Sleep(200 * time.Millisecond)
		unlock.Release()
		wg.Wait()

		assert.Greater(t, dropped.Load(), int64(0), "droppable requests should be dropped under overload")
	})

	t.Run("undroppable", func(t *testing.T) {
		cfg := defaultSnakeConfig()
		cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
		cfg.CoDel.TargetNs = func() int64 { return 1 }
		cfg.CoDel.MinDropDelayNs = func() int64 { return 1_000 }
		cfg.LoadsheddingAllowed = func() bool { return false }
		s := NewSnake(cfg)

		unlock, err := s.Acquire(t.Context(), "")
		require.NoError(t, err)

		var wg sync.WaitGroup
		var dropped atomic.Int64
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				u, err := s.Acquire(t.Context(), "")
				if err != nil {
					dropped.Add(1)
					return
				}
				u.Release()
			}()
		}

		time.Sleep(200 * time.Millisecond)
		unlock.Release()
		wg.Wait()

		assert.Zero(t, dropped.Load(), "undroppable requests must never be dropped")
	})
}

// --- Successive timer fires with count increment ---

func TestCoDelQueue_SuccessiveDrops_IncrementCount(t *testing.T) {
	clock := newTestClock()
	cfg := CoDelConfig{
		IntervalNs:     func() int64 { return 1_000_000 }, // 1ms
		TargetNs:       func() int64 { return 100_000 },   // 0.1ms
		MinDropDelayNs: func() int64 { return 100 },
		Exponent:       func() float64 { return 1.0 },
		EasingLogBase:  func() float64 { return 2.0 },
	}
	q, _ := newTestQueue(cfg, clock)

	// The CoDel algorithm enters dropping state when it detects persistent
	// queue. Once dropping, each successive timer fire increments count and
	// the control law shrinks the inter-drop interval. This test verifies
	// count increases across multiple timer fires.

	// Pre-condition: manually enter dropping state with a known count. The
	// timer is scheduled to fire at dropNextNs=now; it then fires a few
	// intervals late, so the control law catches up by dropping several
	// requests in one pass while leaving a backlog (queue stays in dropping).
	q.dropping = true
	q.count = 3
	q.dropNextNs = clock.now

	// Verify that successive drops within a single timer fire increment count.
	for range 10 {
		testEnqueue(q, 0)
	}

	clock.advance(1_000_000) // 1ms — a few intervals late

	dropFn := func() bool {
		elem := q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		q.lockedPopElem(elem, &DroppedRequestError{})
		return true
	}

	q.lockedRunTimer(dropFn)
	finalCount := q.count

	assert.Greater(t, finalCount, 3,
		"count should increment from starting value during drop cycle")
	assert.Greater(t, 10-q.lockedLen(), 0, "some requests should have been dropped")
}

// --- Self-contention: many IDs under overload, only cross-ID contention gets dropped ---

func TestSnake_Overload_SelfContention_CrossIDDrops(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 5_000_000 }   // 5ms
	cfg.CoDel.TargetNs = func() int64 { return 500_000 }       // 0.5ms
	cfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 } // 0.1ms
	s := NewSnake(cfg)

	// Hold the lock
	unlock, err := s.Acquire(t.Context(), "holder")
	require.NoError(t, err)

	const numIDs = 10
	const perID = 5

	var wg sync.WaitGroup
	type result struct {
		granted int64
		dropped int64
		valveID string
	}
	results := make([]result, numIDs)

	for id := range numIDs {
		results[id].valveID = fmt.Sprintf("overload-id%d", id)
		for range perID {
			idx := id
			wg.Add(1)
			go func() {
				defer wg.Done()
				u, err := s.Acquire(t.Context(), results[idx].valveID)
				if err != nil {
					atomic.AddInt64(&results[idx].dropped, 1)
					return
				}
				atomic.AddInt64(&results[idx].granted, 1)
				u.Release()
			}()
		}
	}

	// Hold long enough for CoDel to enter dropping state
	time.Sleep(150 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	totalGranted := int64(0)
	totalDropped := int64(0)
	for _, r := range results {
		totalGranted += r.granted
		totalDropped += r.dropped
		assert.Equal(t, int64(perID), r.granted+r.dropped,
			"valve %s: granted+dropped should equal perID", r.valveID)
	}

	assert.Greater(t, totalDropped, int64(0),
		"cross-ID contention should cause some drops")
	assert.Equal(t, int64(numIDs*perID), totalGranted+totalDropped)
}

// --- Memory steady-state: clean baseline after many cycles ---

func TestSnake_Memory_CleanBaseline(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	// Run many acquire/release cycles
	for range 1000 {
		u, err := s.Acquire(t.Context(), "cycle-id")
		require.NoError(t, err)
		u.Release()
	}

	// Verify all internal state is clean
	s.mu.Lock()
	defer s.mu.Unlock()

	assert.Empty(t, s.holders, "holders map should be empty after all releases")
	assert.Equal(t, 0, s.q.lockedLen(), "queue should be empty")
	assert.Empty(t, s.q.valves, "valves map should be empty")
	assert.Empty(t, s.q.outstandingCounts, "outstandingCounts map should be empty")
	assert.Empty(t, s.q.droppablePerValve, "droppablePerValve map should be empty")
	assert.Empty(t, s.maxAgeTimers, "max age timers should be empty")
	assert.False(t, s.q.codelq.dropping, "CoDel should not be in dropping state")
}

// --- Memory: many distinct valve IDs map cleanup ---

func TestSnake_Memory_ManyDistinctValveIDs_Cleanup(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	// Use many distinct valve IDs
	for i := range 500 {
		u, err := s.Acquire(t.Context(), fmt.Sprintf("distinct-%d", i))
		require.NoError(t, err)
		u.Release()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	assert.Empty(t, s.q.valves, "valves should be empty after all releases")
	assert.Empty(t, s.q.outstandingCounts, "outstandingCounts should be empty after all releases")
	assert.Empty(t, s.q.droppablePerValve, "droppablePerValve should be empty after all releases")
}

// --- CoDel state transition: healthy → unhealthy → healthy ---

func TestSnake_Memory_CoDelStateTransition(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 5_000_000 }   // 5ms
	cfg.CoDel.TargetNs = func() int64 { return 500_000 }       // 0.5ms
	cfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 } // 0.1ms
	s := NewSnake(cfg)

	// Initially healthy
	assert.True(t, s.IsHealthy())

	// Create overload
	unlock, err := s.Acquire(t.Context(), "")
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), "")
			if err == nil {
				u.Release()
			}
		}()
	}

	// Wait for drops to start
	time.Sleep(100 * time.Millisecond)

	// Should be unhealthy now (or may have recovered if drops cleared queue)
	unlock.Release()
	wg.Wait()

	// After all waiters resolve, the queue should return to healthy
	assert.Eventually(t, func() bool {
		return s.IsHealthy()
	}, 2*time.Second, 10*time.Millisecond, "should recover to healthy after overload resolves")

	// And still usable
	u, err := s.Acquire(t.Context(), "")
	require.NoError(t, err)
	u.Release()
}

// --- Max-age vs context cancel race ---

func TestSnake_MaxAge_VsContextCancel_Race(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.MaxAge = func() time.Duration { return 5 * time.Millisecond }
	s := NewSnake(cfg)

	const iterations = 1000
	for range iterations {
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Millisecond)

		u, err := s.Acquire(ctx, "")
		if err != nil {
			cancel()
			continue
		}

		// Either max-age fires or context expires — either way, lock must not leak
		time.Sleep(6 * time.Millisecond)
		u.Release() // may return error if max-age already released
		cancel()
	}

	assert.True(t, s.isIdle(), "lock must not be stuck after max-age/cancel races")
}

// --- Double-signal panic invariant ---

func TestSnake_NoPanicOnConcurrentCancel(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context(), "no-panic")
	require.NoError(t, err)

	const n = 50
	var wg sync.WaitGroup

	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Millisecond)
			defer cancel()
			u, err := s.Acquire(ctx, "no-panic")
			if err == nil {
				u.Release()
			}
		}()
	}

	time.Sleep(10 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	assert.True(t, s.isIdle())
}
