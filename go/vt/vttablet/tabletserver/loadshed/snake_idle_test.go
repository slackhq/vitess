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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// idleGatingConfig returns a snake config with idle gating enabled, a given
// capacity and floor, and an OnGatedWaiter that counts how often it fires.
func idleGatingConfig(capacity, floor int, enabled *atomic.Bool, gatedKicks *atomic.Int64) SnakeConfig {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return capacity }
	cfg.MinConcurrency = func() int { return floor }
	cfg.IdleGatingEnabled = enabled.Load
	cfg.OnGatedWaiter = func() { gatedKicks.Add(1) }
	return cfg
}

// TestSnakeIdle_BypassWhenDisabled verifies that with idle gating disabled, the
// floor and gated-waiter hooks have no effect: grants happen on capacity and
// OnGatedWaiter never fires.
func TestSnakeIdle_BypassWhenDisabled(t *testing.T) {
	var enabled atomic.Bool // false
	var gatedKicks atomic.Int64
	s := newTestSnake(idleGatingConfig(4, 1, &enabled, &gatedKicks))

	// Acquire several past the floor; all should be granted immediately since
	// gating is off.
	var unlocks []*SafeUnlock
	for range 3 {
		unlock, err := s.Acquire(t.Context(), "", 0)
		require.NoError(t, err)
		unlocks = append(unlocks, unlock)
	}
	assert.Equal(t, 3, s.nGranted())
	assert.Equal(t, int64(0), gatedKicks.Load(), "OnGatedWaiter must not fire when gating disabled")

	for _, u := range unlocks {
		require.NoError(t, u.Release())
	}
}

// TestSnakeIdle_NoNotifierDoesNotGate verifies the fail-safe: if idle gating is
// enabled but OnGatedWaiter is nil, there is no granter to wake a gated waiter,
// so the gate must not engage. Requests are granted on capacity instead of
// being stranded (never granted, never notified).
func TestSnakeIdle_NoNotifierDoesNotGate(t *testing.T) {
	var enabled atomic.Bool
	enabled.Store(true)
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 4 }
	cfg.MinConcurrency = func() int { return 1 }
	cfg.IdleGatingEnabled = enabled.Load
	// Deliberately no OnGatedWaiter.
	s := newTestSnake(cfg)

	// Acquire past the floor. Without a notifier the gate is inert, so these
	// must be granted immediately rather than blocking forever.
	var unlocks []*SafeUnlock
	for range 3 {
		unlock, err := s.Acquire(t.Context(), "", 0)
		require.NoError(t, err)
		unlocks = append(unlocks, unlock)
	}
	assert.Equal(t, 3, s.nGranted())

	for _, u := range unlocks {
		require.NoError(t, u.Release())
	}
}

// TestSnakeIdle_BelowFloorGrantsImmediately verifies that requests below the
// floor are granted immediately even with gating enabled.
func TestSnakeIdle_BelowFloorGrantsImmediately(t *testing.T) {
	var enabled atomic.Bool
	enabled.Store(true)
	var gatedKicks atomic.Int64
	s := newTestSnake(idleGatingConfig(4, 2, &enabled, &gatedKicks))

	// Floor is 2, so the first two acquires bypass the idle gate.
	u1, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	u2, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	assert.Equal(t, 2, s.nGranted())
	assert.Equal(t, int64(0), gatedKicks.Load(), "below-floor grants must not notify the idle granter")

	require.NoError(t, u1.Release())
	require.NoError(t, u2.Release())
}

// TestSnakeIdle_AtFloorWaitsForIdleGrant verifies that a request at/above the
// floor (but under capacity) blocks, fires OnGatedWaiter, and is released only
// by TryGrantIdle.
func TestSnakeIdle_AtFloorWaitsForIdleGrant(t *testing.T) {
	var enabled atomic.Bool
	enabled.Store(true)
	var gatedKicks atomic.Int64
	s := newTestSnake(idleGatingConfig(4, 1, &enabled, &gatedKicks))

	// Fill the floor (1 holder).
	floorHolder, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = floorHolder.Release() })

	// This acquire is above the floor but under capacity → must wait on idle.
	granted := make(chan error, 1)
	go func() {
		unlock, err := s.Acquire(t.Context(), "", 0)
		if err == nil {
			t.Cleanup(func() { _ = unlock.Release() })
		}
		granted <- err
	}()

	// OnGatedWaiter should fire because the request is held only by the gate.
	assert.Eventually(t, func() bool {
		return gatedKicks.Load() >= 1
	}, 30*time.Second, time.Millisecond, "OnGatedWaiter should fire for a gated request")

	// It must not be granted until an idle signal arrives.
	select {
	case <-granted:
		t.Fatal("request granted without an idle signal")
	case <-time.After(50 * time.Millisecond):
	}

	// Simulate the idle granter being scheduled.
	assert.True(t, s.TryGrantIdle(), "TryGrantIdle should grant the waiting request")

	select {
	case err := <-granted:
		assert.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("request not granted after TryGrantIdle")
	}
}

// TestSnakeIdle_FloorRefillOnRelease verifies that when a release drops the
// holder count below the floor, a waiting request is granted immediately
// without any TryGrantIdle call (completion-driven floor refill).
func TestSnakeIdle_FloorRefillOnRelease(t *testing.T) {
	var enabled atomic.Bool
	enabled.Store(true)
	var gatedKicks atomic.Int64
	s := newTestSnake(idleGatingConfig(4, 2, &enabled, &gatedKicks))

	// Two below-floor holders.
	u1, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	u2, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	// A third request is at the floor → gated, waits.
	granted := make(chan error, 1)
	go func() {
		unlock, err := s.Acquire(t.Context(), "", 0)
		if err == nil {
			t.Cleanup(func() { _ = unlock.Release() })
		}
		granted <- err
	}()

	assert.Eventually(t, func() bool {
		return gatedKicks.Load() >= 1
	}, 30*time.Second, time.Millisecond, "third request should be gated")

	// Release one holder: now only 1 holder < floor of 2, so the waiter must be
	// granted immediately on the release path — no TryGrantIdle needed.
	require.NoError(t, u1.Release())

	select {
	case err := <-granted:
		assert.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("waiter not granted on floor refill")
	}

	require.NoError(t, u2.Release())
}

// TestSnakeIdle_CoDelShedsGatedWaiters verifies the central property of the
// design: because Acquire enqueues into the CoDel queue and gating only
// withholds the grant, requests held back by the idle gate (CPU "never idle",
// i.e. no TryGrantIdle calls) remain visible to CoDel and are shed once their
// sojourn exceeds target. This proves Snake retains delay visibility under
// idle gating.
func TestSnakeIdle_CoDelShedsGatedWaiters(t *testing.T) {
	var enabled atomic.Bool
	enabled.Store(true)
	var gatedKicks atomic.Int64
	cfg := idleGatingConfig(4, 1, &enabled, &gatedKicks)
	// Aggressive CoDel so the gated waiters are shed quickly.
	cfg.CoDel.IntervalNs = func() int64 { return 5_000_000 }   // 5ms
	cfg.CoDel.TargetNs = func() int64 { return 500_000 }       // 0.5ms
	cfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 } // 0.1ms
	s := newTestSnake(cfg)

	// Occupy the floor so further requests are gated. We never call
	// TryGrantIdle, simulating a CPU that is never idle.
	floorHolder, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = floorHolder.Release() })

	// keepDroppableFloor retains a reserve of droppable requests, so CoDel sheds
	// the backlog only down to the floor and no further. Enqueue comfortably more
	// than the floor so shedding is exercised; the floor survivors are never shed
	// (CPU never idle, capacity pinned), so use a cancelable context to release
	// them at the end rather than blocking forever on wg.Wait().
	const numWaiters = keepDroppableFloor + 8
	ctx, cancel := context.WithCancel(t.Context())
	var dropped atomic.Int64
	var wg sync.WaitGroup
	for i := range numWaiters {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Distinct valve IDs so each gets its own droppable CoDel entry.
			unlock, err := s.Acquire(ctx, "gated-"+strconv.Itoa(id), 0)
			if err != nil {
				dropped.Add(1)
				return
			}
			_ = unlock.Release()
		}(i)
	}

	// The backlog above the floor is shed; the floor reserve stays parked.
	assert.Eventually(t, func() bool {
		return dropped.Load() == int64(numWaiters-keepDroppableFloor)
	}, 2*time.Second, 5*time.Millisecond,
		"gated waiters above the keep-droppable floor should be shed when the CPU is never idle")

	cancel()
	wg.Wait()
}

// TestSnakeIdle_LiveToggle verifies that flipping IdleGatingEnabled from true to
// false lets a gated waiter be granted on capacity via the normal release path,
// proving the runtime bypass works without restarting anything.
func TestSnakeIdle_LiveToggle(t *testing.T) {
	var enabled atomic.Bool
	enabled.Store(true)
	var gatedKicks atomic.Int64
	s := newTestSnake(idleGatingConfig(4, 1, &enabled, &gatedKicks))

	floorHolder, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	// Gated waiter.
	granted := make(chan error, 1)
	go func() {
		unlock, err := s.Acquire(t.Context(), "", 0)
		if err == nil {
			t.Cleanup(func() { _ = unlock.Release() })
		}
		granted <- err
	}()

	assert.Eventually(t, func() bool {
		return gatedKicks.Load() >= 1
	}, 30*time.Second, time.Millisecond, "waiter should be gated while enabled")

	// Disable gating, then release the floor holder. With gating off, the
	// release path grants the waiter on capacity (no idle signal needed).
	enabled.Store(false)
	require.NoError(t, floorHolder.Release())

	select {
	case err := <-granted:
		assert.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("waiter not granted after disabling idle gating")
	}
}
