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

// --- N-holder: concurrent grants up to capacity ---

func TestSnake_NHolder_ConcurrentGrants(t *testing.T) {
	for _, cap := range []int{2, 5, 10} {
		t.Run(fmt.Sprintf("Cap%d", cap), func(t *testing.T) {
			cfg := defaultSnakeConfig()
			cfg.Capacity = func() int { return cap }
			s := NewSnake(cfg)

			unlocks := make([]*SafeUnlock, cap)
			for i := range cap {
				u, err := s.Acquire(t.Context(), "")
				require.NoError(t, err, "acquire %d should succeed (capacity=%d)", i, cap)
				unlocks[i] = u
			}

			assert.Equal(t, cap, s.InFlight())

			for _, u := range unlocks {
				u.Release()
			}
			assert.Equal(t, 0, s.InFlight())
		})
	}
}

// --- N-holder: blocks at capacity, unblocks on release ---

func TestSnake_NHolder_BlocksAtCapacity(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 3 }
	s := NewSnake(cfg)

	unlocks := make([]*SafeUnlock, 3)
	for i := range 3 {
		u, err := s.Acquire(t.Context(), "")
		require.NoError(t, err)
		unlocks[i] = u
	}

	acquired := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context(), "")
		if err == nil {
			close(acquired)
			u.Release()
		}
	}()

	select {
	case <-acquired:
		t.Fatal("should not acquire when at capacity")
	case <-time.After(20 * time.Millisecond):
	}

	unlocks[0].Release()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("should acquire after one slot freed")
	}
}

// --- N-holder: parallel throughput (all slots saturated) ---

func TestSnake_NHolder_ParallelThroughput(t *testing.T) {
	const capacity = 4
	const workers = 16
	const opsPerWorker = 50

	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return capacity }
	s := NewSnake(cfg)

	var maxConcurrent atomic.Int64
	var ops atomic.Int64
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range opsPerWorker {
				u, err := s.Acquire(t.Context(), "")
				if err != nil {
					continue
				}
				cur := int64(s.InFlight())
				for {
					old := maxConcurrent.Load()
					if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
						break
					}
				}
				ops.Add(1)
				u.Release()
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(workers*opsPerWorker), ops.Load())
	assert.LessOrEqual(t, maxConcurrent.Load(), int64(capacity),
		"max concurrent should never exceed capacity")
}

// --- N-holder: dynamic capacity increase ---

func TestSnake_NHolder_DynamicCapacityIncrease(t *testing.T) {
	var cap atomic.Int64
	cap.Store(1)

	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return int(cap.Load()) }
	s := NewSnake(cfg)

	u1, err := s.Acquire(t.Context(), "")
	require.NoError(t, err)

	acquired := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context(), "")
		if err == nil {
			close(acquired)
			u.Release()
		}
	}()

	select {
	case <-acquired:
		t.Fatal("should block at capacity=1")
	case <-time.After(20 * time.Millisecond):
	}

	cap.Store(2)
	u1.Release()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("should unblock after capacity increase + release")
	}
}

// --- N-holder: dynamic capacity decrease doesn't evict ---

func TestSnake_NHolder_DynamicCapacityDecrease(t *testing.T) {
	var cap atomic.Int64
	cap.Store(4)

	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return int(cap.Load()) }
	s := NewSnake(cfg)

	unlocks := make([]*SafeUnlock, 4)
	for i := range 4 {
		u, err := s.Acquire(t.Context(), "")
		require.NoError(t, err)
		unlocks[i] = u
	}

	cap.Store(2)
	assert.Equal(t, 4, s.InFlight(), "existing holders are not evicted")

	acquired := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context(), "")
		if err == nil {
			close(acquired)
			u.Release()
		}
	}()

	unlocks[0].Release()
	unlocks[1].Release()

	select {
	case <-acquired:
		t.Fatal("should still block (inFlight=2 == new capacity=2)")
	case <-time.After(20 * time.Millisecond):
	}

	unlocks[2].Release()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("should acquire once below new capacity")
	}

	unlocks[3].Release()
}

// --- N-holder: valve serialization with multiple slots ---

func TestSnake_NHolder_ValveSerialization(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 3 }
	s := NewSnake(cfg)

	u1, err := s.Acquire(t.Context(), "valve-a")
	require.NoError(t, err)
	u2, err := s.Acquire(t.Context(), "valve-b")
	require.NoError(t, err)
	u3, err := s.Acquire(t.Context(), "valve-c")
	require.NoError(t, err)
	assert.Equal(t, 3, s.InFlight())

	results := make(chan string, 6)
	var wg sync.WaitGroup
	for _, id := range []string{"valve-a", "valve-b", "valve-c"} {
		for range 2 {
			vid := id
			wg.Add(1)
			go func() {
				defer wg.Done()
				u, err := s.Acquire(t.Context(), vid)
				if err == nil {
					results <- vid
					u.Release()
				}
			}()
		}
	}

	time.Sleep(10 * time.Millisecond)
	u1.Release()
	u2.Release()
	u3.Release()
	wg.Wait()
	close(results)

	var count int
	for range results {
		count++
	}
	assert.Equal(t, 6, count, "all valve-serialized requests should complete")
	assert.Equal(t, 0, s.InFlight())
}

// --- N-holder: InFlight accuracy under concurrency ---

func TestSnake_NHolder_InFlightAccuracy(t *testing.T) {
	const capacity = 5
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return capacity }
	s := NewSnake(cfg)

	var wg sync.WaitGroup
	var violations atomic.Int64

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), "")
			if err != nil {
				return
			}
			if s.InFlight() > capacity {
				violations.Add(1)
			}
			u.Release()
		}()
	}

	wg.Wait()
	assert.Zero(t, violations.Load(), "InFlight must never exceed capacity")
	assert.Equal(t, 0, s.InFlight())
}

// --- N-holder: context cancel frees slot for waiter ---

func TestSnake_NHolder_ContextCancelFreesSlot(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 2 }
	s := NewSnake(cfg)

	ctx1, cancel1 := context.WithCancel(t.Context())
	u1, err := s.Acquire(ctx1, "")
	require.NoError(t, err)

	u2, err := s.Acquire(t.Context(), "")
	require.NoError(t, err)

	waiterDone := make(chan error, 1)
	go func() {
		u, err := s.Acquire(t.Context(), "")
		if err == nil {
			u.Release()
		}
		waiterDone <- err
	}()

	time.Sleep(10 * time.Millisecond)
	cancel1()
	u1.Release()

	select {
	case err := <-waiterDone:
		assert.NoError(t, err, "waiter should get slot after cancel+release")
	case <-time.After(2 * time.Second):
		t.Fatal("waiter should have been granted")
	}

	u2.Release()
}

// --- N-holder: max-age with multiple holders ---

func TestSnake_NHolder_MaxAge_MultipleHolders(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 2 }
	cfg.MaxAge = func() time.Duration { return 20 * time.Millisecond }
	s := NewSnake(cfg)

	u1, err := s.Acquire(t.Context(), "")
	require.NoError(t, err)
	u2, err := s.Acquire(t.Context(), "")
	require.NoError(t, err)

	waiterDone := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context(), "")
		if err == nil {
			close(waiterDone)
			u.Release()
		}
	}()

	select {
	case <-waiterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("max-age should free a slot")
	}

	u1.Release()
	u2.Release()

	assert.Eventually(t, func() bool {
		return s.InFlight() == 0
	}, 1*time.Second, 5*time.Millisecond)
}

// --- N-holder: release callbacks fire for each release ---

func TestSnake_NHolder_ReleaseCallbacks(t *testing.T) {
	var count atomic.Int64
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 3 }
	cfg.ReleaseCBs = []func(error){
		func(_ error) { count.Add(1) },
	}
	s := NewSnake(cfg)

	unlocks := make([]*SafeUnlock, 3)
	for i := range 3 {
		u, err := s.Acquire(t.Context(), "")
		require.NoError(t, err)
		unlocks[i] = u
	}

	for _, u := range unlocks {
		u.Release()
	}

	assert.Equal(t, int64(3), count.Load(), "release callback should fire for each holder")
}

// --- N-holder: memory cleanup with multi-slot ---

func TestSnake_NHolder_MemoryCleanup(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 5 }
	s := NewSnake(cfg)

	for range 500 {
		unlocks := make([]*SafeUnlock, 5)
		for i := range 5 {
			u, err := s.Acquire(t.Context(), fmt.Sprintf("id-%d", i))
			require.NoError(t, err)
			unlocks[i] = u
		}
		for _, u := range unlocks {
			u.Release()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	assert.Equal(t, 0, s.inFlight)
	assert.Empty(t, s.holders)
	assert.Equal(t, 0, s.q.lockedLen())
	assert.Empty(t, s.q.pendingRequests)
	assert.Empty(t, s.q.outstandingCounts)
	assert.Empty(t, s.q.activePerValve)
}
