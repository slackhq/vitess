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
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- High contention ---

func TestSnake_Stress_HighContention(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	var completed atomic.Int64
	var held atomic.Int32
	var wg sync.WaitGroup

	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), "")
			if err != nil {
				return
			}
			val := held.Add(1)
			assert.LessOrEqual(t, val, int32(1), "mutual exclusion violated")
			// random hold time 1-10ms
			time.Sleep(time.Duration(1+rand.IntN(10)) * time.Millisecond)
			held.Add(-1)
			u.Release()
			completed.Add(1)
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(200), completed.Load())
	assert.False(t, s.IsLocked())
}

// --- N-holder stress ---

func TestSnake_Stress_NHolder_CapacityRespected(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 5 }
	s := NewSnake(cfg)

	var held atomic.Int32
	var maxHeld atomic.Int32
	var completed atomic.Int64
	var wg sync.WaitGroup

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), "")
			if err != nil {
				return
			}
			v := held.Add(1)
			for {
				cur := maxHeld.Load()
				if v <= cur || maxHeld.CompareAndSwap(cur, v) {
					break
				}
			}
			time.Sleep(time.Duration(1+rand.IntN(5)) * time.Millisecond)
			held.Add(-1)
			u.Release()
			completed.Add(1)
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(100), completed.Load())
	assert.LessOrEqual(t, maxHeld.Load(), int32(5), "capacity exceeded")
	assert.Greater(t, maxHeld.Load(), int32(1), "capacity not utilized")
	assert.False(t, s.IsLocked())
}

func TestSnake_Stress_NHolder_Sustained(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 3 }
	s := NewSnake(cfg)

	deadline := time.Now().Add(500 * time.Millisecond)
	var held atomic.Int32
	var maxHeld atomic.Int32
	var totalAcquires atomic.Int64
	var wg sync.WaitGroup

	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				u, err := s.Acquire(t.Context(), "")
				if err != nil {
					continue
				}
				v := held.Add(1)
				for {
					cur := maxHeld.Load()
					if v <= cur || maxHeld.CompareAndSwap(cur, v) {
						break
					}
				}
				time.Sleep(time.Duration(500+rand.IntN(1500)) * time.Microsecond)
				held.Add(-1)
				u.Release()
				totalAcquires.Add(1)
			}
		}()
	}

	wg.Wait()
	assert.LessOrEqual(t, maxHeld.Load(), int32(3), "capacity exceeded")
	assert.Greater(t, totalAcquires.Load(), int64(50), "too few acquires")
	assert.False(t, s.IsLocked())
}

// --- Context cancellation under load ---

func TestSnake_Stress_ContextCancellation(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	var wg sync.WaitGroup
	var acquired, cancelled atomic.Int64

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(t.Context(),
				time.Duration(1+rand.IntN(5))*time.Millisecond)
			defer cancel()

			u, err := s.Acquire(ctx, "")
			if err != nil {
				cancelled.Add(1)
				return
			}
			acquired.Add(1)
			time.Sleep(time.Duration(1+rand.IntN(3)) * time.Millisecond)
			u.Release()
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(100), acquired.Load()+cancelled.Load())
	assert.False(t, s.IsLocked())
}

// --- Mixed droppable/undroppable ---

func TestSnake_Stress_MixedPriorities(t *testing.T) {
	droppableCfg := defaultSnakeConfig()
	droppableCfg.CoDel.IntervalNs = func() int64 { return 10_000_000 }  // 10ms
	droppableCfg.CoDel.TargetNs = func() int64 { return 1_000_000 }     // 1ms
	droppableCfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 } // 0.1ms
	droppableCfg.LoadsheddingAllowed = func() bool { return true }
	droppableSnake := NewSnake(droppableCfg)

	undroppableCfg := defaultSnakeConfig()
	undroppableCfg.CoDel.IntervalNs = func() int64 { return 10_000_000 }
	undroppableCfg.CoDel.TargetNs = func() int64 { return 1_000_000 }
	undroppableCfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 }
	undroppableCfg.LoadsheddingAllowed = func() bool { return false }
	undroppableSnake := NewSnake(undroppableCfg)

	unlock, err := droppableSnake.Acquire(t.Context(), "")
	require.NoError(t, err)

	var wg sync.WaitGroup
	var droppableSuccess, droppableFailed atomic.Int64

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := droppableSnake.Acquire(t.Context(), "")
			if err != nil {
				droppableFailed.Add(1)
				return
			}
			droppableSuccess.Add(1)
			u.Release()
		}()
	}

	time.Sleep(200 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	assert.Equal(t, int64(50), droppableSuccess.Load()+droppableFailed.Load())
	assert.False(t, droppableSnake.IsLocked())

	unlock2, err := undroppableSnake.Acquire(t.Context(), "")
	require.NoError(t, err)

	var undroppableSuccess atomic.Int64
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := undroppableSnake.Acquire(t.Context(), "")
			if err != nil {
				return
			}
			undroppableSuccess.Add(1)
			u.Release()
		}()
	}

	time.Sleep(200 * time.Millisecond)
	unlock2.Release()
	wg.Wait()

	assert.Equal(t, int64(50), undroppableSuccess.Load(), "undroppable requests should never be dropped")
	assert.False(t, undroppableSnake.IsLocked())
}

// --- Self-contention ---

func TestSnake_Stress_SelfContention_MutualExclusion(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	var wg sync.WaitGroup
	var globalHeld atomic.Int32
	var globalMax atomic.Int32
	var completed atomic.Int64

	type perID struct {
		held atomic.Int32
		max  atomic.Int32
	}
	ids := make([]*perID, 10)
	for i := range ids {
		ids[i] = &perID{}
	}

	for id := range 10 {
		for range 10 {
			cid := fmt.Sprintf("id%d", id)
			pid := ids[id]
			wg.Add(1)
			go func() {
				defer wg.Done()
				u, err := s.Acquire(t.Context(), cid)
				if err != nil {
					return
				}

				gv := globalHeld.Add(1)
				if gv > globalMax.Load() {
					globalMax.Store(gv)
				}
				pv := pid.held.Add(1)
				if pv > pid.max.Load() {
					pid.max.Store(pv)
				}

				time.Sleep(time.Duration(1+rand.IntN(5)) * time.Millisecond)

				pid.held.Add(-1)
				globalHeld.Add(-1)
				u.Release()
				completed.Add(1)
			}()
		}
	}

	wg.Wait()
	assert.Equal(t, int64(100), completed.Load())
	assert.LessOrEqual(t, globalMax.Load(), int32(1), "mutual exclusion violated")
	for id, pid := range ids {
		assert.LessOrEqual(t, pid.max.Load(), int32(1),
			"contention ID %d had concurrent holders", id)
	}
	assert.False(t, s.IsLocked())
}

func TestSnake_Stress_SelfContention_ValveSerializationOrder(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context(), "order-test")
	require.NoError(t, err)

	const n = 20
	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup

	for i := range n {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), "order-test")
			if err != nil {
				return
			}
			mu.Lock()
			order = append(order, idx)
			mu.Unlock()
			u.Release()
		}()
	}

	time.Sleep(50 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	expected := make([]int, n)
	for i := range n {
		expected[i] = i
	}
	assert.Equal(t, expected, order, "valve should preserve FIFO order within contention ID")
	assert.False(t, s.IsLocked())
}

func TestSnake_Stress_SelfContention_DropPromotionChain(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }     // 1us
	cfg.CoDel.TargetNs = func() int64 { return 1 }           // 1ns
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1_000 } // 1us
	s := NewSnake(cfg)

	unlock, err := s.Acquire(t.Context(), "holder")
	require.NoError(t, err)

	const numIDs = 5
	const perID = 10
	type result struct {
		id      int
		granted bool
	}
	results := make(chan result, numIDs*perID)

	var wg sync.WaitGroup
	for id := range numIDs {
		for range perID {
			cid := fmt.Sprintf("drop-id%d", id)
			idx := id
			wg.Add(1)
			go func() {
				defer wg.Done()
				u, err := s.Acquire(t.Context(), cid)
				if err != nil {
					results <- result{id: idx, granted: false}
					return
				}
				time.Sleep(time.Duration(1+rand.IntN(3)) * time.Millisecond)
				u.Release()
				results <- result{id: idx, granted: true}
			}()
		}
	}

	time.Sleep(100 * time.Millisecond)
	unlock.Release()
	wg.Wait()
	close(results)

	granted := make([]int, numIDs)
	dropped := make([]int, numIDs)
	for r := range results {
		if r.granted {
			granted[r.id]++
		} else {
			dropped[r.id]++
		}
	}

	for id := range numIDs {
		total := granted[id] + dropped[id]
		assert.Equal(t, perID, total,
			"contention ID %d: granted(%d) + dropped(%d) != total(%d)",
			id, granted[id], dropped[id], perID)
	}
	assert.False(t, s.IsLocked())
}

func TestSnake_Stress_SelfContention_CancelInValve(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context(), "cancel-test")
	require.NoError(t, err)

	const n = 20
	ctxs := make([]context.Context, n)
	cancels := make([]context.CancelFunc, n)
	results := make([]chan error, n)

	var wg sync.WaitGroup
	for i := range n {
		ctxs[i], cancels[i] = context.WithCancel(t.Context())
		results[i] = make(chan error, 1)
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(ctxs[idx], "cancel-test")
			if err != nil {
				results[idx] <- err
				return
			}
			time.Sleep(time.Millisecond)
			u.Release()
			results[idx] <- nil
		}()
		time.Sleep(2 * time.Millisecond)
	}

	time.Sleep(20 * time.Millisecond)

	for i := 0; i < n; i += 2 {
		cancels[i]()
	}

	time.Sleep(20 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	for i := range n {
		select {
		case err := <-results[i]:
			if i%2 == 0 {
				assert.ErrorIs(t, err, context.Canceled,
					"waiter %d should have been cancelled", i)
			} else {
				assert.NoError(t, err,
					"waiter %d should have been granted", i)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("waiter %d did not return", i)
		}
	}
	assert.False(t, s.IsLocked())
}

func TestSnake_Stress_SelfContention_MixedCancelDropGrant(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 5_000_000 }   // 5ms
	cfg.CoDel.TargetNs = func() int64 { return 500_000 }       // 0.5ms
	cfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 } // 0.1ms
	cfg.MaxAge = func() time.Duration { return 50 * time.Millisecond }
	s := NewSnake(cfg)

	const numIDs = 5
	const perID = 8
	const total = numIDs * perID

	var granted, dropped, cancelled atomic.Int64
	var wg sync.WaitGroup

	for id := range numIDs {
		for range perID {
			cid := fmt.Sprintf("mix-id%d", id)
			wg.Add(1)
			go func() {
				defer wg.Done()
				timeout := time.Duration(1+rand.IntN(20)) * time.Millisecond
				ctx, cancel := context.WithTimeout(t.Context(), timeout)
				defer cancel()

				u, err := s.Acquire(ctx, cid)
				if err != nil {
					if ctx.Err() != nil {
						cancelled.Add(1)
					} else {
						dropped.Add(1)
					}
					return
				}
				time.Sleep(time.Duration(1+rand.IntN(5)) * time.Millisecond)
				u.Release()
				granted.Add(1)
			}()
		}
	}

	wg.Wait()

	g, d, c := granted.Load(), dropped.Load(), cancelled.Load()
	assert.Equal(t, int64(total), g+d+c,
		"granted(%d) + dropped(%d) + cancelled(%d) != total(%d)", g, d, c, total)
	assert.False(t, s.IsLocked())
}

func TestSnake_Stress_SelfContention_HighConcurrency_Sustained(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	const numIDs = 5
	const goroutinesPerID = 4
	deadline := time.Now().Add(500 * time.Millisecond)

	var globalHeld atomic.Int32
	var globalMax atomic.Int32
	var totalAcquires atomic.Int64
	var wg sync.WaitGroup

	for id := range numIDs {
		for range goroutinesPerID {
			cid := fmt.Sprintf("sustained-id%d", id)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					u, err := s.Acquire(t.Context(), cid)
					if err != nil {
						continue
					}

					v := globalHeld.Add(1)
					if v > globalMax.Load() {
						globalMax.Store(v)
					}

					time.Sleep(time.Duration(500+rand.IntN(1500)) * time.Microsecond)

					globalHeld.Add(-1)
					u.Release()
					totalAcquires.Add(1)
				}
			}()
		}
	}

	wg.Wait()
	assert.LessOrEqual(t, globalMax.Load(), int32(1), "mutual exclusion violated")
	assert.Greater(t, totalAcquires.Load(), int64(50),
		"too few acquires — test may not be exercising contention")
	assert.False(t, s.IsLocked())
}

// --- Rapid acquire/release ---

func TestSnake_Stress_RapidAcquireRelease(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				u, err := s.Acquire(t.Context(), "")
				if err != nil {
					continue
				}
				u.Release()
			}
		}()
	}

	wg.Wait()
	assert.False(t, s.IsLocked())
}

// --- Cancel and grant race under load ---

func TestSnake_Stress_CancelAndGrant_Race(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(t.Context())
			go func() {
				time.Sleep(time.Duration(rand.IntN(5)) * time.Microsecond)
				cancel()
			}()

			u, err := s.Acquire(ctx, "")
			if err == nil {
				time.Sleep(100 * time.Microsecond)
				u.Release()
			}
		}()
	}

	wg.Wait()
	assert.False(t, s.IsLocked(), "lock should not be orphaned")
}

// --- Drop timer + cancel race ---

func TestSnake_Stress_DropTimerAndCancel_Race(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }     // 1us
	cfg.CoDel.TargetNs = func() int64 { return 1 }           // 1ns
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1_000 } // 1us
	s := NewSnake(cfg)

	unlock, err := s.Acquire(t.Context(), "")
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
			defer cancel()

			u, err := s.Acquire(ctx, "")
			if err == nil {
				u.Release()
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	assert.False(t, s.IsLocked())
}

// --- Max age under load ---

func TestSnake_Stress_MaxAge_UnderLoad(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.MaxAge = func() time.Duration { return 5 * time.Millisecond }
	s := NewSnake(cfg)

	var wg sync.WaitGroup
	var completed atomic.Int64

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), "")
			if err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
			u.Release()
			completed.Add(1)
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(50), completed.Load())
	assert.False(t, s.IsLocked())
}

// --- Self-contention with drops ---

func TestSnake_Stress_SelfContention_WithDrops(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 10_000_000 }  // 10ms
	cfg.CoDel.TargetNs = func() int64 { return 1_000_000 }     // 1ms
	cfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 } // 0.1ms
	s := NewSnake(cfg)

	var wg sync.WaitGroup

	for i := range 30 {
		cid := fmt.Sprintf("id%d", i%5)
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), cid)
			if err != nil {
				return
			}
			time.Sleep(time.Duration(1+rand.IntN(5)) * time.Millisecond)
			u.Release()
		}()
	}

	wg.Wait()
	assert.False(t, s.IsLocked())
}

// --- Goroutine leak detector ---

func TestSnake_Stress_GoroutineLeakDetector(t *testing.T) {
	baseline := runtime.NumGoroutine()

	s := NewSnake(defaultSnakeConfig())

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
			defer cancel()
			u, err := s.Acquire(ctx, "")
			if err == nil {
				time.Sleep(time.Millisecond)
				u.Release()
			}
		}()
	}

	wg.Wait()
	time.Sleep(50 * time.Millisecond)

	assert.Eventually(t, func() bool {
		current := runtime.NumGoroutine()
		return current <= baseline+5
	}, 2*time.Second, 50*time.Millisecond, "goroutine leak detected")
}

// --- No starvation ---

func TestSnake_Stress_NoStarvation(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	var wg sync.WaitGroup
	acquiredFlags := make([]atomic.Bool, 100)

	for i := range 100 {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			u, err := s.Acquire(ctx, "")
			if err != nil {
				return
			}
			acquiredFlags[idx].Store(true)
			time.Sleep(time.Duration(1+rand.IntN(3)) * time.Millisecond)
			u.Release()
		}()
	}

	wg.Wait()

	acquired := 0
	for i := range acquiredFlags {
		if acquiredFlags[i].Load() {
			acquired++
		}
	}
	assert.Equal(t, 100, acquired, "some goroutines were starved")
}

// --- Promotion during cancel ---

func TestSnake_Stress_PromotionDuringCancel(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context(), "id1")
	require.NoError(t, err)

	var wg sync.WaitGroup

	ctxs := make([]context.CancelFunc, 20)
	for i := range 20 {
		var ctx context.Context
		ctx, ctxs[i] = context.WithCancel(t.Context())
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(ctx, "id1")
			if err == nil {
				time.Sleep(time.Millisecond)
				u.Release()
			}
		}()
	}

	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 20; i += 2 {
		ctxs[i]()
	}

	time.Sleep(10 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	assert.False(t, s.IsLocked())
}
