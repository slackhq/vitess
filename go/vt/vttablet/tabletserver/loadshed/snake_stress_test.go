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
	l := NewSnake(defaultSnakeConfig())

	var completed atomic.Int64
	var held atomic.Int32
	var wg sync.WaitGroup

	for range 200 {
		wg.Go(func() {
			u, err := l.Acquire(t.Context())
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
		})
	}

	wg.Wait()
	assert.Equal(t, int64(200), completed.Load())
	assert.False(t, l.IsLocked())
}

// --- Context cancellation under load ---

func TestSnake_Stress_ContextCancellation(t *testing.T) {
	l := NewSnake(defaultSnakeConfig())

	var wg sync.WaitGroup
	var acquired, cancelled atomic.Int64

	for range 100 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(),
				time.Duration(1+rand.IntN(5))*time.Millisecond)
			defer cancel()

			u, err := l.Acquire(ctx)
			if err != nil {
				cancelled.Add(1)
				return
			}
			acquired.Add(1)
			time.Sleep(time.Duration(1+rand.IntN(3)) * time.Millisecond)
			u.Release()
		})
	}

	wg.Wait()
	assert.Equal(t, int64(100), acquired.Load()+cancelled.Load())
	assert.False(t, l.IsLocked())
}

// --- Mixed droppable/undroppable ---

func TestSnake_Stress_MixedPriorities(t *testing.T) {
	// Use two gates to test droppable vs undroppable behavior without
	// sharing mutable state between goroutines.
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

	// Test droppable gate: hold and let queue build up
	unlock, err := droppableSnake.Acquire(t.Context())
	require.NoError(t, err)

	var wg sync.WaitGroup
	var droppableSuccess, droppableFailed atomic.Int64

	for range 50 {
		wg.Go(func() {
			u, err := droppableSnake.Acquire(t.Context())
			if err != nil {
				droppableFailed.Add(1)
				return
			}
			droppableSuccess.Add(1)
			u.Release()
		})
	}

	time.Sleep(200 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	assert.Equal(t, int64(50), droppableSuccess.Load()+droppableFailed.Load())
	assert.False(t, droppableSnake.IsLocked())

	// Test undroppable gate: hold and verify nothing is dropped
	unlock2, err := undroppableSnake.Acquire(t.Context())
	require.NoError(t, err)

	var undroppableSuccess atomic.Int64
	for range 50 {
		wg.Go(func() {
			u, err := undroppableSnake.Acquire(t.Context())
			if err != nil {
				return
			}
			undroppableSuccess.Add(1)
			u.Release()
		})
	}

	time.Sleep(200 * time.Millisecond)
	unlock2.Release()
	wg.Wait()

	assert.Equal(t, int64(50), undroppableSuccess.Load(), "undroppable requests should never be dropped")
	assert.False(t, undroppableSnake.IsLocked())
}

// --- Self-contention ---

// selfContentionSnake creates a Snake whose ContentionID is read from a
// goroutine-keyed sync.Map. Callers store their contention ID before
// calling Acquire and delete it afterward.
func selfContentionSnake(cfg SnakeConfig) (*Snake, *sync.Map) {
	var currentID sync.Map
	cfg.ContentionID = func() string {
		id, ok := currentID.Load(goroutineID())
		if !ok {
			return ""
		}
		return id.(string)
	}
	return NewSnake(cfg), &currentID
}

func TestSnake_Stress_SelfContention_MutualExclusion(t *testing.T) {
	l, currentID := selfContentionSnake(defaultSnakeConfig())

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
			wg.Go(func() {
				gid := goroutineID()
				currentID.Store(gid, cid)
				defer currentID.Delete(gid)

				u, err := l.Acquire(t.Context())
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
			})
		}
	}

	wg.Wait()
	assert.Equal(t, int64(100), completed.Load())
	assert.LessOrEqual(t, globalMax.Load(), int32(1), "mutual exclusion violated")
	for id, pid := range ids {
		assert.LessOrEqual(t, pid.max.Load(), int32(1),
			"contention ID %d had concurrent holders", id)
	}
	assert.False(t, l.IsLocked())
}

func TestSnake_Stress_SelfContention_ValveSerializationOrder(t *testing.T) {
	l, currentID := selfContentionSnake(defaultSnakeConfig())

	// Hold the gate so all 20 goroutines enqueue before any are granted.
	gid := goroutineID()
	currentID.Store(gid, "order-test")
	unlock, err := l.Acquire(t.Context())
	currentID.Delete(gid)
	require.NoError(t, err)

	const n = 20
	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup

	for i := range n {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Go(func() {
			gid := goroutineID()
			currentID.Store(gid, "order-test")
			defer currentID.Delete(gid)

			u, err := l.Acquire(t.Context())
			if err != nil {
				return
			}
			mu.Lock()
			order = append(order, idx)
			mu.Unlock()
			u.Release()
		})
	}

	time.Sleep(50 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	expected := make([]int, n)
	for i := range n {
		expected[i] = i
	}
	assert.Equal(t, expected, order, "valve should preserve FIFO order within contention ID")
	assert.False(t, l.IsLocked())
}

func TestSnake_Stress_SelfContention_DropPromotionChain(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }     // 1us
	cfg.CoDel.TargetNs = func() int64 { return 1 }           // 1ns
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1_000 } // 1us
	l, currentID := selfContentionSnake(cfg)

	// Hold the gate to build queue pressure.
	gid := goroutineID()
	currentID.Store(gid, "holder")
	unlock, err := l.Acquire(t.Context())
	currentID.Delete(gid)
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
			wg.Go(func() {
				gid := goroutineID()
				currentID.Store(gid, cid)
				defer currentID.Delete(gid)

				u, err := l.Acquire(t.Context())
				if err != nil {
					results <- result{id: idx, granted: false}
					return
				}
				time.Sleep(time.Duration(1+rand.IntN(3)) * time.Millisecond)
				u.Release()
				results <- result{id: idx, granted: true}
			})
		}
	}

	// Hold long enough for CoDel to start dropping.
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
	assert.False(t, l.IsLocked())
}

func TestSnake_Stress_SelfContention_CancelInValve(t *testing.T) {
	l, currentID := selfContentionSnake(defaultSnakeConfig())

	// Hold the gate so all waiters queue up.
	gid := goroutineID()
	currentID.Store(gid, "cancel-test")
	unlock, err := l.Acquire(t.Context())
	currentID.Delete(gid)
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
		wg.Go(func() {
			gid := goroutineID()
			currentID.Store(gid, "cancel-test")
			defer currentID.Delete(gid)

			u, err := l.Acquire(ctxs[idx])
			if err != nil {
				results[idx] <- err
				return
			}
			time.Sleep(time.Millisecond)
			u.Release()
			results[idx] <- nil
		})
		time.Sleep(2 * time.Millisecond)
	}

	time.Sleep(20 * time.Millisecond)

	// Cancel every other waiter.
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
	assert.False(t, l.IsLocked())
}

func TestSnake_Stress_SelfContention_MixedCancelDropGrant(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 5_000_000 }   // 5ms
	cfg.CoDel.TargetNs = func() int64 { return 500_000 }       // 0.5ms
	cfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 } // 0.1ms
	cfg.MaxAge = func() time.Duration { return 50 * time.Millisecond }
	l, currentID := selfContentionSnake(cfg)

	const numIDs = 5
	const perID = 8
	const total = numIDs * perID

	var granted, dropped, cancelled atomic.Int64
	var wg sync.WaitGroup

	for id := range numIDs {
		for range perID {
			cid := fmt.Sprintf("mix-id%d", id)
			wg.Go(func() {
				gid := goroutineID()
				currentID.Store(gid, cid)
				defer currentID.Delete(gid)

				timeout := time.Duration(1+rand.IntN(20)) * time.Millisecond
				ctx, cancel := context.WithTimeout(t.Context(), timeout)
				defer cancel()

				u, err := l.Acquire(ctx)
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
			})
		}
	}

	wg.Wait()

	g, d, c := granted.Load(), dropped.Load(), cancelled.Load()
	assert.Equal(t, int64(total), g+d+c,
		"granted(%d) + dropped(%d) + cancelled(%d) != total(%d)", g, d, c, total)
	assert.False(t, l.IsLocked())
}

func TestSnake_Stress_SelfContention_HighConcurrency_Sustained(t *testing.T) {
	l, currentID := selfContentionSnake(defaultSnakeConfig())

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
			wg.Go(func() {
				gid := goroutineID()
				currentID.Store(gid, cid)
				defer currentID.Delete(gid)

				for time.Now().Before(deadline) {
					u, err := l.Acquire(t.Context())
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
			})
		}
	}

	wg.Wait()
	assert.LessOrEqual(t, globalMax.Load(), int32(1), "mutual exclusion violated")
	assert.Greater(t, totalAcquires.Load(), int64(50),
		"too few acquires — test may not be exercising contention")
	assert.False(t, l.IsLocked())
}

// goroutineID returns a unique identifier for the current goroutine.
func goroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	var id int64
	for i := len("goroutine "); i < n; i++ {
		if buf[i] < '0' || buf[i] > '9' {
			break
		}
		id = id*10 + int64(buf[i]-'0')
	}
	return id
}

// --- Rapid acquire/release ---

func TestSnake_Stress_RapidAcquireRelease(t *testing.T) {
	l := NewSnake(defaultSnakeConfig())

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 100 {
				u, err := l.Acquire(t.Context())
				if err != nil {
					continue
				}
				u.Release()
			}
		})
	}

	wg.Wait()
	assert.False(t, l.IsLocked())
}

// --- Cancel and grant race under load ---

func TestSnake_Stress_CancelAndGrant_Race(t *testing.T) {
	l := NewSnake(defaultSnakeConfig())

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			ctx, cancel := context.WithCancel(t.Context())
			// cancel almost immediately to race with grant
			go func() {
				time.Sleep(time.Duration(rand.IntN(5)) * time.Microsecond)
				cancel()
			}()

			u, err := l.Acquire(ctx)
			if err == nil {
				time.Sleep(100 * time.Microsecond)
				u.Release()
			}
		})
	}

	wg.Wait()
	assert.False(t, l.IsLocked(), "gate should not be orphaned")
}

// --- Drop timer + cancel race ---

func TestSnake_Stress_DropTimerAndCancel_Race(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }     // 1us
	cfg.CoDel.TargetNs = func() int64 { return 1 }           // 1ns
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1_000 } // 1us
	l := NewSnake(cfg)

	// hold the gate to trigger drops
	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
			defer cancel()

			u, err := l.Acquire(ctx)
			if err == nil {
				u.Release()
			}
		})
	}

	time.Sleep(20 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	assert.False(t, l.IsLocked())
}

// --- Max age under load ---

func TestSnake_Stress_MaxAge_UnderLoad(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.MaxAge = func() time.Duration { return 5 * time.Millisecond }
	l := NewSnake(cfg)

	var wg sync.WaitGroup
	var completed atomic.Int64

	for range 50 {
		wg.Go(func() {
			u, err := l.Acquire(t.Context())
			if err != nil {
				return
			}
			// hold for 20ms — well past max-age
			time.Sleep(20 * time.Millisecond)
			u.Release() // may fail with stale nonce
			completed.Add(1)
		})
	}

	wg.Wait()
	assert.Equal(t, int64(50), completed.Load())
	assert.False(t, l.IsLocked())
}

// --- Self-contention with drops ---

func TestSnake_Stress_SelfContention_WithDrops(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 10_000_000 }  // 10ms
	cfg.CoDel.TargetNs = func() int64 { return 1_000_000 }     // 1ms
	cfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 } // 0.1ms

	var currentID sync.Map
	cfg.ContentionID = func() string {
		id, ok := currentID.Load(goroutineID())
		if !ok {
			return ""
		}
		return id.(string)
	}
	l := NewSnake(cfg)

	var wg sync.WaitGroup

	// mix of same-ID and different-ID
	for i := range 30 {
		idx := i
		wg.Go(func() {
			cid := fmt.Sprintf("id%d", idx%5)
			gid := goroutineID()
			currentID.Store(gid, cid)
			defer currentID.Delete(gid)

			u, err := l.Acquire(t.Context())
			if err != nil {
				return // dropped
			}
			time.Sleep(time.Duration(1+rand.IntN(5)) * time.Millisecond)
			u.Release()
		})
	}

	wg.Wait()
	assert.False(t, l.IsLocked())
}

// --- Goroutine leak detector ---

func TestSnake_Stress_GoroutineLeakDetector(t *testing.T) {
	baseline := runtime.NumGoroutine()

	l := NewSnake(defaultSnakeConfig())

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
			defer cancel()
			u, err := l.Acquire(ctx)
			if err == nil {
				time.Sleep(time.Millisecond)
				u.Release()
			}
		})
	}

	wg.Wait()
	// allow time for timers to clean up
	time.Sleep(50 * time.Millisecond)

	assert.Eventually(t, func() bool {
		current := runtime.NumGoroutine()
		return current <= baseline+5 // small margin for runtime goroutines
	}, 2*time.Second, 50*time.Millisecond, "goroutine leak detected")
}

// --- No starvation ---

func TestSnake_Stress_NoStarvation(t *testing.T) {
	l := NewSnake(defaultSnakeConfig())

	var wg sync.WaitGroup
	acquiredFlags := make([]atomic.Bool, 100)

	for i := range 100 {
		idx := i
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			u, err := l.Acquire(ctx)
			if err != nil {
				return
			}
			acquiredFlags[idx].Store(true)
			time.Sleep(time.Duration(1+rand.IntN(3)) * time.Millisecond)
			u.Release()
		})
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
	var currentID sync.Map
	cfg := defaultSnakeConfig()
	cfg.ContentionID = func() string {
		id, ok := currentID.Load(goroutineID())
		if !ok {
			return ""
		}
		return id.(string)
	}
	l := NewSnake(cfg)

	var wg sync.WaitGroup

	// hold the gate
	gid := goroutineID()
	currentID.Store(gid, "id1")
	unlock, err := l.Acquire(t.Context())
	currentID.Delete(gid)
	require.NoError(t, err)

	// enqueue 20 waiters with same ID, cancel half
	ctxs := make([]context.CancelFunc, 20)
	for i := range 20 {
		var ctx context.Context
		ctx, ctxs[i] = context.WithCancel(t.Context())
		wg.Go(func() {
			gid := goroutineID()
			currentID.Store(gid, "id1")
			defer currentID.Delete(gid)

			u, err := l.Acquire(ctx)
			if err == nil {
				time.Sleep(time.Millisecond)
				u.Release()
			}
		})
	}

	time.Sleep(10 * time.Millisecond)

	// cancel every other waiter
	for i := 0; i < 20; i += 2 {
		ctxs[i]()
	}

	time.Sleep(10 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	assert.False(t, l.IsLocked())
}
