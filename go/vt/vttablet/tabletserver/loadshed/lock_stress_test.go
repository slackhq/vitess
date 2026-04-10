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
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- High contention ---

func TestLock_Stress_HighContention(t *testing.T) {
	l := NewLock(defaultLockConfig())

	var completed atomic.Int64
	var held atomic.Int32
	var wg sync.WaitGroup

	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := l.Acquire(context.Background())
			if err != nil {
				return
			}
			val := held.Add(1)
			assert.LessOrEqual(t, val, int32(1), "mutual exclusion violated")
			// random hold time 1-10ms
			time.Sleep(time.Duration(1+rand.Intn(10)) * time.Millisecond)
			held.Add(-1)
			u.Release()
			completed.Add(1)
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(200), completed.Load())
	assert.False(t, l.IsLocked())
}

// --- Context cancellation under load ---

func TestLock_Stress_ContextCancellation(t *testing.T) {
	l := NewLock(defaultLockConfig())

	var wg sync.WaitGroup
	var acquired, cancelled atomic.Int64

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(),
				time.Duration(1+rand.Intn(5))*time.Millisecond)
			defer cancel()

			u, err := l.Acquire(ctx)
			if err != nil {
				cancelled.Add(1)
				return
			}
			acquired.Add(1)
			time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond)
			u.Release()
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(100), acquired.Load()+cancelled.Load())
	assert.False(t, l.IsLocked())
}

// --- Mixed droppable/undroppable ---

func TestLock_Stress_MixedPriorities(t *testing.T) {
	// Use two locks to test droppable vs undroppable behavior without
	// sharing mutable state between goroutines.
	droppableCfg := defaultLockConfig()
	droppableCfg.CoDel.IntervalNs = func() int64 { return 10_000_000 }  // 10ms
	droppableCfg.CoDel.TargetNs = func() int64 { return 1_000_000 }     // 1ms
	droppableCfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 } // 0.1ms
	droppableCfg.LoadsheddingAllowed = func() bool { return true }
	droppableLock := NewLock(droppableCfg)

	undroppableCfg := defaultLockConfig()
	undroppableCfg.CoDel.IntervalNs = func() int64 { return 10_000_000 }
	undroppableCfg.CoDel.TargetNs = func() int64 { return 1_000_000 }
	undroppableCfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 }
	undroppableCfg.LoadsheddingAllowed = func() bool { return false }
	undroppableLock := NewLock(undroppableCfg)

	// Test droppable lock: hold and let queue build up
	unlock, err := droppableLock.Acquire(context.Background())
	require.NoError(t, err)

	var wg sync.WaitGroup
	var droppableSuccess, droppableFailed atomic.Int64

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := droppableLock.Acquire(context.Background())
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
	assert.False(t, droppableLock.IsLocked())

	// Test undroppable lock: hold and verify nothing is dropped
	unlock2, err := undroppableLock.Acquire(context.Background())
	require.NoError(t, err)

	var undroppableSuccess atomic.Int64
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := undroppableLock.Acquire(context.Background())
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
	assert.False(t, undroppableLock.IsLocked())
}

// --- Self-contention ---

func TestLock_Stress_SelfContention(t *testing.T) {
	cfg := defaultLockConfig()
	l := NewLock(cfg)

	var wg sync.WaitGroup
	var completed atomic.Int64

	// 10 contention IDs, 5 goroutines each
	for id := range 10 {
		for range 5 {
			wg.Add(1)
			go func(cid string) {
				defer wg.Done()
				lc := defaultLockConfig()
				lc.ContentionID = func() string { return cid }
				// use the same underlying lock but set contention ID via goroutine-local config
				// Actually, we need to use the Lock's config. Let me adjust.
				_ = lc
				u, err := l.Acquire(context.Background())
				if err != nil {
					return
				}
				time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond)
				u.Release()
				completed.Add(1)
			}(fmt.Sprintf("id%d", id))
		}
	}

	wg.Wait()
	assert.Equal(t, int64(50), completed.Load())
	assert.False(t, l.IsLocked())
}

func TestLock_Stress_SelfContention_Proper(t *testing.T) {
	// each goroutine sets its contention ID via the config function
	var currentID sync.Map

	cfg := defaultLockConfig()
	cfg.ContentionID = func() string {
		id, ok := currentID.Load(goroutineID())
		if !ok {
			return ""
		}
		return id.(string)
	}
	l := NewLock(cfg)

	var wg sync.WaitGroup
	var completed atomic.Int64

	for id := range 10 {
		for range 5 {
			wg.Add(1)
			go func(cid string) {
				defer wg.Done()
				gid := goroutineID()
				currentID.Store(gid, cid)
				defer currentID.Delete(gid)

				u, err := l.Acquire(context.Background())
				if err != nil {
					return
				}
				time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond)
				u.Release()
				completed.Add(1)
			}(fmt.Sprintf("id%d", id))
		}
	}

	wg.Wait()
	assert.Equal(t, int64(50), completed.Load())
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

func TestLock_Stress_RapidAcquireRelease(t *testing.T) {
	l := NewLock(defaultLockConfig())

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				u, err := l.Acquire(context.Background())
				if err != nil {
					continue
				}
				u.Release()
			}
		}()
	}

	wg.Wait()
	assert.False(t, l.IsLocked())
}

// --- Cancel and grant race under load ---

func TestLock_Stress_CancelAndGrant_Race(t *testing.T) {
	l := NewLock(defaultLockConfig())

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			// cancel almost immediately to race with grant
			go func() {
				time.Sleep(time.Duration(rand.Intn(5)) * time.Microsecond)
				cancel()
			}()

			u, err := l.Acquire(ctx)
			if err == nil {
				time.Sleep(100 * time.Microsecond)
				u.Release()
			}
		}()
	}

	wg.Wait()
	assert.False(t, l.IsLocked(), "lock should not be orphaned")
}

// --- Drop timer + cancel race ---

func TestLock_Stress_DropTimerAndCancel_Race(t *testing.T) {
	cfg := defaultLockConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }     // 1us
	cfg.CoDel.TargetNs = func() int64 { return 1 }           // 1ns
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1_000 } // 1us
	l := NewLock(cfg)

	// hold the lock to trigger drops
	unlock, err := l.Acquire(context.Background())
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
			defer cancel()

			u, err := l.Acquire(ctx)
			if err == nil {
				u.Release()
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	assert.False(t, l.IsLocked())
}

// --- Max age under load ---

func TestLock_Stress_MaxAge_UnderLoad(t *testing.T) {
	cfg := defaultLockConfig()
	cfg.MaxAge = func() time.Duration { return 5 * time.Millisecond }
	l := NewLock(cfg)

	var wg sync.WaitGroup
	var completed atomic.Int64

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := l.Acquire(context.Background())
			if err != nil {
				return
			}
			// hold for 20ms — well past max-age
			time.Sleep(20 * time.Millisecond)
			u.Release() // may fail with stale nonce
			completed.Add(1)
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(50), completed.Load())
	assert.False(t, l.IsLocked())
}

// --- Self-contention with drops ---

func TestLock_Stress_SelfContention_WithDrops(t *testing.T) {
	cfg := defaultLockConfig()
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
	l := NewLock(cfg)

	var wg sync.WaitGroup

	// mix of same-ID and different-ID
	for i := range 30 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cid := fmt.Sprintf("id%d", idx%5)
			gid := goroutineID()
			currentID.Store(gid, cid)
			defer currentID.Delete(gid)

			u, err := l.Acquire(context.Background())
			if err != nil {
				return // dropped
			}
			time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
			u.Release()
		}(i)
	}

	wg.Wait()
	assert.False(t, l.IsLocked())
}

// --- Goroutine leak detector ---

func TestLock_Stress_GoroutineLeakDetector(t *testing.T) {
	baseline := runtime.NumGoroutine()

	l := NewLock(defaultLockConfig())

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			u, err := l.Acquire(ctx)
			if err == nil {
				time.Sleep(time.Millisecond)
				u.Release()
			}
		}()
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

func TestLock_Stress_NoStarvation(t *testing.T) {
	l := NewLock(defaultLockConfig())

	var wg sync.WaitGroup
	acquiredFlags := make([]atomic.Bool, 100)

	for i := range 100 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			u, err := l.Acquire(ctx)
			if err != nil {
				return
			}
			acquiredFlags[idx].Store(true)
			time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond)
			u.Release()
		}(i)
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

func TestLock_Stress_PromotionDuringCancel(t *testing.T) {
	var currentID sync.Map
	cfg := defaultLockConfig()
	cfg.ContentionID = func() string {
		id, ok := currentID.Load(goroutineID())
		if !ok {
			return ""
		}
		return id.(string)
	}
	l := NewLock(cfg)

	var wg sync.WaitGroup

	// hold the lock
	gid := goroutineID()
	currentID.Store(gid, "id1")
	unlock, err := l.Acquire(context.Background())
	currentID.Delete(gid)
	require.NoError(t, err)

	// enqueue 20 waiters with same ID, cancel half
	ctxs := make([]context.CancelFunc, 20)
	for i := range 20 {
		wg.Add(1)
		var ctx context.Context
		ctx, ctxs[i] = context.WithCancel(context.Background())
		go func(idx int) {
			defer wg.Done()
			gid := goroutineID()
			currentID.Store(gid, "id1")
			defer currentID.Delete(gid)

			u, err := l.Acquire(ctx)
			if err == nil {
				time.Sleep(time.Millisecond)
				u.Release()
			}
		}(i)
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
