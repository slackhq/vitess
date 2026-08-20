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
			u, err := s.Acquire(t.Context(), 0)
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
	assert.Equal(t, int64(200), completed.Load(), "ungated: healthy contention sheds nothing")
	assert.True(t, s.isIdle())
}

func TestSnake_Stress_HighContention_TriggerGated(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.DropMode = func() CoDelDropMode { return DropJumpStart }
	s := NewSnake(cfg)

	var completed, dropped atomic.Int64
	var held atomic.Int32
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), 0)
			if err != nil {
				dropped.Add(1)
				return
			}
			val := held.Add(1)
			assert.LessOrEqual(t, val, int32(1), "mutual exclusion violated")
			time.Sleep(time.Duration(1+rand.IntN(10)) * time.Millisecond)
			held.Add(-1)
			u.Release()
			completed.Add(1)
		}()
	}
	wg.Wait()
	// ~1.1s of serialized work spans the 1s trigger; the monitor arms a brief
	// episode near the boundary that may shed a small number before draining.
	assert.Equal(t, int64(200), completed.Load()+dropped.Load(), "every request accounted for")
	assert.True(t, s.isIdle())
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

			u, err := s.Acquire(ctx, 0)
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
	assert.True(t, s.isIdle())
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

	unlock, err := droppableSnake.Acquire(t.Context(), 0)
	require.NoError(t, err)

	var wg sync.WaitGroup
	var droppableSuccess, droppableFailed atomic.Int64

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := droppableSnake.Acquire(t.Context(), 0)
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
	assert.True(t, droppableSnake.isIdle())

	unlock2, err := undroppableSnake.Acquire(t.Context(), 0)
	require.NoError(t, err)

	var undroppableSuccess atomic.Int64
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := undroppableSnake.Acquire(t.Context(), 0)
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
	assert.True(t, undroppableSnake.isIdle())
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
				u, err := s.Acquire(t.Context(), 0)
				if err != nil {
					continue
				}
				u.Release()
			}
		}()
	}

	wg.Wait()
	assert.True(t, s.isIdle())
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

			u, err := s.Acquire(ctx, 0)
			if err == nil {
				time.Sleep(100 * time.Microsecond)
				u.Release()
			}
		}()
	}

	wg.Wait()
	assert.True(t, s.isIdle(), "lock should not be orphaned")
}

// --- Drop timer + cancel race ---

func TestSnake_Stress_DropTimerAndCancel_Race(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }     // 1us
	cfg.CoDel.TargetNs = func() int64 { return 1 }           // 1ns
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1_000 } // 1us
	s := NewSnake(cfg)

	unlock, err := s.Acquire(t.Context(), 0)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
			defer cancel()

			u, err := s.Acquire(ctx, 0)
			if err == nil {
				u.Release()
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	unlock.Release()
	wg.Wait()

	assert.True(t, s.isIdle())
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
			u, err := s.Acquire(ctx, 0)
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

			u, err := s.Acquire(ctx, 0)
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
