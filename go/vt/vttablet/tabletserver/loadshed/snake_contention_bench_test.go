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
)

// High-contention benchmarks for Snake's single global mutex.
//
// Snake protects its CoDel queue and valve maps with one sync.Mutex acquired
// on every Acquire and Release. Existing benchmarks (snake_bench_test.go) only
// reach 8-way contention via b.RunParallel, which says nothing about the
// hundreds-of-thousands-of-waiters regime that has caused convoy collapse in
// past incidents. These benchmarks drive Acquire+Release from an explicit,
// fixed worker pool sized to each contention level (8 .. ~500k) so we measure
// LOCK throughput rather than goroutine-creation overhead, then look at whether
// ns/op stays flat (graceful) or blows up super-linearly (convoy collapse).

// contentionLevels is the set of concurrent-contender counts swept by the
// high-contention benchmarks, from the 8-way ceiling of the existing suite up
// to ~500k.
var contentionLevels = []int{8, 64, 256, 1024, 8192, 65536, 262144, 524288}

// runPooledContention spins up exactly `level` long-lived worker goroutines,
// each repeatedly invoking acquireRelease until the pool has collectively
// performed b.N operations. Using a fixed pool (rather than b.RunParallel,
// which restarts its goroutine set each run and caps parallelism at
// SetParallelism*GOMAXPROCS) means all `level` goroutines are alive and
// contending on the single mutex simultaneously, and goroutine startup is
// excluded from the timed region.
func runPooledContention(b *testing.B, level int, acquireRelease func()) {
	// Each worker grabs work by atomically decrementing a shared counter, so
	// the b.N operations are shared across the pool regardless of per-op cost
	// skew. remaining starts at b.N.
	var remaining int64 = int64(b.N)

	var startWg sync.WaitGroup
	var doneWg sync.WaitGroup
	startWg.Add(1)
	doneWg.Add(level)

	for range level {
		go func() {
			defer doneWg.Done()
			// Wait for the starting gun so timing covers steady-state
			// contention, not staggered worker startup.
			startWg.Wait()
			for atomic.AddInt64(&remaining, -1) >= 0 {
				acquireRelease()
			}
		}()
	}

	b.ResetTimer()
	startWg.Done()
	doneWg.Wait()
	b.StopTimer()
}

// BenchmarkSnake_HighContention sweeps contention from 8 to ~500k contenders
// across three paths that all funnel through the same global mutex:
//
//   - FastPath_NoValve: capacity is effectively unbounded, so every Acquire is
//     granted immediately on the fast path (lock -> enqueue -> grant -> unlock)
//     and Release takes the lock again. This isolates raw mutex throughput with
//     no blocking hand-off and no CoDel shedding.
//   - FastPath_WithValve: same, but a shared valve ID exercises the valve map
//     bookkeeping under the lock.
//   - Capacity1_Serialized: the default capacity-1 gate. Only one holder at a
//     time; every other contender blocks on its signal channel until a Release
//     hands the slot off (or CoDel drops it). This is the realistic Snake
//     configuration and the one most prone to convoy behavior.
func BenchmarkSnake_HighContention(b *testing.B) {
	ctx := context.Background()

	// hugeCapacity keeps Snake on the immediate-grant fast path so we measure
	// the mutex itself, not the blocking grant/Release handoff.
	hugeCapacity := func() int { return 1 << 30 }

	b.Run("FastPath_NoValve", func(b *testing.B) {
		for _, level := range contentionLevels {
			b.Run(fmt.Sprintf("C%d", level), func(b *testing.B) {
				cfg := defaultSnakeConfig()
				cfg.Capacity = hugeCapacity
				s := NewSnake(cfg)
				runPooledContention(b, level, func() {
					u, err := s.Acquire(ctx, "", 0)
					if err != nil {
						return
					}
					u.Release()
				})
			})
		}
	})

	b.Run("FastPath_WithValve", func(b *testing.B) {
		for _, level := range contentionLevels {
			b.Run(fmt.Sprintf("C%d", level), func(b *testing.B) {
				cfg := defaultSnakeConfig()
				cfg.Capacity = hugeCapacity
				s := NewSnake(cfg)
				runPooledContention(b, level, func() {
					u, err := s.Acquire(ctx, "shared-valve", 0)
					if err != nil {
						return
					}
					u.Release()
				})
			})
		}
	})

	b.Run("Capacity1_Serialized", func(b *testing.B) {
		for _, level := range contentionLevels {
			b.Run(fmt.Sprintf("C%d", level), func(b *testing.B) {
				// Default config: Capacity == nil => capacity 1. Contenders
				// beyond the single holder block on signalChan or are shed by
				// CoDel; either way the mutex is the serialization point.
				s := NewSnake(defaultSnakeConfig())
				runPooledContention(b, level, func() {
					u, err := s.Acquire(ctx, "", 0)
					if err != nil {
						// Dropped by CoDel under load; count the attempt and
						// move on, matching the existing contended benchmark.
						return
					}
					u.Release()
				})
			})
		}
	})
}
