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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// --- Uncontended acquire/release ---

func BenchmarkSnake_Uncontended(b *testing.B) {
	s := NewSnake(defaultSnakeConfig())
	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		u, err := s.Acquire(ctx, 0)
		if err != nil {
			b.Fatal(err)
		}
		u.Release()
	}
}

// --- Contended acquire/release (parallel goroutines) ---

func BenchmarkSnake_Contended(b *testing.B) {
	for _, parallelism := range []int{4, 16, 64, 256} {
		b.Run(fmt.Sprintf("P%d", parallelism), func(b *testing.B) {
			s := NewSnake(defaultSnakeConfig())
			ctx := context.Background()

			b.SetParallelism(parallelism / runtime.GOMAXPROCS(0))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u, err := s.Acquire(ctx, 0)
					if err != nil {
						continue
					}
					u.Release()
				}
			})
		})
	}
}

// --- GOMAXPROCS scaling ---

func BenchmarkSnake_GOMAXPROCS(b *testing.B) {
	for _, procs := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("PROCS%d", procs), func(b *testing.B) {
			prev := runtime.GOMAXPROCS(procs)
			defer runtime.GOMAXPROCS(prev)

			s := NewSnake(defaultSnakeConfig())
			ctx := context.Background()

			b.SetParallelism(procs)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u, err := s.Acquire(ctx, 0)
					if err != nil {
						continue
					}
					u.Release()
				}
			})
		})
	}
}

// --- CoDel queue enqueue/dequeue (low-level, no Snake overhead) ---

func BenchmarkCoDelQueue_EnqueueComplete(b *testing.B) {
	clock := newTestClock()
	rec := &testDropTimerRecorder{}
	q := newCoDelQueue(defaultTestConfig(), clock.nowFunc, rec.schedule, rec.stop, nil)

	b.ResetTimer()
	for range b.N {
		req := newRequest(0)
		q.lockedEnqueue(req)
		q.lockedOnGrant(req)
		q.lockedComplete(req)
	}
}

func BenchmarkCoDelQueue_Enqueue_Only(b *testing.B) {
	clock := newTestClock()
	rec := &testDropTimerRecorder{}
	q := newCoDelQueue(defaultTestConfig(), clock.nowFunc, rec.schedule, rec.stop, nil)

	b.ResetTimer()
	for range b.N {
		req := newRequest(0)
		q.lockedEnqueue(req)
	}
	b.StopTimer()
	for q.lockedLen() > 0 {
		testDequeue(q)
	}
}

// --- findLowestPriorityDroppable at various queue depths ---

func BenchmarkCoDelQueue_FindLowestPriority(b *testing.B) {
	for _, depth := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("Depth%d", depth), func(b *testing.B) {
			clock := newTestClock()
			rec := &testDropTimerRecorder{}
			q := newCoDelQueue(defaultTestConfig(), clock.nowFunc, rec.schedule, rec.stop, nil)

			// Use varied priorities so the scan must traverse the full queue.
			// The lowest priority is at position depth-1, forcing a full scan.
			for i := range depth {
				req := newRequest(float64(depth - i))
				q.lockedEnqueue(req)
			}

			b.ResetTimer()
			for range b.N {
				q.lockedFindLowestPriorityDroppable()
			}
		})
	}
}

// --- ValvedCoDelQueue enqueue with valve promotion ---

func BenchmarkValved_EnqueuePromote(b *testing.B) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	b.ResetTimer()
	for range b.N {
		sq.lockedEnqueue("bench-id", 0)
		testValvedDequeue(sq)
	}
}

func BenchmarkValved_ValvePromotion_Chain(b *testing.B) {
	for _, chainLen := range []int{2, 5, 10, 50} {
		b.Run(fmt.Sprintf("Chain%d", chainLen), func(b *testing.B) {
			clock := newTestClock()
			sq, _ := newValvedQueue(clock)

			b.ResetTimer()
			for range b.N {
				for range chainLen {
					sq.lockedEnqueue("bench-id", 0)
				}
				for range chainLen {
					testValvedDequeue(sq)
				}
			}
		})
	}
}

// --- Allocation benchmarks ---

func BenchmarkSnake_Allocs_Uncontended(b *testing.B) {
	s := NewSnake(defaultSnakeConfig())
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		u, err := s.Acquire(ctx, 0)
		if err != nil {
			b.Fatal(err)
		}
		u.Release()
	}
}

func BenchmarkSnake_Allocs_Contended(b *testing.B) {
	s := NewSnake(defaultSnakeConfig())
	ctx := context.Background()
	const workers = 8

	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerWorker := b.N / workers
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range opsPerWorker {
				u, err := s.Acquire(ctx, 0)
				if err != nil {
					continue
				}
				u.Release()
			}
		}()
	}
	wg.Wait()
}

// --- N-holder: multi-slot throughput ---

func BenchmarkSnake_NHolder_Throughput(b *testing.B) {
	for _, capacity := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("Cap%d", capacity), func(b *testing.B) {
			cfg := defaultSnakeConfig()
			cfg.Capacity = func() int { return capacity }
			s := NewSnake(cfg)
			ctx := context.Background()

			b.SetParallelism(capacity * 2 / runtime.GOMAXPROCS(0))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u, err := s.Acquire(ctx, 0)
					if err != nil {
						continue
					}
					u.Release()
				}
			})
		})
	}
}

// --- N-holder: uncontended multi-slot (fast path, no blocking) ---

func BenchmarkSnake_NHolder_Uncontended(b *testing.B) {
	for _, capacity := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("Cap%d", capacity), func(b *testing.B) {
			cfg := defaultSnakeConfig()
			cfg.Capacity = func() int { return capacity }
			s := NewSnake(cfg)
			ctx := context.Background()

			b.ResetTimer()
			for range b.N {
				u, err := s.Acquire(ctx, 0)
				if err != nil {
					b.Fatal(err)
				}
				u.Release()
			}
		})
	}
}

// --- N-holder: saturated (all slots filled, waiters queued) ---

func BenchmarkSnake_NHolder_Saturated(b *testing.B) {
	for _, capacity := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("Cap%d", capacity), func(b *testing.B) {
			cfg := defaultSnakeConfig()
			cfg.Capacity = func() int { return capacity }
			s := NewSnake(cfg)
			ctx := context.Background()

			b.SetParallelism(capacity * 4 / runtime.GOMAXPROCS(0))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u, err := s.Acquire(ctx, 0)
					if err != nil {
						continue
					}
					u.Release()
				}
			})
		})
	}
}

// --- N-holder: allocation profile ---

func BenchmarkSnake_NHolder_Allocs(b *testing.B) {
	for _, capacity := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("Cap%d", capacity), func(b *testing.B) {
			cfg := defaultSnakeConfig()
			cfg.Capacity = func() int { return capacity }
			s := NewSnake(cfg)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				u, err := s.Acquire(ctx, 0)
				if err != nil {
					b.Fatal(err)
				}
				u.Release()
			}
		})
	}
}

// --- High contention: single global mutex under up to ~500k contenders ---
//
// Snake protects its CoDel queue and valve maps with one sync.Mutex acquired
// on every Acquire and Release. The benchmarks above only reach 8-way
// contention via b.RunParallel, which says nothing about the
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
	remaining := int64(b.N)

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
					u, err := s.Acquire(ctx, 0)
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
					u, err := s.Acquire(ctx, 0)
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
