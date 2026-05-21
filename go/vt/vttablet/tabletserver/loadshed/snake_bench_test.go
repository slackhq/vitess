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
		u, err := s.Acquire(ctx, "")
		if err != nil {
			b.Fatal(err)
		}
		u.Release()
	}
}

func BenchmarkSnake_Uncontended_WithValveID(b *testing.B) {
	s := NewSnake(defaultSnakeConfig())
	ctx := context.Background()

	b.ResetTimer()
	for range b.N {
		u, err := s.Acquire(ctx, "valve-1")
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
					u, err := s.Acquire(ctx, "")
					if err != nil {
						continue
					}
					u.Release()
				}
			})
		})
	}
}

// --- Valve overhead: measure cost of valve bookkeeping vs. direct entry ---

func BenchmarkSnake_ValveOverhead(b *testing.B) {
	b.Run("NoValve", func(b *testing.B) {
		s := NewSnake(defaultSnakeConfig())
		ctx := context.Background()

		b.ResetTimer()
		for range b.N {
			u, err := s.Acquire(ctx, "")
			if err != nil {
				b.Fatal(err)
			}
			u.Release()
		}
	})

	b.Run("WithValve", func(b *testing.B) {
		s := NewSnake(defaultSnakeConfig())
		ctx := context.Background()

		b.ResetTimer()
		for range b.N {
			u, err := s.Acquire(ctx, "single-id")
			if err != nil {
				b.Fatal(err)
			}
			u.Release()
		}
	})
}

// --- Valve ID scaling: many distinct valve IDs ---

func BenchmarkSnake_ValveIDScaling(b *testing.B) {
	for _, numIDs := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("IDs%d", numIDs), func(b *testing.B) {
			s := NewSnake(defaultSnakeConfig())
			ctx := context.Background()
			ids := make([]string, numIDs)
			for i := range numIDs {
				ids[i] = fmt.Sprintf("valve-%d", i)
			}

			b.ResetTimer()
			for i := range b.N {
				u, err := s.Acquire(ctx, ids[i%numIDs])
				if err != nil {
					b.Fatal(err)
				}
				u.Release()
			}
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
					u, err := s.Acquire(ctx, "")
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

// --- SelfContentionAwareCoDelQueue enqueue with valve promotion ---

func BenchmarkSelfAware_EnqueuePromote(b *testing.B) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	b.ResetTimer()
	for range b.N {
		sq.lockedEnqueue("bench-id", 0)
		testSelfAwareDequeue(sq)
	}
}

func BenchmarkSelfAware_ValvePromotion_Chain(b *testing.B) {
	for _, chainLen := range []int{2, 5, 10, 50} {
		b.Run(fmt.Sprintf("Chain%d", chainLen), func(b *testing.B) {
			clock := newTestClock()
			sq, _ := newTestSelfAware(clock)

			b.ResetTimer()
			for range b.N {
				for range chainLen {
					sq.lockedEnqueue("bench-id", 0)
				}
				for range chainLen {
					testSelfAwareDequeue(sq)
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
		u, err := s.Acquire(ctx, "")
		if err != nil {
			b.Fatal(err)
		}
		u.Release()
	}
}

func BenchmarkSnake_Allocs_WithValve(b *testing.B) {
	s := NewSnake(defaultSnakeConfig())
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		u, err := s.Acquire(ctx, "valve-1")
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
				u, err := s.Acquire(ctx, "")
				if err != nil {
					continue
				}
				u.Release()
			}
		}()
	}
	wg.Wait()
}

// --- Throughput under contention (ops/sec) ---

func BenchmarkSnake_Throughput_SelfContention(b *testing.B) {
	for _, parallelism := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("SameID_P%d", parallelism), func(b *testing.B) {
			s := NewSnake(defaultSnakeConfig())
			ctx := context.Background()

			b.SetParallelism(parallelism / runtime.GOMAXPROCS(0))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					u, err := s.Acquire(ctx, "shared-id")
					if err != nil {
						continue
					}
					u.Release()
				}
			})
		})
	}
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
					u, err := s.Acquire(ctx, "")
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
				u, err := s.Acquire(ctx, "")
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
					u, err := s.Acquire(ctx, "")
					if err != nil {
						continue
					}
					u.Release()
				}
			})
		})
	}
}

// --- N-holder: valve + multi-slot (self-contention across multiple slots) ---

func BenchmarkSnake_NHolder_WithValve(b *testing.B) {
	for _, capacity := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("Cap%d", capacity), func(b *testing.B) {
			cfg := defaultSnakeConfig()
			cfg.Capacity = func() int { return capacity }
			s := NewSnake(cfg)
			ctx := context.Background()

			ids := make([]string, 4)
			for i := range ids {
				ids[i] = fmt.Sprintf("valve-%d", i)
			}

			b.SetParallelism(capacity * 2 / runtime.GOMAXPROCS(0))
			b.ResetTimer()
			var counter atomic.Int64
			b.RunParallel(func(pb *testing.PB) {
				idx := int(counter.Add(1) - 1)
				id := ids[idx%len(ids)]
				for pb.Next() {
					u, err := s.Acquire(ctx, id)
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
				u, err := s.Acquire(ctx, "")
				if err != nil {
					b.Fatal(err)
				}
				u.Release()
			}
		})
	}
}
