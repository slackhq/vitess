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
	"testing"
)

// --- Uncontended Latency ---
//
// Measures the fast-path cost of Acquire+Release and basic CoDel queue operations
// when no other goroutines are competing. This is the floor: every request pays at
// least this much. The CoDel queue itself is cheap; most of the Snake overhead comes
// from the mutex, channel signaling, and valve bookkeeping layered on top.

// BenchmarkSnake_Uncontended measures Acquire+Release with no valve ID and no
// contention — the absolute minimum cost Snake adds to every query.
func BenchmarkSnake_Uncontended(b *testing.B) {
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

// BenchmarkSnake_Uncontended_WithValveID measures the incremental cost of valve
// bookkeeping (map lookup + outstanding count) on the uncontended path.
func BenchmarkSnake_Uncontended_WithValveID(b *testing.B) {
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

// BenchmarkSnake_Contended_WithValveID measures Acquire+Release with valve ID
// under 8-way goroutine contention — the realistic production path where multiple
// queries with different valve IDs compete for the mutex simultaneously.
func BenchmarkSnake_Contended_WithValveID(b *testing.B) {
	s := NewSnake(defaultSnakeConfig())
	ctx := context.Background()
	const workers = 8

	ids := make([]string, workers)
	for i := range workers {
		ids[i] = fmt.Sprintf("valve-%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerWorker := b.N / workers
	for i := range workers {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for range opsPerWorker {
				u, err := s.Acquire(ctx, id)
				if err != nil {
					continue
				}
				u.Release()
			}
		}(ids[i])
	}
	wg.Wait()
}

// BenchmarkCoDelQueue_EnqueueComplete measures the raw CoDel queue cost without
// Snake's mutex, channel signaling, or valve bookkeeping — just enqueue + grant + complete.
func BenchmarkCoDelQueue_EnqueueComplete(b *testing.B) {
	clock := newTestClock()
	rec := &testDropTimerRecorder{}
	q := newCoDelQueue(defaultTestConfig(), clock.nowFunc, rec.schedule, rec.stop, nil)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		req := newRequest(0)
		q.lockedEnqueue(req)
		q.lockedOnGrant(req)
		q.lockedComplete(req)
	}
}

// BenchmarkCoDelQueue_Enqueue_Only measures enqueue in isolation — the allocation
// cost of creating a request and inserting into the linked list.
func BenchmarkCoDelQueue_Enqueue_Only(b *testing.B) {
	clock := newTestClock()
	rec := &testDropTimerRecorder{}
	q := newCoDelQueue(defaultTestConfig(), clock.nowFunc, rec.schedule, rec.stop, nil)

	b.ReportAllocs()
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

// --- Valve ID Scaling ---
//
// Asserts that the bookkeeping for the various valve data structures is all O(1).
// The operation is an acquire+release for a given valve ID.

// BenchmarkSnake_ValveIDScaling checks whether map cardinality affects performance.
// In production, many distinct valve IDs are in-flight simultaneously.
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

// --- GOMAXPROCS Scaling ---
//
// Measures how the single-mutex approach fares as the number of threads competing
// for the lock increases. Degradation is sublinear.

// BenchmarkSnake_GOMAXPROCS isolates the effect of real CPU-level parallelism on
// mutex contention — cache line bouncing, atomic CAS retries under increasing
// thread counts.
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

// --- findLowestPriorityDroppable ---
//
// Currently linear, which is expected based on our current implementation. Zero
// allocations. We're planning on modifying our data structures to make the various
// queue operations O(log(n)). Based on drop timer intervals for the parameters we
// expect, this isn't a mandatory optimization, but it's desirable nonetheless since
// the drop timer runs more frequently under overload.

// BenchmarkCoDelQueue_FindLowestPriority measures the O(n) scan at various queue
// depths.
func BenchmarkCoDelQueue_FindLowestPriority(b *testing.B) {
	for _, depth := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("Depth%d", depth), func(b *testing.B) {
			clock := newTestClock()
			rec := &testDropTimerRecorder{}
			q := newCoDelQueue(defaultTestConfig(), clock.nowFunc, rec.schedule, rec.stop, nil)

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

// --- Self-Contention Throughput ---
//
// Verifies that multiple goroutines contending on the same valve ID don't degrade
// throughput. The valve serializes them (only one in the CoDel queue at a time),
// so adding more parallel goroutines for the same ID shouldn't hurt.

// BenchmarkSnake_Throughput_SelfContention measures throughput when multiple
// goroutines share the same valve ID under varying parallelism.
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
