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
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSnakeMutexContentionLatency is not a correctness test — it is a
// measurement harness (run with -run) that quantifies how s.mu contention
// affects per-Acquire latency percentiles under a fixed offered load. It is the
// experiment that decides whether lock contention is actually the p99 driver
// before investing in a lock-free restructuring.
//
// It runs `workers` goroutines that each repeatedly Acquire (fast-path grant:
// huge capacity so we isolate the mutex, not the blocking/shed handoff), hold
// briefly, and Release, recording every Acquire's wall-clock latency. Reports
// p50/p90/p99/p999/max across a sweep of worker counts.
func TestSnakeMutexContentionLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("contention measurement; skipped in -short")
	}

	const (
		opsPerWorker = 2000
		holdNs       = 0 // pure mutex contention; set >0 to add hold time
	)
	workerCounts := []int{1, 2, 4, 8, 16, 32, 64, 128}

	for _, workers := range workerCounts {
		cfg := defaultSnakeConfig()
		// Huge capacity keeps every Acquire on the immediate-grant fast path so
		// we measure the s.mu critical section itself, not the CoDel
		// blocking/shed handoff.
		cfg.Capacity = func() int { return 1 << 30 }
		s := NewSnake[struct{}](cfg)

		total := workers * opsPerWorker
		lat := make([]int64, total)
		var idx atomic.Int64

		var start sync.WaitGroup
		var done sync.WaitGroup
		start.Add(1)
		done.Add(workers)

		for range workers {
			go func() {
				defer done.Done()
				start.Wait()
				ctx := context.Background()
				for range opsPerWorker {
					t0 := time.Now()
					u, err := s.Acquire(ctx, "", 0)
					d := time.Since(t0).Nanoseconds()
					if err == nil {
						if holdNs > 0 {
							busySpinNs(holdNs)
						}
						u.Release()
					}
					lat[idx.Add(1)-1] = d
				}
			}()
		}

		wall0 := time.Now()
		start.Done()
		done.Wait()
		wall := time.Since(wall0)

		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		pct := func(q float64) float64 {
			if len(lat) == 0 {
				return 0
			}
			return float64(lat[min(len(lat)-1, int(float64(len(lat))*q))]) / 1000.0 // µs
		}
		thru := float64(total) / wall.Seconds()
		t.Logf("workers=%-4d  ops=%-7d  p50=%6.2fµs p90=%6.2fµs p99=%7.2fµs p999=%8.2fµs max=%9.2fµs  thru=%.0f/s",
			workers, total, pct(.50), pct(.90), pct(.99), pct(.999), pct(1.0)-0.001, thru)
	}
}

func busySpinNs(ns int64) {
	deadline := time.Now().Add(time.Duration(ns))
	for time.Now().Before(deadline) {
	}
}
