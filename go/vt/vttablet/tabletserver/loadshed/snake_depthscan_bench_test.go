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
	"fmt"
	"testing"
)

// depthScanDepths is the queue-depth axis swept by the benchmarks below. The
// existing contention benchmark sweeps contender count at shallow depth, so it
// never exercises lockedFindLowestPriorityDroppable (codelq.go:283) over a deep
// queue. These depths push well past the {10, 100, 1000} of
// BenchmarkCoDelQueue_FindLowestPriority so the O(n) shape is unambiguous.
var depthScanDepths = []int{16, 64, 256, 1024, 4096, 16384, 65536}

// BenchmarkSnake_DropScanDepth characterizes how the cost of the O(n)
// lowest-priority drop scan grows with queue DEPTH.
//
// Design 1 (undroppable holders create depth). lockedFindLowestPriorityDroppable
// walks the entire container/list (codelq.go:286 `for e := q.queue.Front()`).
// Undroppable entries are skipped via `continue` (codelq.go:288) but are still
// traversed by e.Next(), so they inflate the n the scan walks. This mirrors the
// realistic "grant stall" regime exercised by
// TestSnake_Overload_GrantStall_ShedsDuringStall: many granted (undroppable)
// holders pin the queue while a droppable waiter must be located and shed.
//
// We measure two levels:
//
//   - ScanOnly: calls lockedFindLowestPriorityDroppable directly on a queue of
//     D undroppable holders followed by a single droppable tail entry. Because
//     the only droppable entry sits at the end and is not priority 0, the scan
//     cannot early-exit (codelq.go:291) and must traverse all D+1 elements.
//     This isolates the scan cost from timer scheduling and drop bookkeeping.
//
//   - DropPath: drives the full drop path (lockedRunScheduledDrop -> dropFn ->
//     lockedFindLowestPriorityDroppable -> lockedDrop) once per op against a
//     queue pre-populated with D undroppable holders and a fresh droppable tail.
//     This includes the realistic per-drop work around the scan.
//
// We call the in-package scan/drop entry points directly rather than driving
// the real Acquire path because the production drop is timer-driven
// (time.AfterFunc in snake.go:277); forcing exactly one scan per timed
// iteration synchronously through Acquire at depths up to 65536 is infeasible.
// We are in package loadshed, so calling the locked* methods directly is the
// honest way to put D entries in the list and trigger the scan deterministically.
func BenchmarkSnake_DropScanDepth(b *testing.B) {
	b.Run("ScanOnly", func(b *testing.B) {
		for _, depth := range depthScanDepths {
			b.Run(fmt.Sprintf("Depth%d", depth), func(b *testing.B) {
				clock := newTestClock()
				q, _ := newTestQueue(defaultTestConfig(), clock)

				// D undroppable holders create depth without being eligible to
				// drop. The scan still traverses every one of them.
				for range depth {
					q.lockedEnqueue(newRequest(priorityUndroppable))
				}
				// A single droppable entry at the tail with a non-zero priority,
				// so the scan must walk the whole list to find it (no early exit).
				q.lockedEnqueue(newRequest(1))

				b.ResetTimer()
				for range b.N {
					if q.lockedFindLowestPriorityDroppable() == nil {
						b.Fatal("expected a droppable entry")
					}
				}
			})
		}
	})

	b.Run("DropPath", func(b *testing.B) {
		for _, depth := range depthScanDepths {
			b.Run(fmt.Sprintf("Depth%d", depth), func(b *testing.B) {
				clock := newTestClock()
				cfg := CoDelConfig{
					IntervalNs:     func() int64 { return 1_000_000 }, // 1ms
					TargetNs:       func() int64 { return 1 },         // 1ns
					MinDropDelayNs: func() int64 { return 100 },
					Exponent:       func() float64 { return 1.0 },
				}
				q, _ := newTestQueue(cfg, clock)

				// D undroppable holders that create depth and never leave.
				for range depth {
					q.lockedEnqueue(newRequest(priorityUndroppable))
				}

				// Force the dropping state so lockedRunScheduledDrop actually
				// scans+drops rather than no-opping. dropNextNs in the past plus
				// now advanced past it makes the drop loop fire.
				q.dropping = true
				q.count = 1

				dropFn := func() bool {
					elem := q.lockedFindLowestPriorityDroppable()
					if elem == nil {
						return false
					}
					q.lockedPopElem(elem, &DroppedRequestError{})
					return true
				}

				b.ResetTimer()
				for range b.N {
					// Each op: add one droppable tail entry (non-zero priority so
					// the scan can't early-exit), then run one scan+drop over the
					// D-deep list. StopTimer fences the setup so only the
					// scan+drop is timed.
					b.StopTimer()
					q.lockedEnqueue(newRequest(1))
					q.dropping = true
					q.count = 1
					clock.now = 0
					q.dropNextNs = -1 // in the past => drop loop fires immediately
					b.StartTimer()

					q.lockedRunScheduledDrop(dropFn)
				}
			})
		}
	})
}
