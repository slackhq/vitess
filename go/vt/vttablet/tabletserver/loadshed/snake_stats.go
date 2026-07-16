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
	"time"

	"vitess.io/vitess/go/stats"
)

// statsExporter is the subset of servenv.Exporter that PublishStats needs.
// Declaring it here (rather than importing the concrete Exporter) keeps the
// loadshed package free of a servenv dependency and lets tests pass a
// throwaway exporter.
type statsExporter interface {
	NewCounterFunc(name, help string, f func() int64) *stats.CounterFunc
	NewHistogram(name, help string, cutoffs []int64) *stats.Histogram
	NewCountersWithMultiLabels(name, help string, labels []string) *stats.CountersWithMultiLabels
}

var loadshedBucketCutoffs = durationNanos(
	500*time.Nanosecond,
	time.Microsecond,
	10*time.Microsecond,
	50*time.Microsecond,
	200*time.Microsecond,
	time.Millisecond,
	5*time.Millisecond,
	20*time.Millisecond,
	100*time.Millisecond,
	500*time.Millisecond,
)

var intervalBucketCutoffs = loadshedBucketCutoffs

var lengthBucketCutoffs = []int64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}

// holderBucketCutoffs is a finer-grained set for slot-holder counts, which are
// bounded by the pool size (typically <= 64) and where the useful resolution is
// at the low end — the powers-of-two lengthBucketCutoffs collapse the entire
// operating range into ~6 buckets. Unit steps through the single digits, then
// tightening steps across the 32-64 range under test, with a little headroom
// above 64 to catch misconfiguration.
var holderBucketCutoffs = []int64{1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 24, 32, 40, 48, 56, 64, 96, 128}

func durationNanos(ds ...time.Duration) []int64 {
	out := make([]int64, len(ds))
	for i, d := range ds {
		out[i] = d.Nanoseconds()
	}
	return out
}

// PublishStats registers Snake's counters and distribution histograms, each
// name prefixed with prefix (e.g. "SnakeOltpRead" or "SnakeDml"). Call this once
// per Snake instance from engine init — never from NewSnake, which is also
// exercised by tests and the benchmark harness where duplicate registration
// would panic.
//
// Each Snake gets its own prefixed metric names rather than a shared "pool"
// label: both the oltp-read and dml snakes register through the same tablet
// Exporter, whose single label dimension is already the tablet name, so a
// shared labeled metric would collide on that one key.
func PublishStats(exporter statsExporter, prefix string, s *Snake) {
	exporter.NewCounterFunc(prefix+"ShedCount", "Cumulative requests shed by the Snake load shedder", func() int64 {
		return s.ShedCount()
	})
	s.shedByPriority = exporter.NewCountersWithMultiLabels(prefix+"ShedByPriority", "Cumulative requests shed by the Snake load shedder, labeled by the caller's original query priority (\"0\" most important .. \"100\" least, \"overflow\"); sum equals ShedCount", []string{"priority"})
	s.acquireByPriority = exporter.NewCountersWithMultiLabels(prefix+"AcquireByPriority", "Cumulative Acquire attempts (offered load) labeled by the caller's original query priority (\"0\" most important .. \"100\" least, \"overflow\"); ShedByPriority/AcquireByPriority is the per-priority shed rate", []string{"priority"})
	exporter.NewCounterFunc(prefix+"UnderfillCount", "Cumulative times a released Snake slot found no waiter and went idle (semaphore underfill)", func() int64 {
		return s.UnderfillCount()
	})
	exporter.NewCounterFunc(prefix+"SlotIdleNanosTotal", "Cumulative time-integral of unfilled Snake capacity ((capacity-holders)*elapsed) in nanoseconds; backend concurrency left idle by the gate", func() int64 {
		return s.SlotIdleNanos()
	})
	exporter.NewCounterFunc(prefix+"ShedBelowCapacityCount", "Cumulative requests shed by Snake CoDel while the semaphore had a free slot (holders < capacity); counterproductive drops the keep-droppable floor is meant to prevent", func() int64 {
		return s.ShedBelowCapacityCount()
	})
	exporter.NewCounterFunc(prefix+"DroppingNanosTotal", "Cumulative nanoseconds Snake CoDel spent in the dropping state; rate() yields the fraction of time shedding", func() int64 {
		return s.DroppingNanos()
	})
	s.sojourn = exporter.NewHistogram(prefix+"SojournNs", "Distribution of Snake sojourn (time-to-grant: queue wait before slot grant), in nanoseconds", loadshedBucketCutoffs)
	s.queueLen = exporter.NewHistogram(prefix+"QueueLenObserved", "Distribution of Snake CoDel queue length, sampled at each change", lengthBucketCutoffs)
	s.droppableLen = exporter.NewHistogram(prefix+"DroppableLenObserved", "Distribution of Snake CoDel droppable queue length, sampled at each change", lengthBucketCutoffs)
	s.holderCount = exporter.NewHistogram(prefix+"HolderCountObserved", "Distribution of Snake slot holders, sampled at each change", holderBucketCutoffs)
	s.interval = exporter.NewHistogram(prefix+"IntervalObservedNs", "Distribution of Snake CoDel control interval in nanoseconds, sampled at each timer fire", intervalBucketCutoffs)
	s.dropCount = exporter.NewHistogram(prefix+"DropCountObserved", "Distribution of Snake CoDel drop count (control-law state), sampled at each timer fire", lengthBucketCutoffs)
	s.timerLag = exporter.NewHistogram(prefix+"DropTimerLagNs", "Distribution of how late the Snake CoDel drop timer fired versus its scheduled time, in nanoseconds; high values mean shedding decisions are delayed under CPU contention", loadshedBucketCutoffs)
	s.dropOvershoot = exporter.NewHistogram(prefix+"DropOvershootNs", "Distribution of how late (now - dropNextNs) a due-drop pass was serviced, in nanoseconds, on either the backstop timer or the synchronous dequeue/release path; unlike DropTimerLagNs this covers the dequeue path, where a descheduled goroutine under CPU saturation delays shedding", loadshedBucketCutoffs)
	s.dropsPerFire = exporter.NewHistogram(prefix+"DropsPerFireObserved", "Distribution of requests shed by a single Snake CoDel control-law advance; ~1 on an on-time fire, but a late fire catch-up-loops through missed intervals and sheds a burst under one lock acquisition — the tail-latency signal MaxDropsPerFire bounds", lengthBucketCutoffs)
	s.valveDepth = exporter.NewHistogram(prefix+"ValveDepthObserved", "Distribution of Snake self-contention valve depth (requests stacked behind one valve's droppable representative), sampled at each valve-keyed enqueue", lengthBucketCutoffs)
}
