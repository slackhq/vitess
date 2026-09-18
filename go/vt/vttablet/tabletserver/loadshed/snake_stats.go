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

type statsExporter interface {
	NewCounterFunc(name, help string, f func() int64) *stats.CounterFunc
	NewHistogram(name, help string, cutoffs []int64) *stats.Histogram
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

var (
	intervalBucketCutoffs = loadshedBucketCutoffs
	lengthBucketCutoffs   = []int64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096}
)

func durationNanos(durations ...time.Duration) []int64 {
	values := make([]int64, len(durations))
	for i, duration := range durations {
		values[i] = duration.Nanoseconds()
	}
	return values
}

func PublishStats[T any](exporter statsExporter, prefix string, s *Snake[T]) {
	exporter.NewCounterFunc(prefix+"ShedCount", "Cumulative requests shed by the Snake load shedder", s.ShedCount)
	exporter.NewCounterFunc(prefix+"DroppingNanosTotal", "Cumulative nanoseconds Snake CoDel spent in the dropping state", s.DroppingNanos)
	exporter.NewCounterFunc(prefix+"InitialTargetShadow20xCensoredCount", "Cumulative fixed-20x initial-target shadow bursts censored because shadow mode ended before an outcome was known", s.shadowCensored.Load)
	s.sojourn = exporter.NewHistogram(prefix+"SojournNs", "Distribution of Snake queue wait before dequeue, in nanoseconds", loadshedBucketCutoffs)
	s.queueLen = exporter.NewHistogram(prefix+"QueueLenObserved", "Distribution of Snake queue length", lengthBucketCutoffs)
	s.droppableLen = exporter.NewHistogram(prefix+"DroppableLenObserved", "Distribution of Snake droppable queue length", lengthBucketCutoffs)
	s.interval = exporter.NewHistogram(prefix+"IntervalObservedNs", "Distribution of Snake CoDel control intervals", intervalBucketCutoffs)
	s.dropCount = exporter.NewHistogram(prefix+"DropCountObserved", "Distribution of Snake CoDel drop counts", lengthBucketCutoffs)
	s.timerLag = exporter.NewHistogram(prefix+"DropTimerLagNs", "Distribution of Snake CoDel timer lag", loadshedBucketCutoffs)
	s.shadowRequiredTarget = exporter.NewHistogram(prefix+"InitialTargetShadow20xMs", "Smallest candidate initial target that hit during a completed no-drop shadow burst using fixed target*20 intervals, in milliseconds; +Inf means every candidate missed", initialTargetShadowMetricCutoffsMs)
}
