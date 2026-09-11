/*
Copyright 2019 The Vitess Authors.

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

package stats

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Timings is meant to tracks timing data
// by named categories as well as histograms.
type Timings struct {
	totalCount atomic.Int64
	totalTime  atomic.Int64

	mu         sync.RWMutex
	histograms map[string]*Histogram

	name          string
	help          string
	label         string
	labelCombined bool
}

// NewTimings creates a new Timings object, and publishes it if name is set.
// categories is an optional list of categories to initialize to 0.
// Categories that aren't initialized will be missing from the map until the
// first time they are updated.
func NewTimings(name, help, label string, categories ...string) *Timings {
	t := &Timings{
		histograms:    make(map[string]*Histogram),
		name:          name,
		help:          help,
		label:         label,
		labelCombined: IsDimensionCombined(label),
	}
	for _, cat := range categories {
		t.histograms[cat] = NewGenericHistogram("", "", bucketCutoffs, bucketLabels, "Count", "Time")
	}
	if name != "" {
		publish(name, t)
	}

	return t
}

// Reset will clear histograms and counters: used during testing
func (t *Timings) Reset() {
	t.mu.RLock()
	t.histograms = make(map[string]*Histogram)
	t.totalCount.Store(0)
	t.totalTime.Store(0)
	t.mu.RUnlock()
}

// Add will add a new value to the named histogram.
func (t *Timings) Add(name string, elapsed time.Duration) {
	if t.labelCombined {
		name = StatsAllStr
	}
	// Get existing Histogram.
	t.mu.RLock()
	hist, ok := t.histograms[name]
	t.mu.RUnlock()

	// Create Histogram if it does not exist.
	if !ok {
		t.mu.Lock()
		hist, ok = t.histograms[name]
		if !ok {
			hist = NewGenericHistogram("", "", bucketCutoffs, bucketLabels, "Count", "Time")
			t.histograms[name] = hist
		}
		t.mu.Unlock()
	}
	if defaultStatsdHook.timerHook != nil && t.name != "" {
		defaultStatsdHook.timerHook(t.name, name, elapsed.Milliseconds(), t)
	}

	elapsedNs := int64(elapsed)
	hist.Add(elapsedNs)
	t.totalCount.Add(1)
	t.totalTime.Add(elapsedNs)
}

// Record is a convenience function that records completion
// timing data based on the provided start time of an event.
func (t *Timings) Record(name string, startTime time.Time) {
	if t.labelCombined {
		name = StatsAllStr
	}
	t.Add(name, time.Since(startTime))
}

// String is for expvar.
func (t *Timings) String() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	tm := struct {
		TotalCount int64
		TotalTime  int64
		Histograms map[string]*Histogram
	}{
		t.totalCount.Load(),
		t.totalTime.Load(),
		t.histograms,
	}

	data, err := json.Marshal(tm)
	if err != nil {
		data, _ = json.Marshal(err.Error())
	}
	return string(data)
}

// Histograms returns a map pointing at the histograms.
func (t *Timings) Histograms() (h map[string]*Histogram) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	h = make(map[string]*Histogram, len(t.histograms))
	for k, v := range t.histograms {
		h[k] = v
	}
	return
}

// Count returns the total count for all values.
func (t *Timings) Count() int64 {
	return t.totalCount.Load()
}

// Time returns the total time elapsed for all values.
func (t *Timings) Time() int64 {
	return t.totalTime.Load()
}

// Counts returns the total count for each value.
func (t *Timings) Counts() map[string]int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	counts := make(map[string]int64, len(t.histograms)+1)
	for k, v := range t.histograms {
		counts[k] = v.Count()
	}
	counts["All"] = t.totalCount.Load()
	return counts
}

// Cutoffs returns the cutoffs used in the component histograms.
// Do not change the returned slice.
func (t *Timings) Cutoffs() []int64 {
	return bucketCutoffs
}

// Help returns the help string.
func (t *Timings) Help() string {
	return t.help
}

// Label returns the label name.
func (t *Timings) Label() string {
	return t.label
}

// bucketCutoffs are the shared timing-histogram boundaries, in nanoseconds,
// used by every Timings/MultiTimings metric. histogram_quantile interpolates
// linearly within a bucket, so a bucket [a,b] bounds the estimate's relative
// error by the ratio b/a. The original set alternated x2 and x5 steps
// (0.5,1,5,10,50,100,500,...), so the x5 buckets (e.g. 100ms->500ms) gave up to
// 5x error exactly where tablet p99s sit — the estimate leapt as samples
// crossed a boundary.
//
// These are GEOMETRIC with a constant multiplier of ~10^(1/5) ~= 1.58 (Renard
// R5 preferred numbers: 1.0, 1.6, 2.5, 4.0, 6.3 per decade), giving uniform
// relative resolution — every bucket bounds the quantile to ~1.6x (~±23%) —
// across the whole 0.5ms..10s range rather than only at a hand-picked band.
// 22 cutoffs; a modest cardinality increase over the shared timing metrics.
var bucketCutoffs = []int64{
	500_000,        // 0.5ms (original floor)
	1_000_000,      // 1ms
	1_600_000,      // 1.6ms
	2_500_000,      // 2.5ms
	4_000_000,      // 4ms
	6_300_000,      // 6.3ms
	10_000_000,     // 10ms
	16_000_000,     // 16ms
	25_000_000,     // 25ms
	40_000_000,     // 40ms
	63_000_000,     // 63ms
	100_000_000,    // 100ms
	160_000_000,    // 160ms
	250_000_000,    // 250ms
	400_000_000,    // 400ms
	630_000_000,    // 630ms
	1_000_000_000,  // 1s
	1_600_000_000,  // 1.6s
	2_500_000_000,  // 2.5s
	4_000_000_000,  // 4s
	6_300_000_000,  // 6.3s
	10_000_000_000, // 10s
}

var bucketLabels []string

func init() {
	bucketLabels = make([]string, len(bucketCutoffs)+1)
	for i, v := range bucketCutoffs {
		bucketLabels[i] = fmt.Sprintf("%d", v)
	}
	bucketLabels[len(bucketLabels)-1] = "inf"
}

// MultiTimings is meant to tracks timing data by categories as well
// as histograms. The names of the categories are compound names made
// with joining multiple strings with '.'.
type MultiTimings struct {
	Timings
	labels         []string
	combinedLabels []bool
}

// NewMultiTimings creates a new MultiTimings object.
func NewMultiTimings(name string, help string, labels []string) *MultiTimings {
	combinedLabels := make([]bool, len(labels))
	for i, label := range labels {
		combinedLabels[i] = IsDimensionCombined(label)
	}
	t := &MultiTimings{
		Timings: Timings{
			histograms: make(map[string]*Histogram),
			name:       name,
			help:       help,
			label:      safeJoinLabels(labels, combinedLabels),
		},
		labels:         labels,
		combinedLabels: combinedLabels,
	}
	if name != "" {
		publish(name, t)
	}

	return t
}

// Labels returns descriptions of the parts of each compound category name.
func (mt *MultiTimings) Labels() []string {
	return mt.labels
}

// Add will add a new value to the named histogram.
func (mt *MultiTimings) Add(names []string, elapsed time.Duration) {
	if len(names) != len(mt.labels) {
		panic("MultiTimings: wrong number of values in Add")
	}
	mt.Timings.Add(safeJoinLabels(names, mt.combinedLabels), elapsed)
}

// Record is a convenience function that records completion
// timing data based on the provided start time of an event.
func (mt *MultiTimings) Record(names []string, startTime time.Time) {
	if len(names) != len(mt.labels) {
		panic("MultiTimings: wrong number of values in Record")
	}
	mt.Timings.Record(safeJoinLabels(names, mt.combinedLabels), startTime)
}

// Cutoffs returns the cutoffs used in the component histograms.
// Do not change the returned slice.
func (mt *MultiTimings) Cutoffs() []int64 {
	return bucketCutoffs
}
