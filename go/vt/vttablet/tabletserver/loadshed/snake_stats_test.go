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
	"vitess.io/vitess/go/stats"
)

// fakeExporter captures the CounterFuncs and Histograms registered by
// PublishStats so the test can invoke them directly, without touching global
// stats registration.
type fakeExporter struct {
	counters      map[string]func() int64
	histograms    map[string]*stats.Histogram
	multiCounters map[string]*stats.CountersWithMultiLabels
}

func newFakeExporter() *fakeExporter {
	return &fakeExporter{
		counters:      make(map[string]func() int64),
		histograms:    make(map[string]*stats.Histogram),
		multiCounters: make(map[string]*stats.CountersWithMultiLabels),
	}
}

func (e *fakeExporter) NewCounterFunc(name, _ string, f func() int64) *stats.CounterFunc {
	e.counters[name] = f
	return nil
}

func (e *fakeExporter) NewHistogram(name, help string, cutoffs []int64) *stats.Histogram {
	h := stats.NewHistogram("", help, cutoffs)
	e.histograms[name] = h
	return h
}

func (e *fakeExporter) NewCountersWithMultiLabels(name, help string, labels []string) *stats.CountersWithMultiLabels {
	c := stats.NewCountersWithMultiLabels("", help, labels)
	e.multiCounters[name] = c
	return c
}
