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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
)

// fakeExporter captures the CounterFuncs and Histograms registered by
// PublishStats so the test can invoke them directly, without touching global
// stats registration.
type fakeExporter struct {
	counters   map[string]func() int64
	histograms map[string]*stats.Histogram
}

func newFakeExporter() *fakeExporter {
	return &fakeExporter{
		counters:   make(map[string]func() int64),
		histograms: make(map[string]*stats.Histogram),
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

func newStatsTestSnake() (*Snake[string], *testClock, *fakeExporter) {
	clock := newTestClock()
	clock.now = 1
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeEnabled }
	cfg.CoDel.IntervalNs = func() int64 { return 10 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	snake := NewSnake[string](cfg)
	snake.clockFunc = clock.nowFunc
	snake.q.nowNs = clock.nowFunc
	exporter := newFakeExporter()
	PublishStats(exporter, "SnakeTest", snake)
	return snake, clock, exporter
}

func TestPublishStatsRegistersIsolatedMetrics(t *testing.T) {
	exporter := newFakeExporter()
	for _, prefix := range []string{"SnakeOltpRead", "SnakeDml"} {
		PublishStats(exporter, prefix, NewSnake[string](defaultSnakeConfig()))
		for _, suffix := range []string{"ShedCount", "DroppingNanosTotal", "InitialTargetShadow20xCensoredCount"} {
			assert.Contains(t, exporter.counters, prefix+suffix)
		}
		for _, suffix := range []string{
			"SojournNs",
			"QueueLenObserved",
			"DroppableLenObserved",
			"IntervalObservedNs",
			"DropCountObserved",
			"DropTimerLagNs",
			"InitialTargetShadow20xMs",
		} {
			assert.Contains(t, exporter.histograms, prefix+suffix)
		}
	}
	assert.Len(t, exporter.counters, 6)
	assert.Len(t, exporter.histograms, 14)
}

func TestPublishStatsShedCountTracksDropsNotCancellation(t *testing.T) {
	snake, clock, exporter := newStatsTestSnake()
	shedCount := exporter.counters["SnakeTestShedCount"]

	cancelled, _ := snake.Enqueue("cancelled")
	removed, dropped := snake.Cancel(cancelled)
	require.True(t, removed)
	assert.Empty(t, dropped)
	assert.Zero(t, shedCount())

	for range keepDroppableFloor + 2 {
		snake.Enqueue("")
	}
	clock.advance(20)
	dropped = snake.LockedDropTimerFired()

	require.NotEmpty(t, dropped)
	assert.Equal(t, int64(len(dropped)), shedCount())
}

func TestPublishStatsRecordsQueueObservations(t *testing.T) {
	snake, clock, exporter := newStatsTestSnake()

	snake.EnqueueExisting("existing")
	snake.Enqueue("droppable")
	clock.advance(25)
	value, ok, _ := snake.Dequeue()

	require.True(t, ok)
	assert.Equal(t, "existing", value)
	assert.Equal(t, int64(25), exporter.histograms["SnakeTestSojournNs"].Total())
	assert.Positive(t, exporter.histograms["SnakeTestQueueLenObserved"].Count())
	assert.Positive(t, exporter.histograms["SnakeTestDroppableLenObserved"].Count())
	assert.Positive(t, exporter.histograms["SnakeTestIntervalObservedNs"].Count())
	assert.Positive(t, exporter.histograms["SnakeTestDropCountObserved"].Count())
}

func TestNewSnakeInitializesDistributionMetrics(t *testing.T) {
	snake := NewSnake[string](defaultSnakeConfig())
	assert.NotContains(t, []*stats.Histogram{
		snake.sojourn,
		snake.queueLen,
		snake.droppableLen,
		snake.interval,
		snake.dropCount,
		snake.timerLag,
		snake.initialTargetShadowRequired,
	}, nil)
}

func TestPublishStatsRecordsDropTimerLag(t *testing.T) {
	snake, clock, exporter := newStatsTestSnake()

	snake.lockedScheduleDropTimer(int64(10 * time.Millisecond))
	clock.advance(int64(14 * time.Millisecond))
	snake.LockedDropTimerFired()

	lag := exporter.histograms["SnakeTestDropTimerLagNs"]
	assert.Equal(t, int64(1), lag.Count())
	assert.Equal(t, int64(4*time.Millisecond), lag.Total())
}

func TestDroppingNanosIntegratesExactly(t *testing.T) {
	snake, clock, _ := newStatsTestSnake()

	clock.now = 100
	snake.q.dropping = true
	snake.lockedObserveDropping()
	clock.now = 250
	assert.Equal(t, int64(150), snake.DroppingNanos())

	clock.now = 400
	snake.q.dropping = false
	snake.lockedObserveDropping()
	assert.Equal(t, int64(300), snake.DroppingNanos())

	clock.now = 1000
	snake.q.dropping = true
	snake.lockedObserveDropping()
	clock.now = 1100
	snake.q.dropping = false
	snake.lockedObserveDropping()
	assert.Equal(t, int64(400), snake.DroppingNanos())
}
