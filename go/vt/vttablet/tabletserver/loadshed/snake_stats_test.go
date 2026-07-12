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
	"sync/atomic"
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

func TestPublishStats_RegistersCountersAndHistograms(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())
	exp := newFakeExporter()

	PublishStats(exp, "SnakeOltpRead", s)

	for _, name := range []string{"ShedCount", "DroppingNanosTotal"} {
		assert.Contains(t, exp.counters, "SnakeOltpRead"+name)
	}
	for _, name := range []string{"SojournNs", "QueueLenObserved", "DroppableLenObserved", "HolderCountObserved", "IntervalObservedNs", "DropCountObserved", "DropTimerLagNs", "ValveDepthObserved"} {
		assert.Contains(t, exp.histograms, "SnakeOltpRead"+name)
	}
}

func TestPublishStats_PrefixIsolation(t *testing.T) {
	// Two snakes registered under different prefixes through the same exporter
	// must not collide — this is the two-pool case (oltp-read vs dml).
	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", newTestSnake(defaultSnakeConfig()))
	PublishStats(exp, "SnakeDml", newTestSnake(defaultSnakeConfig()))

	assert.Contains(t, exp.counters, "SnakeOltpReadShedCount")
	assert.Contains(t, exp.counters, "SnakeDmlShedCount")
	assert.Contains(t, exp.counters, "SnakeOltpReadDroppingNanosTotal")
	assert.Contains(t, exp.counters, "SnakeDmlDroppingNanosTotal")
	assert.Len(t, exp.counters, 4, "expected shed + dropping counters per snake, two snakes")

	assert.Contains(t, exp.histograms, "SnakeOltpReadQueueLenObserved")
	assert.Contains(t, exp.histograms, "SnakeDmlQueueLenObserved")
	// sojourn + queueLen + droppableLen + holderCount + interval + dropCount +
	// timerLag + valveDepth, per snake.
	assert.Len(t, exp.histograms, 16, "expected 8 histograms per snake, two snakes")
}

func TestPublishStats_ShedCountTracksDrops(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	s := newTestSnake(cfg)

	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	shedGauge := exp.counters["SnakeOltpReadShedCount"]
	require.NotNil(t, shedGauge)

	assert.Equal(t, int64(0), shedGauge(), "no sheds before any contention")

	// Hold the single slot, then pile on contending acquires so CoDel drops some.
	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	errCh := make(chan error, 5)
	for range 5 {
		go func() {
			_, err := s.Acquire(t.Context(), "", 0)
			errCh <- err
		}()
	}
	time.Sleep(200 * time.Millisecond)
	unlock.Release()

	dropped := 0
	for range 5 {
		select {
		case err := <-errCh:
			if err != nil {
				dropped++
			}
		case <-time.After(2 * time.Second):
			t.Fatal("goroutine did not return")
		}
	}
	require.Greater(t, dropped, 0, "CoDel should have dropped some requests")
	assert.Equal(t, int64(dropped), shedGauge(), "shed counter should equal the number of dropped requests")
}

func TestPublishStats_ShedCountIgnoresContextCancellation(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig()) // capacity 1

	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	shedGauge := exp.counters["SnakeOltpReadShedCount"]

	// Occupy the only slot so the next acquire must wait.
	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	defer unlock.Release()

	// A second acquire whose context is cancelled returns ctx.Err(), which is
	// the caller giving up — not a gate shed — so it must not be counted.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = s.Acquire(ctx, "", 0)
	require.Error(t, err)

	assert.Equal(t, int64(0), shedGauge(), "context cancellation must not count as a shed")
}

func TestPublishStats_SojournRecordsGrant(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())
	exp := newFakeExporter()

	PublishStats(exp, "SnakeOltpRead", s)

	hist := exp.histograms["SnakeOltpReadSojournNs"]
	require.NotNil(t, hist, "expected histogram SnakeOltpReadSojournNs to be registered")
	assert.Equal(t, int64(0), hist.Count(), "no grants before any acquire")

	// A granted request records exactly one sojourn observation.
	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), hist.Count(), "one grant should record one sojourn observation")
	assert.GreaterOrEqual(t, hist.Total(), int64(0), "sojourn total should be a non-negative duration")

	require.NoError(t, unlock.Release())
}

func TestPublishStats_SojournBucketing(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())
	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	hist := exp.histograms["SnakeOltpReadSojournNs"]
	require.NotNil(t, hist)

	// A fast, uncontended grant lands in one of the low buckets: well under the
	// 1ms cutoff. Assert nothing landed at or above the 5ms (5e6) cutoff.
	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	require.NoError(t, unlock.Release())

	counts := hist.Counts()
	slow := counts["5000000"] + counts["20000000"] + counts["100000000"] + counts["500000000"] + counts["inf"]
	assert.Equal(t, int64(0), slow, "an uncontended grant must not land in the >=5ms buckets")
	assert.Equal(t, int64(1), hist.Count(), "exactly one observation recorded")
}

func TestNewSnake_DistributionMetricsInitializedBeforePublish(t *testing.T) {
	// NewSnake initializes every distribution histogram to a detached (unnamed,
	// unregistered) instance, so the observation paths never nil-check. A Snake
	// built without PublishStats records into these throwaway histograms and
	// registers nothing globally.
	s := newTestSnake(defaultSnakeConfig())
	require.NotNil(t, s.sojourn)
	require.NotNil(t, s.queueLen)
	require.NotNil(t, s.droppableLen)
	require.NotNil(t, s.holderCount)
	require.NotNil(t, s.interval)
	require.NotNil(t, s.dropCount)

	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	// The detached histograms accumulate even before PublishStats swaps in the
	// exporter-registered instances.
	assert.Positive(t, s.queueLen.Count())
	assert.Positive(t, s.holderCount.Count())
	assert.Positive(t, s.sojourn.Count())
	require.NoError(t, unlock.Release())
}

func TestPublishStats_QueueAndHolderHistogramsRecord(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig()) // capacity 1
	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)

	queueLen := exp.histograms["SnakeOltpReadQueueLenObserved"]
	holderCount := exp.histograms["SnakeOltpReadHolderCountObserved"]
	require.NotNil(t, queueLen)
	require.NotNil(t, holderCount)
	assert.Equal(t, int64(0), queueLen.Count(), "no observations before any acquire")
	assert.Equal(t, int64(0), holderCount.Count())

	// A grant observes both: the enqueue records queue length, the holder insert
	// records holder count.
	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	assert.Positive(t, queueLen.Count(), "enqueue should record a queue-length observation")
	assert.Positive(t, holderCount.Count(), "grant should record a holder-count observation")

	// Release records again (queue length drops as the granted entry leaves; the
	// holder delete is observed too).
	beforeQueue := queueLen.Count()
	beforeHolder := holderCount.Count()
	require.NoError(t, unlock.Release())
	assert.Greater(t, queueLen.Count(), beforeQueue, "release should record another queue-length observation")
	assert.Greater(t, holderCount.Count(), beforeHolder, "release should record another holder-count observation")
}

func TestPublishStats_ValveDepthHistogramRecords(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig()) // capacity 1
	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)

	valveDepth := exp.histograms["SnakeOltpReadValveDepthObserved"]
	require.NotNil(t, valveDepth)
	assert.Equal(t, int64(0), valveDepth.Count(), "no observations before any acquire")

	// Occupy the single slot with an empty-valve acquire. This also covers the
	// exclusion rule: an empty valve ID bypasses the valve and is not observed.
	holder, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), valveDepth.Count(), "empty valve ID should not record a valve-depth observation")

	// With the slot occupied, a valve-keyed acquire cannot be granted, so it
	// waits in the CoDel queue as valve "v"'s droppable representative. Its
	// enqueue records depth 0 (nothing stacked behind it yet). The observation
	// is synchronous inside Acquire before it blocks, so wait for the count.
	errCh := make(chan error, 2)
	go func() {
		_, err := s.Acquire(t.Context(), "v", 0)
		errCh <- err
	}()
	assert.Eventually(t, func() bool { return valveDepth.Count() == 1 }, 2*time.Second, time.Millisecond,
		"valve-keyed representative enqueue should record a depth observation")
	assert.Equal(t, int64(0), valveDepth.Total(), "first valve entry becomes the representative, depth 0")

	// A second acquire on the same valve ID stacks behind the representative
	// and records depth 1.
	go func() {
		_, err := s.Acquire(t.Context(), "v", 0)
		errCh <- err
	}()
	assert.Eventually(t, func() bool { return valveDepth.Count() == 2 }, 2*time.Second, time.Millisecond,
		"stacked valve entry should record a second depth observation")
	assert.Equal(t, int64(1), valveDepth.Total(), "second same-valve entry stacks at depth 1")

	// Release the holder so the queued "v" entries drain and the goroutines
	// return. Whether each is granted or shed by CoDel is irrelevant here — this
	// is teardown; the depth observations above are the assertions under test.
	require.NoError(t, holder.Release())
	for range 2 {
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("queued goroutine did not return")
		}
	}
}

func TestPublishStats_DroppableAndIntervalHistogramsRecordUnderLoad(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	s := newTestSnake(cfg)

	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	droppable := exp.histograms["SnakeOltpReadDroppableLenObserved"]
	interval := exp.histograms["SnakeOltpReadIntervalObservedNs"]
	dropCount := exp.histograms["SnakeOltpReadDropCountObserved"]
	require.NotNil(t, droppable)
	require.NotNil(t, interval)
	require.NotNil(t, dropCount)

	// Occupy the slot, then contend so droppable entries queue and the drop timer
	// fires (recording interval and drop-count observations).
	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	errCh := make(chan error, 5)
	for range 5 {
		go func() {
			_, err := s.Acquire(t.Context(), "", 0)
			errCh <- err
		}()
	}
	time.Sleep(200 * time.Millisecond)
	unlock.Release()
	for range 5 {
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("goroutine did not return")
		}
	}

	assert.Positive(t, droppable.Count(), "contending droppable enqueues should record droppable-length observations")
	assert.Positive(t, interval.Count(), "drop-timer fires should record interval observations")
	assert.Positive(t, dropCount.Count(), "drop-timer fires should record drop-count observations")
}

func TestPublishStats_DropTimerLagRecordsLateness(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())
	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	lag := exp.histograms["SnakeOltpReadDropTimerLagNs"]
	require.NotNil(t, lag)

	// Drive the timer with a controllable clock so the recorded lag is exact.
	var now atomic.Int64
	s.clockFunc = now.Load

	// Arm for a 10ms delay at t=0 (above the backstop floor so the requested
	// delay is used verbatim), then fire at t=14ms: 4ms late.
	s.mu.Lock()
	s.lockedScheduleDropTimer(int64(10 * time.Millisecond))
	s.mu.Unlock()
	now.Store(int64(14 * time.Millisecond))
	s.runDropTimer()

	require.Equal(t, int64(1), lag.Count(), "one timer fire should record one lag sample")
	assert.Equal(t, int64(4*time.Millisecond), lag.Total(), "recorded lag should be actual minus scheduled fire time")
}

func TestPublishStats_DroppingNanosAdvancesDuringEpisode(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	s := newTestSnake(cfg)

	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	droppingNanos := exp.counters["SnakeOltpReadDroppingNanosTotal"]
	require.NotNil(t, droppingNanos)
	assert.Equal(t, int64(0), droppingNanos(), "no dropping time before contention")

	// Drive a dropping episode: hold the slot and pile on contenders.
	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	errCh := make(chan error, 5)
	for range 5 {
		go func() {
			_, err := s.Acquire(t.Context(), "", 0)
			errCh <- err
		}()
	}
	time.Sleep(200 * time.Millisecond)
	unlock.Release()
	for range 5 {
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("goroutine did not return")
		}
	}

	assert.Positive(t, droppingNanos(), "an induced dropping episode should accumulate dropping nanoseconds")
}

func TestDroppingNanos_IntegratesExactlyOverEpisode(t *testing.T) {
	// Hermetic test of the accumulator math with an injected clock: no timing
	// dependence. Drive dropping true→false transitions directly through the
	// observer and assert the accumulated nanoseconds match the clock deltas.
	s := newTestSnake(defaultSnakeConfig())
	var now int64
	s.clockFunc = func() int64 { return now }

	// Not dropping yet: nothing accrues, and the open-segment flush is a no-op.
	assert.Equal(t, int64(0), s.DroppingNanos())

	// Open an episode at t=100 by forcing the queue into the dropping state.
	now = 100
	s.mu.Lock()
	s.q.codelq.dropping = true
	s.lockedObserveDropping()
	s.mu.Unlock()

	// Mid-episode at t=250: the open segment (250-100) is visible before it ends.
	now = 250
	assert.Equal(t, int64(150), s.DroppingNanos(), "open segment must be included before the episode ends")

	// Close the episode at t=400: 300ns total accrued (400-100).
	now = 400
	s.mu.Lock()
	s.q.codelq.dropping = false
	s.lockedObserveDropping()
	s.mu.Unlock()
	assert.Equal(t, int64(300), s.DroppingNanos())

	// Idle stretch adds nothing.
	now = 1000
	assert.Equal(t, int64(300), s.DroppingNanos())

	// A second episode from t=1000 to t=1100 adds 100ns for 400ns total.
	s.mu.Lock()
	s.q.codelq.dropping = true
	s.lockedObserveDropping()
	s.mu.Unlock()
	now = 1100
	s.mu.Lock()
	s.q.codelq.dropping = false
	s.lockedObserveDropping()
	s.mu.Unlock()
	assert.Equal(t, int64(400), s.DroppingNanos())
}
