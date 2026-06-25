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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
)

// fakeExporter captures the GaugeFuncs and CounterFuncs registered by
// PublishStats so the test can invoke them directly, without touching global
// stats registration.
type fakeExporter struct {
	gauges   map[string]func() int64
	counters map[string]func() int64
}

func newFakeExporter() *fakeExporter {
	return &fakeExporter{
		gauges:   make(map[string]func() int64),
		counters: make(map[string]func() int64),
	}
}

func (e *fakeExporter) NewGaugeFunc(name, _ string, f func() int64) *stats.GaugeFunc {
	e.gauges[name] = f
	return nil
}

func (e *fakeExporter) NewCounterFunc(name, _ string, f func() int64) *stats.CounterFunc {
	e.counters[name] = f
	return nil
}

func TestPublishStats_ReflectsSnakeState(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())
	exp := newFakeExporter()

	PublishStats(exp, "SnakeOltpRead", s)

	// All six fields are registered under the prefix.
	for _, field := range []string{"QueueLen", "DroppableLen", "HolderCount", "Dropping", "DropCount", "CurrentIntervalNs"} {
		_, ok := exp.gauges["SnakeOltpRead"+field]
		assert.True(t, ok, "expected gauge SnakeOltpRead%s to be registered", field)
	}

	// Idle: no holders.
	assert.Equal(t, int64(0), exp.gauges["SnakeOltpReadHolderCount"]())

	// Acquire a slot (capacity defaults to 1) and confirm the gauge tracks it.
	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), exp.gauges["SnakeOltpReadHolderCount"]())

	// The gauge closure reads live Stats(), so it matches after release too.
	require.NoError(t, unlock.Release())
	assert.Equal(t, int64(s.Stats().HolderCount), exp.gauges["SnakeOltpReadHolderCount"]())

	// CurrentIntervalNs mirrors the raw ns value from Stats (not a unit-converted one).
	assert.Equal(t, s.Stats().CurrentInterval, exp.gauges["SnakeOltpReadCurrentIntervalNs"]())
}

func TestPublishStats_PrefixIsolation(t *testing.T) {
	// Two snakes registered under different prefixes through the same exporter
	// must not collide — this is the two-pool case (oltp-read vs dml).
	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", newTestSnake(defaultSnakeConfig()))
	PublishStats(exp, "SnakeDml", newTestSnake(defaultSnakeConfig()))

	assert.Contains(t, exp.gauges, "SnakeOltpReadQueueLen")
	assert.Contains(t, exp.gauges, "SnakeDmlQueueLen")
	assert.Len(t, exp.gauges, 12, "expected 6 gauges per snake, two snakes")

	assert.Contains(t, exp.counters, "SnakeOltpReadShedCount")
	assert.Contains(t, exp.counters, "SnakeDmlShedCount")
	assert.Len(t, exp.counters, 2, "expected 1 shed counter per snake, two snakes")
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
