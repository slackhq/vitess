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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func defaultSnakeConfig() SnakeConfig {
	return SnakeConfig{CoDel: defaultTestConfig()}
}

func TestInitialTargetShadow_SmallestHittingCandidate(t *testing.T) {
	var tracker initialTargetShadowTracker
	require.True(t, tracker.start(0))

	sojourn := int64(6 * time.Millisecond)
	outcome := tracker.observe(int64(150*time.Millisecond), &sojourn, false)
	assert.False(t, outcome.completed)

	outcome = tracker.observe(initialTargetShadowMaxIntervalNs, nil, false)
	require.True(t, outcome.completed)
	assert.Equal(t, int64(10*time.Millisecond), outcome.requiredTargetNs)
}

func TestInitialTargetShadow_StrictBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		atNs       int64
		sojournNs  int64
		wantTarget int64
	}{
		{
			name:       "deadline equality misses",
			atNs:       int64(100 * time.Millisecond),
			sojournNs:  int64(4 * time.Millisecond),
			wantTarget: int64(10 * time.Millisecond),
		},
		{
			name:       "target equality misses",
			atNs:       int64(99 * time.Millisecond),
			sojournNs:  int64(5 * time.Millisecond),
			wantTarget: int64(10 * time.Millisecond),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tracker initialTargetShadowTracker
			require.True(t, tracker.start(0))

			outcome := tracker.observe(tc.atNs, &tc.sojournNs, false)
			assert.False(t, outcome.completed)

			outcome = tracker.observe(initialTargetShadowMaxIntervalNs, nil, false)
			require.True(t, outcome.completed)
			assert.Equal(t, tc.wantTarget, outcome.requiredTargetNs)
		})
	}
}

func TestInitialTargetShadow_NaturalDrainHits(t *testing.T) {
	var tracker initialTargetShadowTracker
	require.True(t, tracker.start(0))

	outcome := tracker.observe(int64(99*time.Millisecond), nil, true)

	require.True(t, outcome.completed)
	assert.Equal(t, int64(5*time.Millisecond), outcome.requiredTargetNs)
	assert.False(t, tracker.active)
	assert.False(t, tracker.waitingForDrain)
}

func TestInitialTargetShadow_DrainAtDeadlineMissesCandidate(t *testing.T) {
	var tracker initialTargetShadowTracker
	require.True(t, tracker.start(0))

	outcome := tracker.observe(int64(100*time.Millisecond), nil, true)

	require.True(t, outcome.completed)
	assert.Equal(t, int64(10*time.Millisecond), outcome.requiredTargetNs)
}

func TestInitialTargetShadow_AllCandidatesMiss(t *testing.T) {
	var tracker initialTargetShadowTracker
	require.True(t, tracker.start(0))

	outcome := tracker.observe(initialTargetShadowMaxIntervalNs, nil, false)

	require.True(t, outcome.completed)
	assert.Equal(t, initialTargetShadowMissNs, outcome.requiredTargetNs)
	assert.True(t, tracker.waitingForDrain)
	assert.False(t, tracker.start(outcome.requiredTargetNs))

	tracker.observe(outcome.requiredTargetNs, nil, true)
	assert.False(t, tracker.waitingForDrain)
	assert.True(t, tracker.start(outcome.requiredTargetNs))
}

func TestInitialTargetShadow_DeadlineCompletesAtMaximumWindow(t *testing.T) {
	var tracker initialTargetShadowTracker
	require.True(t, tracker.start(0))

	outcome := tracker.observe(initialTargetShadowMaxIntervalNs, nil, false)

	assert.True(t, outcome.completed)
	assert.Equal(t, initialTargetShadowMissNs, outcome.requiredTargetNs)
}

// TestInitialTargetShadow_ShadowModeRecordsBurst enqueues a droppable request in
// shadow mode (starting a burst at t=0), then grants it at t=101ms. The grant
// drains the backlog, so the smallest candidate whose interval (target*20) has
// not yet elapsed — 10ms, whose 200ms interval outlasts 101ms — is recorded,
// and nothing is shed.
func TestInitialTargetShadow_ShadowModeRecordsBurst(t *testing.T) {
	var now atomic.Int64
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeShadow }
	s := NewSnake[struct{}](cfg)
	s.clockFunc = now.Load
	s.q.codelq.nowNs = now.Load

	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	histogram := exp.histograms["SnakeOltpReadInitialTargetShadow20xNs"]
	require.NotNil(t, histogram)

	_, dropped := s.Enqueue(struct{}{}, "", 0)
	require.Empty(t, dropped)
	require.True(t, s.initialTargetShadow.active)

	now.Store(int64(101 * time.Millisecond))
	_, ok, dropped := s.Dequeue()
	require.True(t, ok)
	require.Empty(t, dropped)

	assert.Equal(t, int64(1), histogram.Count())
	assert.Equal(t, int64(1), histogram.Counts()["10000000"])
	assert.Equal(t, int64(0), s.ShedCount())
}

// TestInitialTargetShadow_ShadowModeDoesNotRunCoDel verifies that a droppable
// enqueue in shadow mode starts a shadow burst but leaves the CoDel controller
// completely idle: no drop timer, no dropping state, no control-law warming, and
// no interval/drop-count observations.
func TestInitialTargetShadow_ShadowModeDoesNotRunCoDel(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeShadow }
	s := NewSnake[struct{}](cfg)

	_, dropped := s.Enqueue(struct{}{}, "", 0)
	require.Empty(t, dropped)
	require.True(t, s.initialTargetShadow.active)

	assert.False(t, s.dropTimerArmed)
	assert.False(t, s.q.codelq.dropping)
	assert.Zero(t, s.q.codelq.dropNextNs)
	assert.Equal(t, 1, s.q.codelq.count)
	assert.Zero(t, s.interval.Count())
	assert.Zero(t, s.dropCount.Count())
}

// TestInitialTargetShadow_OffModeRunsNeitherCoDelNorShadow verifies that in off
// mode a droppable enqueue runs neither the CoDel controller nor the shadow
// backtest: the queue behaves as a plain FIFO.
func TestInitialTargetShadow_OffModeRunsNeitherCoDelNorShadow(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeOff }
	s := NewSnake[struct{}](cfg)

	_, dropped := s.Enqueue(struct{}{}, "", 0)
	require.Empty(t, dropped)
	require.Equal(t, 1, s.q.lockedDroppableLen())

	assert.False(t, s.dropTimerArmed)
	assert.False(t, s.shadowTimerArmed)
	assert.False(t, s.initialTargetShadow.active)
	assert.Zero(t, s.initialTargetShadowRequired.Count())
}

// TestInitialTargetShadow_StartsIndependentlyOfControllerCount verifies a burst
// starts on the backlog transition regardless of the live CoDel count: every
// fresh burst is modeled as starting fully relaxed.
func TestInitialTargetShadow_StartsIndependentlyOfControllerCount(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeShadow }
	s := NewSnake[struct{}](cfg)

	s.q.codelq.count = 2
	req := s.q.lockedEnqueue("", 0)
	s.lockedStartInitialTargetShadow(req)

	assert.True(t, s.initialTargetShadow.active)
}

// TestInitialTargetShadow_StartsAtBacklogTransition verifies a burst is anchored
// to the request's enqueue time (the backlog-transition instant), not the wall
// clock at the time the burst logic runs.
func TestInitialTargetShadow_StartsAtBacklogTransition(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeShadow }
	s := NewSnake[struct{}](cfg)
	s.clockFunc = func() int64 { return int64(2 * time.Millisecond) }
	s.q.codelq.nowNs = func() int64 { return int64(time.Millisecond) }

	req := s.q.lockedEnqueue("", 0)
	s.lockedStartInitialTargetShadow(req)
	startedAtNs := s.initialTargetShadow.startedAtNs

	assert.Equal(t, int64(time.Millisecond), startedAtNs)
}

// TestInitialTargetShadow_RuntimeDisableDoesNotStartWithExistingBacklog verifies
// that if shadow mode is entered while a droppable backlog already exists, no
// burst is started for the pre-existing backlog (a burst only begins on a fresh
// 0->1 transition).
func TestInitialTargetShadow_RuntimeDisableDoesNotStartWithExistingBacklog(t *testing.T) {
	var enabled atomic.Bool
	enabled.Store(true)
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode {
		if enabled.Load() {
			return ModeEnabled
		}
		return ModeShadow
	}
	s := NewSnake[struct{}](cfg)

	first := s.q.lockedEnqueue("", 0)
	s.lockedStartInitialTargetShadow(first)
	enabled.Store(false)
	second := s.q.lockedEnqueue("", 0)
	s.lockedStartInitialTargetShadow(second)

	assert.False(t, s.initialTargetShadow.active)
}

// TestInitialTargetShadow_EnabledCoDelCannotRecordShadowSample verifies that an
// observation taken while the mode is enabled (not shadow) censors an active
// burst rather than recording a sample.
func TestInitialTargetShadow_EnabledCoDelCannotRecordShadowSample(t *testing.T) {
	var enabled atomic.Bool
	enabled.Store(true)
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode {
		if enabled.Load() {
			return ModeEnabled
		}
		return ModeShadow
	}
	s := NewSnake[struct{}](cfg)

	s.q.lockedEnqueue("", 0)
	require.True(t, s.initialTargetShadow.start(s.clockFunc()))
	s.lockedObserveInitialTargetShadow(nil)

	assert.Equal(t, int64(0), s.initialTargetShadowRequired.Count())
	assert.Equal(t, int64(1), s.initialTargetShadowCensored.Load())
}

// TestInitialTargetShadow_LeavingShadowForOffCensorsBurst verifies that leaving
// shadow mode (for off) while a burst is active and the backlog has not drained
// censors the burst instead of recording a sample.
func TestInitialTargetShadow_LeavingShadowForOffCensorsBurst(t *testing.T) {
	var shadowing atomic.Bool
	shadowing.Store(true)
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode {
		if shadowing.Load() {
			return ModeShadow
		}
		return ModeOff
	}
	s := NewSnake[struct{}](cfg)

	s.q.lockedEnqueue("", 0)
	require.True(t, s.initialTargetShadow.start(s.clockFunc()))
	shadowing.Store(false)
	s.lockedObserveInitialTargetShadow(nil)

	assert.Equal(t, int64(0), s.initialTargetShadowRequired.Count())
	assert.Equal(t, int64(1), s.initialTargetShadowCensored.Load())
}

// TestInitialTargetShadow_LeavingShadowClearsWaitingForDrain verifies that
// leaving shadow mode clears a lingering waitingForDrain state so a subsequent
// shadow burst can start. The mode change is observed lazily via
// lockedObserveInitialTargetShadow (the smartconnpool queue has no RefreshMode).
func TestInitialTargetShadow_LeavingShadowClearsWaitingForDrain(t *testing.T) {
	var shadowing atomic.Bool
	shadowing.Store(true)
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode {
		if shadowing.Load() {
			return ModeShadow
		}
		return ModeOff
	}
	s := NewSnake[struct{}](cfg)

	s.initialTargetShadow.reset(true)

	shadowing.Store(false)
	s.lockedObserveInitialTargetShadow(nil)
	assert.False(t, s.initialTargetShadow.waitingForDrain)

	shadowing.Store(true)
	req := s.q.lockedEnqueue("", 0)
	s.lockedStartInitialTargetShadow(req)
	assert.True(t, s.initialTargetShadow.active)
	s.lockedStopShadowTimer()
}

// TestInitialTargetShadow_DeadlineTimerCompletesWithoutTraffic verifies the
// shadow backstop timer completes a burst as a full miss (+Inf) when no traffic
// resolves it before the maximum window elapses.
func TestInitialTargetShadow_DeadlineTimerCompletesWithoutTraffic(t *testing.T) {
	var now atomic.Int64
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeShadow }
	s := NewSnake[struct{}](cfg)
	s.clockFunc = now.Load
	s.q.codelq.nowNs = now.Load

	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	histogram := exp.histograms["SnakeOltpReadInitialTargetShadow20xNs"]
	require.NotNil(t, histogram)

	_, dropped := s.Enqueue(struct{}{}, "", 0)
	require.Empty(t, dropped)
	require.True(t, s.shadowTimerArmed)

	now.Store(initialTargetShadowMaxIntervalNs)
	s.LockedShadowTimerFired()

	assert.Equal(t, int64(1), histogram.Count())
	assert.Equal(t, int64(1), histogram.Counts()["inf"])
}

// TestInitialTargetShadow_FinalCancellationCountsAsDrain verifies that cancelling
// the last droppable request drains the backlog and completes the burst, hitting
// the smallest candidate (5ms) whose interval (100ms) has not yet elapsed at
// t=99ms.
func TestInitialTargetShadow_FinalCancellationCountsAsDrain(t *testing.T) {
	var now atomic.Int64
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeShadow }
	s := NewSnake[struct{}](cfg)
	s.clockFunc = now.Load
	s.q.codelq.nowNs = now.Load

	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	histogram := exp.histograms["SnakeOltpReadInitialTargetShadow20xNs"]
	require.NotNil(t, histogram)

	req, dropped := s.Enqueue(struct{}{}, "", 0)
	require.Empty(t, dropped)
	require.True(t, s.initialTargetShadow.active)

	now.Store(int64(99 * time.Millisecond))
	cancelled, dropped := s.Cancel(req)
	require.True(t, cancelled)
	require.Empty(t, dropped)

	assert.Equal(t, int64(1), histogram.Count())
	assert.Equal(t, int64(1), histogram.Counts()["5000000"])
}
