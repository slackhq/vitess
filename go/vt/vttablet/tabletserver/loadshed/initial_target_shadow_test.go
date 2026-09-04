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
)

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

func TestInitialTargetShadow_ShadowModeRecordsBurst(t *testing.T) {
	var now atomic.Int64
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 1 }
	cfg.Mode = func() Mode { return ModeShadow }
	s := newTestSnake(cfg)
	s.clockFunc = now.Load
	s.q.codelq.nowNs = now.Load
	t.Cleanup(func() {
		s.mu.Lock()
		s.lockedStopDropTimer()
		s.lockedStopInitialTargetShadowTimer()
		s.mu.Unlock()
	})

	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	histogram := exp.histograms["SnakeOltpReadInitialTargetShadow20xNs"]
	require.NotNil(t, histogram)

	holder, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	type result struct {
		unlock *testSafeUnlock
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		unlock, err := s.Acquire(t.Context(), "", 0)
		resultCh <- result{unlock: unlock, err: err}
	}()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.initialTargetShadow.active
	}, time.Second, time.Millisecond)

	now.Store(int64(101 * time.Millisecond))
	require.NoError(t, holder.Release())

	waiter := <-resultCh
	require.NoError(t, waiter.err)
	require.NotNil(t, waiter.unlock)
	require.NoError(t, waiter.unlock.Release())

	assert.Equal(t, int64(1), histogram.Count())
	assert.Equal(t, int64(1), histogram.Counts()["10000000"])
	assert.Equal(t, int64(0), s.ShedCount())
}

func TestInitialTargetShadow_ShadowModeDoesNotRunCoDel(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 1 }
	cfg.Mode = func() Mode { return ModeShadow }
	s := newTestSnake(cfg)
	t.Cleanup(func() {
		s.mu.Lock()
		s.lockedStopDropTimer()
		s.lockedStopInitialTargetShadowTimer()
		s.mu.Unlock()
	})

	holder, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Acquire(ctx, "", 0)
		errCh <- err
	}()

	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.initialTargetShadow.active
	}, time.Second, time.Millisecond)

	s.mu.Lock()
	assert.False(t, s.dropTimer.armed)
	assert.False(t, s.q.codelq.dropping)
	assert.Zero(t, s.q.codelq.dropNextNs)
	assert.Equal(t, 1, s.q.codelq.count)
	assert.Zero(t, s.interval.Count())
	assert.Zero(t, s.dropCount.Count())
	s.mu.Unlock()

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
	require.NoError(t, holder.Release())
}

func TestInitialTargetShadow_OffModeRunsNeitherCoDelNorShadow(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 1 }
	cfg.Mode = func() Mode { return ModeOff }
	s := newTestSnake(cfg)
	t.Cleanup(func() {
		s.mu.Lock()
		s.lockedStopDropTimer()
		s.lockedStopInitialTargetShadowTimer()
		s.mu.Unlock()
	})

	holder, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Acquire(ctx, "", 0)
		errCh <- err
	}()

	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.q.lockedDroppableLen() == 1
	}, time.Second, time.Millisecond)

	s.mu.Lock()
	assert.False(t, s.dropTimer.armed)
	assert.False(t, s.initialTargetShadowTimer.armed)
	assert.False(t, s.initialTargetShadow.active)
	assert.Zero(t, s.initialTargetShadowRequired.Count())
	s.mu.Unlock()

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
	require.NoError(t, holder.Release())
}

func TestInitialTargetShadow_StartsIndependentlyOfControllerCount(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeShadow }
	s := newTestSnake(cfg)
	t.Cleanup(func() {
		s.mu.Lock()
		s.lockedStopDropTimer()
		s.lockedStopInitialTargetShadowTimer()
		s.mu.Unlock()
	})

	s.mu.Lock()
	s.q.codelq.count = 2
	req := s.q.lockedEnqueue("", 0)
	s.lockedStartInitialTargetShadow(req)
	s.mu.Unlock()

	assert.True(t, s.initialTargetShadow.active)
}

func TestInitialTargetShadow_StartsAtBacklogTransition(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeShadow }
	s := newTestSnake(cfg)
	s.clockFunc = func() int64 { return int64(2 * time.Millisecond) }
	s.q.codelq.nowNs = func() int64 { return int64(time.Millisecond) }
	t.Cleanup(func() {
		s.mu.Lock()
		s.lockedStopDropTimer()
		s.lockedStopInitialTargetShadowTimer()
		s.mu.Unlock()
	})

	s.mu.Lock()
	req := s.q.lockedEnqueue("", 0)
	s.lockedStartInitialTargetShadow(req)
	startedAtNs := s.initialTargetShadow.startedAtNs
	s.mu.Unlock()

	assert.Equal(t, int64(time.Millisecond), startedAtNs)
}

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
	s := newTestSnake(cfg)
	t.Cleanup(func() {
		s.mu.Lock()
		s.lockedStopDropTimer()
		s.lockedStopInitialTargetShadowTimer()
		s.mu.Unlock()
	})

	s.mu.Lock()
	first := s.q.lockedEnqueue("", 0)
	s.lockedStartInitialTargetShadow(first)
	enabled.Store(false)
	second := s.q.lockedEnqueue("", 0)
	s.lockedStartInitialTargetShadow(second)
	s.mu.Unlock()

	assert.False(t, s.initialTargetShadow.active)
}

func TestInitialTargetShadow_CoDelPassUsesSingleModeSnapshot(t *testing.T) {
	var modeReads atomic.Int64
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode {
		modeReads.Add(1)
		return ModeEnabled
	}
	s := newTestSnake(cfg)

	s.mu.Lock()
	s.q.lockedEnqueue("", 0)
	modeReads.Store(0)
	require.True(t, s.initialTargetShadow.start(s.clockFunc()))
	s.lockedRunCoDelTimer()
	s.mu.Unlock()

	assert.Equal(t, int64(1), modeReads.Load())
	assert.Equal(t, int64(1), s.initialTargetShadowCensored.Load())
}

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
	s := newTestSnake(cfg)

	s.mu.Lock()
	s.q.lockedEnqueue("", 0)
	require.True(t, s.initialTargetShadow.start(s.clockFunc()))
	s.lockedObserveInitialTargetShadow(nil)
	s.mu.Unlock()

	assert.Equal(t, int64(0), s.initialTargetShadowRequired.Count())
	assert.Equal(t, int64(1), s.initialTargetShadowCensored.Load())
}

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
	s := newTestSnake(cfg)

	s.mu.Lock()
	s.q.lockedEnqueue("", 0)
	require.True(t, s.initialTargetShadow.start(s.clockFunc()))
	shadowing.Store(false)
	s.lockedObserveInitialTargetShadow(nil)
	s.mu.Unlock()

	assert.Equal(t, int64(0), s.initialTargetShadowRequired.Count())
	assert.Equal(t, int64(1), s.initialTargetShadowCensored.Load())
}

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
	s := newTestSnake(cfg)
	t.Cleanup(func() {
		s.mu.Lock()
		s.lockedStopInitialTargetShadowTimer()
		s.mu.Unlock()
	})

	s.mu.Lock()
	s.initialTargetShadow.reset(true)
	s.mu.Unlock()

	shadowing.Store(false)
	s.RefreshMode()

	s.mu.Lock()
	assert.False(t, s.initialTargetShadow.waitingForDrain)
	s.mu.Unlock()

	shadowing.Store(true)
	s.mu.Lock()
	req := s.q.lockedEnqueue("", 0)
	s.lockedStartInitialTargetShadow(req)
	assert.True(t, s.initialTargetShadow.active)
	s.lockedStopInitialTargetShadowTimer()
	s.mu.Unlock()
}

func TestInitialTargetShadow_DeadlineTimerCompletesWithoutTraffic(t *testing.T) {
	var now atomic.Int64
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 1 }
	cfg.Mode = func() Mode { return ModeShadow }
	s := newTestSnake(cfg)
	s.clockFunc = now.Load
	s.q.codelq.nowNs = now.Load
	t.Cleanup(func() {
		s.mu.Lock()
		s.lockedStopDropTimer()
		s.lockedStopInitialTargetShadowTimer()
		s.mu.Unlock()
	})

	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	histogram := exp.histograms["SnakeOltpReadInitialTargetShadow20xNs"]

	holder, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Acquire(ctx, "", 0)
		errCh <- err
	}()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.initialTargetShadowTimer.armed
	}, time.Second, time.Millisecond)

	s.mu.Lock()
	generation := s.initialTargetShadowTimer.generation
	s.mu.Unlock()
	now.Store(initialTargetShadowMaxIntervalNs)
	s.runInitialTargetShadowTimer(generation)

	assert.Equal(t, int64(1), histogram.Count())
	assert.Equal(t, int64(1), histogram.Counts()["inf"])

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
	require.NoError(t, holder.Release())
}

func TestInitialTargetShadow_StaleTimerDoesNotConsumeNextBurst(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeShadow }
	s := newTestSnake(cfg)

	s.mu.Lock()
	require.True(t, s.initialTargetShadow.start(0))
	s.lockedScheduleInitialTargetShadowTimer(initialTargetShadowMaxIntervalNs)
	staleGeneration := s.initialTargetShadowTimer.generation
	s.initialTargetShadow.reset(false)
	s.lockedStopInitialTargetShadowTimer()

	require.True(t, s.initialTargetShadow.start(1))
	s.lockedScheduleInitialTargetShadowTimer(initialTargetShadowMaxIntervalNs)
	currentGeneration := s.initialTargetShadowTimer.generation
	s.mu.Unlock()

	s.runInitialTargetShadowTimer(staleGeneration)

	s.mu.Lock()
	assert.True(t, s.initialTargetShadow.active)
	assert.True(t, s.initialTargetShadowTimer.armed)
	assert.Equal(t, currentGeneration, s.initialTargetShadowTimer.generation)
	s.lockedStopInitialTargetShadowTimer()
	s.mu.Unlock()
}

func TestInitialTargetShadow_FinalCancellationCountsAsDrain(t *testing.T) {
	var now atomic.Int64
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 1 }
	cfg.Mode = func() Mode { return ModeShadow }
	s := newTestSnake(cfg)
	s.clockFunc = now.Load
	s.q.codelq.nowNs = now.Load
	t.Cleanup(func() {
		s.mu.Lock()
		s.lockedStopDropTimer()
		s.lockedStopInitialTargetShadowTimer()
		s.mu.Unlock()
	})

	exp := newFakeExporter()
	PublishStats(exp, "SnakeOltpRead", s)
	histogram := exp.histograms["SnakeOltpReadInitialTargetShadow20xNs"]

	holder, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Acquire(ctx, "", 0)
		errCh <- err
	}()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.initialTargetShadow.active
	}, time.Second, time.Millisecond)

	now.Store(int64(99 * time.Millisecond))
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
	require.NoError(t, holder.Release())

	assert.Equal(t, int64(1), histogram.Count())
	assert.Equal(t, int64(1), histogram.Counts()["5000000"])
}
