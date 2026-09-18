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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newShadowTestSnake(mode func() Mode) (*Snake[struct{}], *atomic.Int64) {
	var now atomic.Int64
	s := NewSnake[struct{}](SnakeConfig{
		Mode: mode,
		CoDel: CoDelConfig{
			IntervalNs:     func() int64 { return (10 * time.Millisecond).Nanoseconds() },
			TargetNs:       func() int64 { return time.Millisecond.Nanoseconds() },
			Exponent:       func() float64 { return 1 },
			MinDropDelayNs: func() int64 { return 1 },
		},
	})
	s.clockFunc = now.Load
	s.q.codelq.nowNs = now.Load
	return s, &now
}

func TestInitialTargetShadowSmallestHittingCandidate(t *testing.T) {
	var tracker initialTargetShadowTracker
	require.True(t, tracker.start(0))

	sojourn := int64(6 * time.Millisecond)
	assert.False(t, tracker.observe(int64(150*time.Millisecond), &sojourn, false).completed)
	outcome := tracker.observe(initialTargetShadowMaxIntervalNs, nil, false)

	require.True(t, outcome.completed)
	assert.Equal(t, int64(10*time.Millisecond), outcome.requiredTargetNs)
}

func TestInitialTargetShadowStrictBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		at      time.Duration
		sojourn time.Duration
		want    time.Duration
	}{
		{name: "deadline", at: 100 * time.Millisecond, sojourn: 4 * time.Millisecond, want: 10 * time.Millisecond},
		{name: "target", at: 99 * time.Millisecond, sojourn: 5 * time.Millisecond, want: 10 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tracker initialTargetShadowTracker
			require.True(t, tracker.start(0))
			sojourn := int64(tt.sojourn)
			assert.False(t, tracker.observe(int64(tt.at), &sojourn, false).completed)
			outcome := tracker.observe(initialTargetShadowMaxIntervalNs, nil, false)
			require.True(t, outcome.completed)
			assert.Equal(t, int64(tt.want), outcome.requiredTargetNs)
		})
	}
}

func TestInitialTargetShadowDrainOutcome(t *testing.T) {
	tests := []struct {
		name string
		at   time.Duration
		want time.Duration
	}{
		{name: "before deadline", at: 99 * time.Millisecond, want: 5 * time.Millisecond},
		{name: "at deadline", at: 100 * time.Millisecond, want: 10 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tracker initialTargetShadowTracker
			require.True(t, tracker.start(0))
			outcome := tracker.observe(int64(tt.at), nil, true)
			require.True(t, outcome.completed)
			assert.Equal(t, int64(tt.want), outcome.requiredTargetNs)
			assert.False(t, tracker.active)
			assert.False(t, tracker.waitingForDrain)
		})
	}
}

func TestInitialTargetShadowAllCandidatesMiss(t *testing.T) {
	var tracker initialTargetShadowTracker
	require.True(t, tracker.start(0))

	outcome := tracker.observe(initialTargetShadowMaxIntervalNs, nil, false)

	require.True(t, outcome.completed)
	assert.Equal(t, initialTargetShadowMissNs, outcome.requiredTargetNs)
	assert.True(t, tracker.waitingForDrain)
	assert.False(t, tracker.start(initialTargetShadowMaxIntervalNs))
	tracker.observe(initialTargetShadowMaxIntervalNs, nil, true)
	assert.False(t, tracker.waitingForDrain)
}

func TestInitialTargetShadowMetricContract(t *testing.T) {
	s, now := newShadowTestSnake(func() Mode { return ModeShadow })
	exporter := newFakeExporter()
	PublishStats(exporter, "SnakeOltpRead", s)
	histogram := exporter.histograms["SnakeOltpReadInitialTargetShadow20xMs"]

	require.NotNil(t, histogram)
	assert.Equal(t, []int64{5, 10, 20, 40, 80, 160, 320, 640}, histogram.Cutoffs())
	assert.True(t, strings.Contains(exporter.histogramHelp["SnakeOltpReadInitialTargetShadow20xMs"], "milliseconds"))

	_, dropped := s.Enqueue(struct{}{}, "", 0)
	require.Empty(t, dropped)
	now.Store(int64(101 * time.Millisecond))
	_, ok, dropped := s.Dequeue()
	require.True(t, ok)
	require.Empty(t, dropped)

	assert.Equal(t, int64(1), histogram.Count())
	assert.Equal(t, int64(1), histogram.Counts()["10"])
	assert.Equal(t, int64(10), histogram.Total())
}

func TestInitialTargetShadowModeDoesNotRunCoDel(t *testing.T) {
	s, _ := newShadowTestSnake(func() Mode { return ModeShadow })

	_, dropped := s.Enqueue(struct{}{}, "", 0)
	require.Empty(t, dropped)

	assert.True(t, s.initialTargetShadow.active)
	assert.False(t, s.dropTimerArmed)
	assert.False(t, s.q.codelq.dropping)
	assert.Zero(t, s.q.codelq.dropNextNs)
	assert.Equal(t, 1, s.q.codelq.count)
	assert.Zero(t, s.interval.Count())
	assert.Zero(t, s.dropCount.Count())
}

func TestInitialTargetOffModeRunsNeitherControllerNorShadow(t *testing.T) {
	s, _ := newShadowTestSnake(func() Mode { return ModeOff })

	_, dropped := s.Enqueue(struct{}{}, "", 0)
	require.Empty(t, dropped)

	assert.Equal(t, 1, s.q.lockedDroppableLen())
	assert.False(t, s.dropTimerArmed)
	assert.False(t, s.shadowTimerArmed)
	assert.False(t, s.initialTargetShadow.active)
	assert.Zero(t, s.shadowRequiredTarget.Count())
}

func TestInitialTargetShadowStartsAtBacklogTransition(t *testing.T) {
	s, now := newShadowTestSnake(func() Mode { return ModeShadow })
	now.Store(int64(time.Millisecond))
	req := s.q.lockedEnqueue("", 0)
	now.Store(int64(2 * time.Millisecond))

	s.lockedStartInitialTargetShadow(req)

	assert.Equal(t, int64(time.Millisecond), s.initialTargetShadow.startedAtNs)
}

func TestInitialTargetShadowStartsIndependentlyOfControllerCount(t *testing.T) {
	s, _ := newShadowTestSnake(func() Mode { return ModeShadow })
	s.q.count = 2
	req := newRequest(struct{}{}, 0)
	s.q.lockedEnqueueIf(req, false)

	s.lockedStartInitialTargetShadow(req)

	assert.True(t, s.initialTargetShadow.active)
}

func TestInitialTargetShadowDoesNotStartWithExistingBacklog(t *testing.T) {
	var shadow atomic.Bool
	s, _ := newShadowTestSnake(func() Mode {
		if shadow.Load() {
			return ModeShadow
		}
		return ModeEnabled
	})
	s.Enqueue(struct{}{}, "", 0)
	shadow.Store(true)

	s.Enqueue(struct{}{}, "", 0)

	assert.False(t, s.initialTargetShadow.active)
}

func TestInitialTargetShadowEnabledModeCannotRecordSample(t *testing.T) {
	s, _ := newShadowTestSnake(func() Mode { return ModeEnabled })
	s.q.lockedEnqueueIf(newRequest(struct{}{}, 0), false)
	require.True(t, s.initialTargetShadow.start(s.clockFunc()))

	s.lockedObserveInitialTargetShadow(nil)

	assert.Zero(t, s.shadowRequiredTarget.Count())
	assert.Equal(t, int64(1), s.shadowCensored.Load())
	assert.False(t, s.initialTargetShadow.active)
}

func TestInitialTargetShadowLeavingModeCensorsBurst(t *testing.T) {
	var shadow atomic.Bool
	shadow.Store(true)
	s, _ := newShadowTestSnake(func() Mode {
		if shadow.Load() {
			return ModeShadow
		}
		return ModeOff
	})
	s.Enqueue(struct{}{}, "", 0)
	require.True(t, s.initialTargetShadow.active)

	shadow.Store(false)
	s.lockedObserveInitialTargetShadow(nil)

	assert.Zero(t, s.shadowRequiredTarget.Count())
	assert.Equal(t, int64(1), s.shadowCensored.Load())
	assert.False(t, s.initialTargetShadow.active)
}

func TestInitialTargetShadowLeavingModeClearsWaitingForDrain(t *testing.T) {
	var shadow atomic.Bool
	shadow.Store(true)
	s, _ := newShadowTestSnake(func() Mode {
		if shadow.Load() {
			return ModeShadow
		}
		return ModeOff
	})
	s.initialTargetShadow.reset(true)

	shadow.Store(false)
	s.lockedObserveInitialTargetShadow(nil)

	assert.False(t, s.initialTargetShadow.waitingForDrain)
	shadow.Store(true)
	req := newRequest(struct{}{}, 0)
	s.q.lockedEnqueueIf(req, false)
	s.lockedStartInitialTargetShadow(req)
	assert.True(t, s.initialTargetShadow.active)
	s.lockedStopShadowTimer()
}

func TestInitialTargetShadowDeadlineTimerCompletesWithoutTraffic(t *testing.T) {
	s, now := newShadowTestSnake(func() Mode { return ModeShadow })
	s.Enqueue(struct{}{}, "", 0)
	require.True(t, s.shadowTimerArmed)

	now.Store(initialTargetShadowMaxIntervalNs)
	s.LockedShadowTimerFired()

	assert.Equal(t, int64(1), s.shadowRequiredTarget.Count())
	assert.Equal(t, int64(1), s.shadowRequiredTarget.Counts()["inf"])
}

func TestInitialTargetShadowFinalCancellationCountsAsDrain(t *testing.T) {
	s, now := newShadowTestSnake(func() Mode { return ModeShadow })
	req, dropped := s.Enqueue(struct{}{}, "", 0)
	require.Empty(t, dropped)
	now.Store(int64(99 * time.Millisecond))

	cancelled, dropped := s.Cancel(req)

	require.True(t, cancelled)
	require.Empty(t, dropped)
	assert.Equal(t, int64(1), s.shadowRequiredTarget.Counts()["5"])
}
