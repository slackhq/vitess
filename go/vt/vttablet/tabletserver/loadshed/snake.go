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
	"strconv"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/stats"
)

type (
	// Mode selects which load-shedding mechanism is active.
	Mode string

	// SnakeConfig configures a Snake. Functions are used to allow dynamic runtime
	// tuning.
	SnakeConfig struct {
		CoDel            CoDelConfig
		Mode             func() Mode
		DropTimerFired   func()
		ShadowTimerFired func()
	}

	// Snake is a CoDel-based load-shedding queue. It decides which waiting
	// request may proceed; the caller owns execution capacity and handoff.
	Snake[T any] struct {
		q              *ValvedCoDelQueue[T]
		dropTimer      *time.Timer
		dropTimerArmed bool
		// dropTimerExpectedNs is the clock time the drop timer was scheduled to
		// fire (arm time + delay), used to measure how late it actually fires.
		dropTimerExpectedNs int64
		shadowTimer         *time.Timer
		shadowTimerArmed    bool
		cfg                 SnakeConfig
		clockFunc           func() int64
		length              atomic.Int64

		shedCount atomic.Int64
		// shedByPriority breaks shedCount down by the shed request's priority label
		// (the caller's original query priority: "0" most important .. "100" least,
		// "overflow"), so operators can see whether the queue is correctly shedding
		// low-priority traffic first rather than eating high-priority requests. Nil
		// until PublishStats registers it (tests and the benchmark build a Snake
		// without it); the shed path nil-checks. Its sum equals shedCount.
		shedByPriority *stats.CountersWithMultiLabels
		// acquireByPriority counts every enqueue, labeled by the same caller
		// priority as shedByPriority, so shed rate per priority class can be
		// computed exactly (shedByPriority / acquireByPriority) rather than from
		// assumed offered-load weights. Nil until PublishStats registers it.
		acquireByPriority *stats.CountersWithMultiLabels

		sojourn      *stats.Histogram
		queueLen     *stats.Histogram
		droppableLen *stats.Histogram
		interval     *stats.Histogram
		dropCount    *stats.Histogram
		timerLag     *stats.Histogram
		valveDepth   *stats.Histogram

		initialTargetShadow         initialTargetShadowTracker
		initialTargetShadowRequired *stats.Histogram
		initialTargetShadowCensored atomic.Int64

		droppingNanos   atomic.Int64
		droppingSinceNs atomic.Int64
	}
)

const (
	ModeOff     Mode = "off"
	ModeShadow  Mode = "shadow"
	ModeEnabled Mode = "enabled"
)

var epoch = time.Now()

func defaultClock() int64 {
	return time.Since(epoch).Nanoseconds()
}

// NewSnake creates a new CoDel-based load-shedding queue.
func NewSnake[T any](cfg SnakeConfig) *Snake[T] {
	s := &Snake[T]{
		cfg:          cfg,
		clockFunc:    defaultClock,
		sojourn:      stats.NewHistogram("", "", loadshedBucketCutoffs),
		queueLen:     stats.NewHistogram("", "", lengthBucketCutoffs),
		droppableLen: stats.NewHistogram("", "", lengthBucketCutoffs),
		interval:     stats.NewHistogram("", "", intervalBucketCutoffs),
		dropCount:    stats.NewHistogram("", "", lengthBucketCutoffs),
		timerLag:     stats.NewHistogram("", "", loadshedBucketCutoffs),
		valveDepth:   stats.NewHistogram("", "", lengthBucketCutoffs),

		initialTargetShadowRequired: stats.NewHistogram("", "", initialTargetShadowCandidates),
	}
	s.q = newValvedCoDelQueue[T](cfg.CoDel, defaultClock, s.lockedScheduleDropTimer, s.lockedStopDropTimer, s.mode)
	return s
}

func (s *Snake[T]) lockedObserveLengths() {
	s.queueLen.Add(int64(s.q.lockedLen()))
	s.droppableLen.Add(int64(s.q.lockedDroppableLen()))
}

func (s *Snake[T]) lockedObserveValveDepth(valveID string) {
	s.valveDepth.Add(int64(s.q.lockedValveDepth(valveID)))
}

func (s *Snake[T]) Enqueue(value T, valveID string, priority float64) (*Request[T], []T) {
	if s.acquireByPriority != nil {
		s.acquireByPriority.Add([]string{shedPriorityLabel(priority)}, 1)
	}

	req := s.q.lockedEnqueue(valveID, priority)
	req.value = value
	s.length.Add(1)
	if valveID != "" {
		s.lockedObserveValveDepth(valveID)
	}
	s.lockedObserveInitialTargetShadow(nil)
	s.lockedStartInitialTargetShadow(req)
	dropped := s.lockedEnqueueAdvance()
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return req, s.droppedValues(dropped)
}

func (s *Snake[T]) Dequeue() (T, bool, []T) {
	return s.dequeue(nil)
}

func (s *Snake[T]) DequeueMatching(match func(T) bool) (T, bool, []T) {
	return s.dequeue(match)
}

func (s *Snake[T]) dequeue(match func(T) bool) (T, bool, []T) {
	var pending []*Request[T]
	if s.q.lockedNeedsAdvance() {
		pending = s.lockedEnqueueAdvance()
	}
	req := s.q.lockedPeek()
	if match != nil {
		req = s.q.lockedFind(match)
	}
	var value T
	ok := false
	if req != nil {
		s.q.lockedDequeue(req)
		req.signal(grantSentinel)
		s.length.Add(-1)
		now := s.clockFunc()
		s.lockedAccrueDropping(now)
		sojournNs := now - req.codelqEnqueuedAtNs
		s.lockedObserveInitialTargetShadowAt(now, &sojournNs)
		s.sojourn.Add(sojournNs)
		value = req.value
		ok = true
		var zero T
		req.value = zero
	}
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return value, ok, s.droppedValues(pending)
}

func (s *Snake[T]) Len() int {
	return int(s.length.Load())
}

func (s *Snake[T]) Cancel(req *Request[T]) (bool, []T) {
	if req.signaledValue != nil {
		return false, nil
	}
	s.q.lockedCancel(req)
	s.length.Add(-1)
	var zero T
	req.value = zero
	s.lockedObserveInitialTargetShadow(nil)
	dropped := s.q.lockedTakePendingDrops()
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return true, s.droppedValues(dropped)
}

// lockedEnqueueAdvance runs the CoDel control-law advance on every enqueue so
// an arrival can drive shedding, not just the dequeue path and the backstop
// timer. The pending drops are returned so the caller can signal them after
// releasing the parent mutex.
func (s *Snake[T]) lockedEnqueueAdvance() []*Request[T] {
	s.lockedObserveInitialTargetShadow(nil)
	enabled := s.loadsheddingAllowed()
	s.q.lockedRunTimerIf(enabled)
	if enabled {
		s.interval.Add(s.q.lockedCurrentInterval())
		s.dropCount.Add(int64(s.q.lockedCount()))
	}
	return s.q.lockedTakePendingDrops()
}

func (s *Snake[T]) lockedObserveDropping() {
	dropping := !s.q.lockedIsHealthy()
	if dropping == (s.droppingSinceNs.Load() != 0) {
		return
	}
	s.lockedAccrueDropping(s.clockFunc())
}

func (s *Snake[T]) lockedAccrueDropping(now int64) {
	dropping := !s.q.lockedIsHealthy()
	switch {
	case dropping && s.droppingSinceNs.Load() == 0:
		s.droppingSinceNs.Store(now)
	case !dropping && s.droppingSinceNs.Load() != 0:
		s.droppingNanos.Add(now - s.droppingSinceNs.Swap(0))
	}
}

func (s *Snake[T]) loadsheddingAllowed() bool {
	return s.mode() == ModeEnabled
}

func (s *Snake[T]) mode() Mode {
	if s.cfg.Mode == nil {
		return ModeEnabled
	}
	return s.cfg.Mode()
}

func (s *Snake[T]) droppedValues(requests []*Request[T]) []T {
	if len(requests) == 0 {
		return nil
	}
	values := make([]T, len(requests))
	for i, req := range requests {
		s.length.Add(-1)
		s.shedCount.Add(1)
		if s.shedByPriority != nil {
			s.shedByPriority.Add([]string{shedPriorityLabel(req.priority)}, 1)
		}
		values[i] = req.value
		var zero T
		req.value = zero
	}
	return values
}

// shedPriorityLabel maps a request's internal Snake priority to its shed-metric
// label, reported as the ORIGINAL caller priority (the value passed to the query,
// where 0 is most important) rather than the internal Snake value. The caller
// inverts on the way in (snake = maxPriorityBucket - caller, so lower Snake value
// sheds first); we invert back here so the label matches what was passed in.
// Out-of-range/non-integer/PriorityUndroppable values fall in "overflow".
func shedPriorityLabel(priority float64) string {
	if b := bucketFor(priority); b >= 0 {
		return strconv.Itoa(maxPriorityBucket - b)
	}
	return "overflow"
}

// ShedCount returns the cumulative number of requests this Snake has shed.
// Context cancellations are not counted — only queue-driven drops.
func (s *Snake[T]) ShedCount() int64 {
	return s.shedCount.Load()
}

func (s *Snake[T]) DroppingNanos() int64 {
	total := s.droppingNanos.Load()
	if since := s.droppingSinceNs.Load(); since != 0 {
		total += s.clockFunc() - since
	}
	return total
}

// --- timer management (must be called with the parent mutex held) ---

func (s *Snake[T]) lockedScheduleDropTimer(delayNs int64) {
	if s.dropTimerArmed {
		return
	}
	s.dropTimerArmed = true
	s.dropTimerExpectedNs = s.clockFunc() + delayNs
	if s.cfg.DropTimerFired != nil {
		s.dropTimer = time.AfterFunc(time.Duration(delayNs)*time.Nanosecond, s.cfg.DropTimerFired)
	}
}

func (s *Snake[T]) lockedStopDropTimer() {
	if !s.dropTimerArmed {
		return
	}
	s.dropTimerArmed = false
	if s.dropTimer != nil {
		s.dropTimer.Stop()
	}
}

func (s *Snake[T]) LockedDropTimerFired() []T {
	if !s.dropTimerArmed {
		return nil
	}
	s.dropTimerArmed = false
	// Record how late this fire is versus when it was scheduled. Under CPU
	// contention the normal-priority timer goroutine can fire well past its
	// deadline, which delays shedding; this surfaces that lag.
	if lag := s.clockFunc() - s.dropTimerExpectedNs; lag > 0 {
		s.timerLag.Add(lag)
	} else {
		s.timerLag.Add(0)
	}
	s.lockedObserveInitialTargetShadow(nil)
	enabled := s.loadsheddingAllowed()
	s.q.lockedRunTimerIf(enabled)
	if enabled {
		s.interval.Add(s.q.lockedCurrentInterval())
		s.dropCount.Add(int64(s.q.lockedCount()))
	}
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	dropped := s.q.lockedTakePendingDrops()
	return s.droppedValues(dropped)
}

// --- initial-target shadow backtesting (must be called with the parent mutex held) ---

func (s *Snake[T]) lockedStartInitialTargetShadow(req *Request[T]) {
	if !req.isDroppable() ||
		req.codelqElem == nil ||
		s.q.lockedDroppableLen() != 1 ||
		s.initialTargetShadow.active ||
		s.initialTargetShadow.waitingForDrain ||
		s.mode() != ModeShadow {
		return
	}
	startedAtNs := req.codelqEnqueuedAtNs
	nowNs := s.clockFunc()
	if s.initialTargetShadow.start(startedAtNs) {
		s.lockedScheduleShadowTimer(
			max(startedAtNs+initialTargetShadowMaxIntervalNs-nowNs, 0),
		)
	}
}

func (s *Snake[T]) lockedObserveInitialTargetShadow(sojournNs *int64) {
	if !s.initialTargetShadow.active && !s.initialTargetShadow.waitingForDrain {
		return
	}
	if s.mode() != ModeShadow {
		s.lockedLeaveInitialTargetShadow(s.clockFunc())
		return
	}
	s.lockedObserveInitialTargetShadowAt(s.clockFunc(), sojournNs)
}

func (s *Snake[T]) lockedObserveInitialTargetShadowAt(nowNs int64, sojournNs *int64) {
	if !s.initialTargetShadow.active && !s.initialTargetShadow.waitingForDrain {
		return
	}
	outcome := s.initialTargetShadow.observe(
		nowNs,
		sojournNs,
		s.q.lockedDroppableLen() == 0,
	)
	if outcome.completed {
		s.initialTargetShadowRequired.Add(outcome.requiredTargetNs)
		s.lockedStopShadowTimer()
	}
}

func (s *Snake[T]) lockedLeaveInitialTargetShadow(nowNs int64) {
	if !s.initialTargetShadow.active && !s.initialTargetShadow.waitingForDrain {
		return
	}

	if s.initialTargetShadow.active &&
		(s.q.lockedDroppableLen() == 0 ||
			nowNs >= s.initialTargetShadow.startedAtNs+initialTargetShadowMaxIntervalNs) {
		s.lockedObserveInitialTargetShadowAt(nowNs, nil)
		s.initialTargetShadow.reset(false)
		return
	}

	if s.initialTargetShadow.active {
		s.initialTargetShadowCensored.Add(1)
	}
	s.initialTargetShadow.reset(false)
	s.lockedStopShadowTimer()
}

func (s *Snake[T]) lockedScheduleShadowTimer(delayNs int64) {
	if s.shadowTimerArmed {
		return
	}
	s.shadowTimerArmed = true
	if s.cfg.ShadowTimerFired != nil {
		s.shadowTimer = time.AfterFunc(time.Duration(delayNs)*time.Nanosecond, s.cfg.ShadowTimerFired)
	}
}

func (s *Snake[T]) lockedStopShadowTimer() {
	if !s.shadowTimerArmed {
		return
	}
	s.shadowTimerArmed = false
	if s.shadowTimer != nil {
		s.shadowTimer.Stop()
	}
}

// LockedShadowTimerFired advances the initial-target shadow backtest when its
// backstop timer fires. Called by the caller under the parent mutex.
func (s *Snake[T]) LockedShadowTimerFired() {
	if !s.shadowTimerArmed {
		return
	}
	s.shadowTimerArmed = false
	s.lockedObserveInitialTargetShadow(nil)
}
