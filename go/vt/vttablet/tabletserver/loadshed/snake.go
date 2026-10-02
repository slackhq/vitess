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
	Mode string

	// SnakeConfig uses callbacks so runtime changes do not require rebuilding the queue.
	SnakeConfig struct {
		CoDel            CoDelConfig
		DefaultPriority  int
		Mode             func() Mode
		DropTimerFired   func()
		ShadowTimerFired func()
	}

	// Snake owns queueing and shedding; its caller owns execution capacity and handoff.
	Snake[T any] struct {
		q              *CoDelQueue[T]
		dropTimer      *time.Timer
		dropTimerArmed bool
		// Kept separately from dropNextNs to measure scheduler delay.
		dropTimerExpectedNs int64
		shadowTimer         *time.Timer
		shadowTimerArmed    bool
		cfg                 SnakeConfig
		clockFunc           func() int64
		length              atomic.Int64

		shedCount atomic.Int64
		// shedByPriority breaks shedCount down by the shed request's priority label
		// ("0" undroppable, "1" most important .. "100" least), so operators can
		// see whether the queue is correctly shedding low-priority traffic first
		// rather than eating high-priority requests. Nil until PublishStats
		// registers it (tests build a Snake without it); the shed path nil-checks.
		// Its sum equals shedCount.
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

		initialTargetShadow         initialTargetShadowTracker
		initialTargetShadowRequired *stats.Histogram
		initialTargetShadowCensored atomic.Int64

		droppingNanos   atomic.Int64
		droppingSinceNs atomic.Int64

		priorityDequeueReordered  atomic.Int64
		priorityDequeueForcedHead atomic.Int64
	}
)

const (
	ModeOff     Mode = "off"
	ModeShadow  Mode = "shadow"
	ModeEnabled Mode = "enabled"

	keepDroppableFloor = 4
)

var epoch = time.Now()

func defaultClock() int64 {
	return time.Since(epoch).Nanoseconds()
}

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

		initialTargetShadowRequired: stats.NewHistogram("", "", initialTargetShadowMetricCutoffsMs),
	}
	s.q = newCoDelQueue[T](cfg.CoDel, defaultClock, s.lockedScheduleDropTimer, s.lockedStopDropTimer)
	return s
}

func (s *Snake[T]) lockedObserveLengths() {
	s.queueLen.Add(int64(s.q.lockedLen()))
	s.droppableLen.Add(int64(s.q.droppableLen))
}

func (s *Snake[T]) Enqueue(value T, priority int) (*Request[T], []T) {
	return s.enqueue(value, priority, true)
}

func (s *Snake[T]) EnqueueExisting(value T, priority int) (*Request[T], []T) {
	return s.enqueue(value, priority, false)
}

func (s *Snake[T]) enqueue(value T, priority int, recordAcquire bool) (*Request[T], []T) {
	if !IsValidPriority(priority) {
		priority = s.cfg.DefaultPriority
	}
	if recordAcquire && s.acquireByPriority != nil {
		s.acquireByPriority.Add([]string{strconv.Itoa(priority)}, 1)
	}
	req := newRequest(value, priority)
	s.q.lockedEnqueueIf(req, s.loadsheddingAllowed())
	s.length.Add(1)
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
	pending := s.lockedEnqueueAdvance()
	req := s.lockedSelect(match)
	var value T
	ok := false
	if req != nil {
		s.q.lockedDequeue(req)
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

// lockedSelect picks the request to grant. While the head is over target, it
// grants the most important over-target request instead: any over-target pick
// yields the same CoDel health decision as the head, and leaves lower-priority
// requests queued as drop candidates. The head is granted after maxSkips
// bypasses so it cannot starve.
func (s *Snake[T]) lockedSelect(match func(T) bool) *Request[T] {
	head := s.q.lockedPeek()
	maxSkips := s.cfg.CoDel.priorityDequeueMaxSkips()
	if maxSkips <= 0 || head == nil || !s.loadsheddingAllowed() {
		return s.lockedSelectFIFO(match)
	}
	cutoffNs := s.q.nowNs() - s.q.lockedTargetNs()
	if head.codelqEnqueuedAtNs > cutoffNs {
		return s.lockedSelectFIFO(match)
	}
	if head.skips >= maxSkips {
		s.priorityDequeueForcedHead.Add(1)
		return head
	}
	req := s.q.byPriority.firstOverTarget(cutoffNs, match)
	if req == nil || req == head {
		return head
	}
	head.skips++
	s.priorityDequeueReordered.Add(1)
	return req
}

func (s *Snake[T]) lockedSelectFIFO(match func(T) bool) *Request[T] {
	if match != nil {
		return s.q.lockedFind(match)
	}
	return s.q.lockedPeek()
}

func (cfg CoDelConfig) keepDroppableFloor() int {
	if cfg.KeepDroppableFloor == nil {
		return keepDroppableFloor
	}
	return cfg.KeepDroppableFloor()
}

func (cfg CoDelConfig) priorityDequeueMaxSkips() int {
	if cfg.PriorityDequeueMaxSkips == nil {
		return 0
	}
	return cfg.PriorityDequeueMaxSkips()
}

func (s *Snake[T]) Len() int {
	return int(s.length.Load())
}

// CountMatching requires the caller to hold the mutex protecting the Snake.
func (s *Snake[T]) CountMatching(match func(T) bool) int {
	count := 0
	for elem := s.q.queue.Front(); elem != nil; elem = elem.Next() {
		req := elem.Value
		if match(req.value) {
			count++
		}
	}
	return count
}

// Drain bypasses shedding and requires the caller to hold the parent mutex.
func (s *Snake[T]) Drain() []T {
	s.lockedObserveInitialTargetShadow(nil)
	s.q.lockedDisable()

	values := make([]T, 0, s.Len())
	for {
		req := s.q.lockedPeek()
		if req == nil {
			break
		}
		s.q.lockedDequeue(req)
		s.length.Add(-1)
		values = append(values, req.value)
		var zero T
		req.value = zero
	}

	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return values
}

func (s *Snake[T]) Cancel(req *Request[T]) bool {
	if req.codelqElem == nil {
		return false
	}
	s.q.lockedRemove(req)
	s.length.Add(-1)
	var zero T
	req.value = zero
	s.lockedObserveInitialTargetShadow(nil)
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return true
}

func (s *Snake[T]) CancelMatching(match func(T) bool) bool {
	for elem := s.q.queue.Front(); elem != nil; elem = elem.Next() {
		req := elem.Value
		if match(req.value) {
			return s.Cancel(req)
		}
	}
	return false
}

// lockedEnqueueAdvance lets arrivals drive shedding before the backstop timer.
func (s *Snake[T]) lockedEnqueueAdvance() []*Request[T] {
	s.lockedObserveInitialTargetShadow(nil)
	if !s.loadsheddingAllowed() {
		s.q.lockedDisable()
		return nil
	}

	s.q.lockedEnable()
	var dropped []*Request[T]
	s.q.lockedRunTimer(func() bool {
		if s.q.droppableLen <= s.cfg.CoDel.keepDroppableFloor() {
			return false
		}
		elem := s.q.lockedFindLowestPriorityDroppable()
		if elem == nil {
			return false
		}
		req := elem.Value
		s.q.lockedRemove(req)
		dropped = append(dropped, req)
		return true
	})
	s.interval.Add(s.q.lockedCurrentInterval())
	s.dropCount.Add(int64(s.q.count))
	return dropped
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
		return ModeOff
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
			s.shedByPriority.Add([]string{strconv.Itoa(req.priority)}, 1)
		}
		values[i] = req.value
		var zero T
		req.value = zero
	}
	return values
}

// ShedCount excludes context cancellations.
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
	// Timer lag exposes CPU contention that delays shedding.
	if lag := s.clockFunc() - s.dropTimerExpectedNs; lag > 0 {
		s.timerLag.Add(lag)
	} else {
		s.timerLag.Add(0)
	}
	s.lockedObserveInitialTargetShadow(nil)
	dropped := s.lockedEnqueueAdvance()
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return s.droppedValues(dropped)
}

func (s *Snake[T]) lockedStartInitialTargetShadow(req *Request[T]) {
	if !req.isDroppable() ||
		req.codelqElem == nil ||
		s.q.droppableLen != 1 ||
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
		s.q.droppableLen == 0,
	)
	if outcome.completed {
		s.initialTargetShadowRequired.Add(initialTargetShadowMetricValueMs(outcome.requiredTargetNs))
		s.lockedStopShadowTimer()
	}
}

func (s *Snake[T]) lockedLeaveInitialTargetShadow(nowNs int64) {
	if !s.initialTargetShadow.active && !s.initialTargetShadow.waitingForDrain {
		return
	}

	if s.initialTargetShadow.active &&
		(s.q.droppableLen == 0 ||
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

func (s *Snake[T]) LockedShadowTimerFired() {
	if !s.shadowTimerArmed {
		return
	}
	s.shadowTimerArmed = false
	s.lockedObserveInitialTargetShadow(nil)
}
