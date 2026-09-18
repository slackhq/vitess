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

	CoDelConfig struct {
		IntervalNs        func() int64
		InitialIntervalNs func() int64
		TargetNs          func() int64
		InitialTargetNs   func() int64
		Exponent          func() float64
		MinDropDelayNs    func() int64
		EasingLogBase     func() float64
	}

	SnakeConfig struct {
		CoDel            CoDelConfig
		Mode             func() Mode
		DropTimerFired   func()
		ShadowTimerFired func()
	}

	Snake[T any] struct {
		q                    *ValvedCoDelQueue[T]
		cfg                  SnakeConfig
		clockFunc            func() int64
		dropTimer            *time.Timer
		dropTimerArmed       bool
		dropTimerExpectedNs  int64
		shadowTimer          *time.Timer
		shadowTimerArmed     bool
		length               atomic.Int64
		shedCount            atomic.Int64
		shedByPriority       *stats.CountersWithMultiLabels
		acquireByPriority    *stats.CountersWithMultiLabels
		droppingNanos        atomic.Int64
		droppingSinceNs      atomic.Int64
		sojourn              *stats.Histogram
		queueLen             *stats.Histogram
		droppableLen         *stats.Histogram
		interval             *stats.Histogram
		dropCount            *stats.Histogram
		timerLag             *stats.Histogram
		valveDepth           *stats.Histogram
		initialTargetShadow  initialTargetShadowTracker
		shadowRequiredTarget *stats.Histogram
		shadowCensored       atomic.Int64
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
	cfg = normalizeConfig(cfg)
	s := &Snake[T]{
		cfg:                  cfg,
		clockFunc:            defaultClock,
		sojourn:              stats.NewHistogram("", "", loadshedBucketCutoffs),
		queueLen:             stats.NewHistogram("", "", lengthBucketCutoffs),
		droppableLen:         stats.NewHistogram("", "", lengthBucketCutoffs),
		interval:             stats.NewHistogram("", "", intervalBucketCutoffs),
		dropCount:            stats.NewHistogram("", "", lengthBucketCutoffs),
		timerLag:             stats.NewHistogram("", "", loadshedBucketCutoffs),
		valveDepth:           stats.NewHistogram("", "", lengthBucketCutoffs),
		shadowRequiredTarget: stats.NewHistogram("", "", initialTargetShadowMetricCutoffsMs),
	}
	s.q = newValvedCoDelQueue[T](cfg.CoDel, s.clockFunc, s.lockedScheduleDropTimer, s.lockedStopDropTimer, s.mode)
	return s
}

func normalizeConfig(cfg SnakeConfig) SnakeConfig {
	if cfg.Mode == nil {
		cfg.Mode = func() Mode { return ModeOff }
	}
	if cfg.CoDel.IntervalNs == nil {
		cfg.CoDel.IntervalNs = func() int64 { return int64(time.Second) }
	}
	if cfg.CoDel.TargetNs == nil {
		cfg.CoDel.TargetNs = func() int64 { return int64(time.Second) }
	}
	if cfg.CoDel.Exponent == nil {
		cfg.CoDel.Exponent = func() float64 { return 1 }
	}
	if cfg.CoDel.MinDropDelayNs == nil {
		cfg.CoDel.MinDropDelayNs = func() int64 { return int64(100 * time.Millisecond) }
	}
	return cfg
}

func (s *Snake[T]) Enqueue(value T, valveID string, priority float64) (*Request[T], []T) {
	return s.enqueue(value, valveID, priority, true)
}

func (s *Snake[T]) EnqueueExisting(value T, valveID string, priority float64) (*Request[T], []T) {
	return s.enqueue(value, valveID, priority, false)
}

func (s *Snake[T]) enqueue(value T, valveID string, priority float64, recordAcquire bool) (*Request[T], []T) {
	if recordAcquire && s.acquireByPriority != nil {
		s.acquireByPriority.Add([]string{shedPriorityLabel(priority)}, 1)
	}
	req := s.q.lockedEnqueue(valveID, priority)
	req.value = value
	s.length.Add(1)
	if valveID != "" {
		s.valveDepth.Add(int64(s.q.lockedValveDepth(valveID)))
	}
	s.lockedObserveInitialTargetShadow(nil)
	s.lockedStartInitialTargetShadow(req)
	dropped := s.lockedAdvance()
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
	pending := s.lockedAdvance()
	req := s.q.lockedPeek()
	if match != nil {
		req = s.q.lockedFind(match)
	}
	if req == nil {
		var zero T
		return zero, false, s.droppedValues(pending)
	}

	s.q.lockedDequeue(req)
	req.signal(grantSentinel)
	s.length.Add(-1)
	now := s.clockFunc()
	s.lockedAccrueDropping(now)
	sojournNs := now - req.codelqEnqueuedAtNs
	s.lockedObserveInitialTargetShadowAt(now, &sojournNs)
	s.sojourn.Add(sojournNs)
	value := req.value
	var zero T
	req.value = zero
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return value, true, s.droppedValues(pending)
}

func (s *Snake[T]) Len() int {
	return int(s.length.Load())
}

func (s *Snake[T]) CountMatching(match func(T) bool) int {
	count := 0
	for elem := s.q.codelq.queue.Front(); elem != nil; elem = elem.Next() {
		if elem.Value.signaledValue == nil && match(elem.Value.value) {
			count++
		}
	}
	for _, pending := range s.q.valves {
		for _, req := range pending {
			if req != nil && req.signaledValue == nil && match(req.value) {
				count++
			}
		}
	}
	return count
}

func (s *Snake[T]) Drain() []T {
	s.lockedObserveInitialTargetShadow(nil)
	s.q.lockedRunTimerIf(false)

	values := make([]T, 0, s.Len())
	for {
		req := s.q.lockedPeek()
		if req == nil {
			break
		}
		s.q.lockedDequeue(req)
		req.signal(grantSentinel)
		s.length.Add(-1)
		values = append(values, req.value)
		var zero T
		req.value = zero
	}

	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return values
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
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return true, s.droppedValues(s.q.lockedTakePendingDrops())
}

func (s *Snake[T]) CancelMatching(match func(T) bool) (bool, []T) {
	for elem := s.q.codelq.queue.Front(); elem != nil; elem = elem.Next() {
		if elem.Value.signaledValue == nil && match(elem.Value.value) {
			return s.Cancel(elem.Value)
		}
	}
	for _, pending := range s.q.valves {
		for _, req := range pending {
			if req != nil && req.signaledValue == nil && match(req.value) {
				return s.Cancel(req)
			}
		}
	}
	return false, nil
}

func (s *Snake[T]) lockedAdvance() []*Request[T] {
	s.lockedObserveInitialTargetShadow(nil)
	if s.mode() != ModeEnabled {
		s.q.lockedRunTimerIf(false)
		return s.q.lockedTakePendingDrops()
	}

	s.q.lockedRunTimerIf(true)
	s.interval.Add(s.q.lockedCurrentInterval())
	s.dropCount.Add(int64(s.q.lockedCount()))
	return s.q.lockedTakePendingDrops()
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

func shedPriorityLabel(priority float64) string {
	if bucket := bucketFor(priority); bucket >= 0 {
		return strconv.Itoa(maxPriorityBucket - bucket)
	}
	return "overflow"
}

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

func (s *Snake[T]) mode() Mode {
	return s.cfg.Mode()
}

func (s *Snake[T]) lockedObserveLengths() {
	s.queueLen.Add(int64(s.q.lockedLen()))
	s.droppableLen.Add(int64(s.q.lockedDroppableLen()))
}

func (s *Snake[T]) lockedObserveDropping() {
	dropping := !s.q.lockedIsHealthy()
	if dropping == (s.droppingSinceNs.Load() != 0) {
		return
	}
	s.lockedAccrueDropping(s.clockFunc())
}

func (s *Snake[T]) lockedAccrueDropping(now int64) {
	switch {
	case !s.q.lockedIsHealthy() && s.droppingSinceNs.Load() == 0:
		s.droppingSinceNs.Store(now)
	case s.q.lockedIsHealthy() && s.droppingSinceNs.Load() != 0:
		s.droppingNanos.Add(now - s.droppingSinceNs.Swap(0))
	}
}

func (s *Snake[T]) lockedScheduleDropTimer(delayNs int64) {
	if s.dropTimerArmed {
		return
	}
	s.dropTimerArmed = true
	s.dropTimerExpectedNs = s.clockFunc() + delayNs
	if s.cfg.DropTimerFired != nil {
		s.dropTimer = time.AfterFunc(time.Duration(delayNs), s.cfg.DropTimerFired)
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
	s.timerLag.Add(max(s.clockFunc()-s.dropTimerExpectedNs, 0))
	dropped := s.lockedAdvance()
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return s.droppedValues(dropped)
}

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
	if s.initialTargetShadow.start(startedAtNs) {
		s.lockedScheduleShadowTimer(max(startedAtNs+initialTargetShadowMaxIntervalNs-s.clockFunc(), 0))
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
	outcome := s.initialTargetShadow.observe(nowNs, sojournNs, s.q.lockedDroppableLen() == 0)
	if outcome.completed {
		s.shadowRequiredTarget.Add(initialTargetShadowMetricValueMs(outcome.requiredTargetNs))
		s.lockedStopShadowTimer()
	}
}

func (s *Snake[T]) lockedLeaveInitialTargetShadow(nowNs int64) {
	if !s.initialTargetShadow.active && !s.initialTargetShadow.waitingForDrain {
		return
	}
	if s.initialTargetShadow.active &&
		(s.q.lockedDroppableLen() == 0 || nowNs >= s.initialTargetShadow.startedAtNs+initialTargetShadowMaxIntervalNs) {
		s.lockedObserveInitialTargetShadowAt(nowNs, nil)
		s.initialTargetShadow.reset(false)
		return
	}
	if s.initialTargetShadow.active {
		s.shadowCensored.Add(1)
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
		s.shadowTimer = time.AfterFunc(time.Duration(delayNs), s.cfg.ShadowTimerFired)
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
