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
	// SnakeConfig configures a Snake. Functions are used to allow dynamic runtime
	// tuning.
	SnakeConfig struct {
		CoDel               CoDelConfig
		LoadsheddingAllowed func() bool
	}

	// Snake is a CoDel-based load-shedding queue. It decides which waiting
	// request may proceed; the caller owns execution capacity and handoff.
	Snake[T any] struct {
		q                *ValvedCoDelQueue[T]
		dropTimerArmed   bool
		dropTimerChanged bool
		dropTimerDelayNs int64
		// dropTimerExpectedNs is the clock time the drop timer was scheduled to
		// fire (arm time + delay), used to measure how late it actually fires.
		dropTimerExpectedNs int64
		cfg                 SnakeConfig
		clockFunc           func() int64

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

		droppingNanos   atomic.Int64
		droppingSinceNs atomic.Int64
	}
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
	}
	s.q = newValvedCoDelQueue[T](cfg.CoDel, defaultClock, s.lockedScheduleDropTimer, s.lockedStopDropTimer)
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
	if valveID != "" {
		s.lockedObserveValveDepth(valveID)
	}
	dropped := s.lockedEnqueueAdvance()
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return req, s.droppedValues(dropped)
}

func (s *Snake[T]) Dequeue() (T, bool, []T) {
	var pending []*Request[T]
	if s.q.lockedNeedsAdvance() {
		pending = s.lockedEnqueueAdvance()
	}
	req := s.q.lockedPeek()
	var value T
	ok := false
	if req != nil {
		s.q.lockedDequeue(req)
		req.signal(grantSentinel)
		now := s.clockFunc()
		s.lockedAccrueDropping(now)
		s.sojourn.Add(now - req.codelqEnqueuedAtNs)
		value = req.value
		ok = true
		var zero T
		req.value = zero
	}
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	return value, ok, s.droppedValues(pending)
}

func (s *Snake[T]) Cancel(req *Request[T]) (bool, []T) {
	if req.signaledValue != nil {
		return false, nil
	}
	s.q.lockedCancel(req)
	var zero T
	req.value = zero
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
	s.q.lockedRunTimerIf(s.loadsheddingAllowed)
	s.interval.Add(s.q.lockedCurrentInterval())
	s.dropCount.Add(int64(s.q.lockedCount()))
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
	return s.cfg.LoadsheddingAllowed == nil || s.cfg.LoadsheddingAllowed()
}

func (s *Snake[T]) droppedValues(requests []*Request[T]) []T {
	if len(requests) == 0 {
		return nil
	}
	values := make([]T, len(requests))
	for i, req := range requests {
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
	s.dropTimerChanged = true
	s.dropTimerDelayNs = delayNs
	s.dropTimerExpectedNs = s.clockFunc() + delayNs
}

func (s *Snake[T]) lockedStopDropTimer() {
	s.dropTimerArmed = false
}

func (s *Snake[T]) LockedTimerUpdate() (time.Duration, bool) {
	if !s.dropTimerChanged {
		return 0, false
	}
	s.dropTimerChanged = false
	return time.Duration(s.dropTimerDelayNs) * time.Nanosecond, true
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
	s.q.lockedRunTimerIf(s.loadsheddingAllowed)
	s.interval.Add(s.q.lockedCurrentInterval())
	s.dropCount.Add(int64(s.q.lockedCount()))
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	dropped := s.q.lockedTakePendingDrops()
	return s.droppedValues(dropped)
}
