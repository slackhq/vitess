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
	"log"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/stats"
)

type (
	// SnakeConfig configures a Snake. Functions are used to allow dynamic runtime
	// tuning.
	SnakeConfig struct {
		Name                string
		CoDel               CoDelConfig
		Capacity            func() int
		LoadsheddingAllowed func() bool
		AcquireError        func() error
		ReleaseCBs          []func(error)
	}

	// Snake is a CoDel-based load-shedding gate with dynamic capacity. Up to
	// Capacity() concurrent holders are allowed. Acquire requests are either
	// granted or dropped, each within a timely manner.
	//
	// Granted requests stay in the CoDel queue as undroppable until Release,
	// preserving the queue's system-pressure signal for accurate shedding.
	Snake struct {
		mu sync.Mutex

		q              *ValvedCoDelQueue
		holders        map[*Request]struct{}
		dropTimer      *time.Timer
		dropTimerArmed bool
		// dropTimerExpectedNs is the clock time the drop timer was scheduled to
		// fire (arm time + delay), used to measure how late it actually fires.
		dropTimerExpectedNs int64
		cfg                 SnakeConfig
		clockFunc           func() int64

		shedCount atomic.Int64

		sojourn      *stats.Histogram
		queueLen     *stats.Histogram
		droppableLen *stats.Histogram
		holderCount  *stats.Histogram
		interval     *stats.Histogram
		dropCount    *stats.Histogram
		timerLag     *stats.Histogram
		valveDepth   *stats.Histogram

		droppingNanos   int64
		droppingSinceNs int64
	}

	// SafeUnlock is a handle for releasing a slot. Only the goroutine that
	// acquired the slot should call Release. Release is idempotent.
	SafeUnlock struct {
		s    *Snake
		req  *Request
		once sync.Once
		err  error
	}
)

var epoch = time.Now()

func defaultClock() int64 {
	return time.Since(epoch).Nanoseconds()
}

// NewSnake creates a new CoDel-based load-shedding gate.
func NewSnake(cfg SnakeConfig) *Snake {
	s := &Snake{
		cfg:          cfg,
		clockFunc:    defaultClock,
		holders:      make(map[*Request]struct{}),
		sojourn:      stats.NewHistogram("", "", loadshedBucketCutoffs),
		queueLen:     stats.NewHistogram("", "", lengthBucketCutoffs),
		droppableLen: stats.NewHistogram("", "", lengthBucketCutoffs),
		holderCount:  stats.NewHistogram("", "", lengthBucketCutoffs),
		interval:     stats.NewHistogram("", "", intervalBucketCutoffs),
		dropCount:    stats.NewHistogram("", "", lengthBucketCutoffs),
		timerLag:     stats.NewHistogram("", "", loadshedBucketCutoffs),
		valveDepth:   stats.NewHistogram("", "", lengthBucketCutoffs),
	}
	s.q = newValvedCoDelQueue(cfg.CoDel, defaultClock, s.lockedScheduleDropTimer, s.lockedStopDropTimer)
	return s
}

func (s *Snake) lockedObserveLengths() {
	s.queueLen.Add(int64(s.q.lockedLen()))
	s.droppableLen.Add(int64(s.q.lockedDroppableLen()))
}

func (s *Snake) lockedObserveValveDepth(valveID string) {
	s.valveDepth.Add(int64(s.q.lockedValveDepth(valveID)))
}

func (s *Snake) capacity() int {
	if s.cfg.Capacity == nil {
		return 1
	}
	return max(s.cfg.Capacity(), 1)
}

func (s *Snake) hasCapacity() bool {
	return len(s.holders) < s.capacity()
}

// Acquire acquires a slot. It blocks until a slot is granted, the request
// is dropped by CoDel, or the context is cancelled. The returned SafeUnlock
// must be released via defer unlock.Release().
//
// An empty valveID is valid: such requests bypass the per-valve fairness
// layer but still pass through the CoDel gate. Callers should pass the valve
// ID through unconditionally rather than gating Acquire on a non-empty ID,
// which would silently exclude all unkeyed traffic from load shedding.
//
// The priority ordering convention is that lower-valued priorities indicate
// less important requests. Lower values are shed first.
func (s *Snake) Acquire(ctx context.Context, valveID string, priority float64) (*SafeUnlock, error) {
	priority = s.priority(priority)

	s.mu.Lock()
	req := s.q.lockedEnqueue(valveID, priority)
	if valveID != "" {
		s.lockedObserveValveDepth(valveID)
	}

	if s.hasCapacity() && req.codelqElem != nil {
		s.lockedGrant(req)
		s.lockedObserveLengths()
		s.mu.Unlock()
		return &SafeUnlock{s: s, req: req}, nil
	}

	s.lockedObserveLengths()
	s.lockedObserveDropping()
	s.mu.Unlock()

	select {
	case val := <-req.signalChan:
		if val != grantSentinel {
			return nil, s.acquireError()
		}
		return &SafeUnlock{s: s, req: req}, nil

	case <-ctx.Done():
		// Race: Go's select picks randomly when both signalChan and
		// ctx.Done() are ready simultaneously. The grant may have already
		// been sent (or be in-flight) by the time we land here. The inner
		// select resolves this:
		//   - If signalChan has a grant: we own the slot, release it.
		//   - If signalChan is empty: the grant might still be in-flight
		//     (releaser unlocked the mutex but hasn't sent the signal yet).
		//     We re-acquire the mutex and check holders — if we're already
		//     granted, release; otherwise cancel from the queue.
		select {
		case val := <-req.signalChan:
			if val == grantSentinel {
				s.releaseOnCancel(req)
			}
		default:
			s.mu.Lock()
			if _, granted := s.holders[req]; granted {
				s.mu.Unlock()
				s.releaseOnCancel(req)
			} else {
				s.q.lockedCancel(req)
				s.lockedObserveLengths()
				s.lockedObserveDropping()
				s.mu.Unlock()
			}
		}
		return nil, ctx.Err()
	}
}

// IsHealthy reports whether the CoDel queue is healthy.
func (s *Snake) IsHealthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.q.lockedIsHealthy()
}

// SnakeStats is a point-in-time snapshot of Snake's internal state.
type SnakeStats struct {
	QueueLen        int
	DroppableLen    int
	HolderCount     int
	Dropping        bool
	DropCount       int
	CurrentInterval int64 // ns
}

// Stats returns a point-in-time snapshot of Snake's internal state.
func (s *Snake) Stats() SnakeStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SnakeStats{
		QueueLen:        s.q.codelq.lockedLen(),
		DroppableLen:    s.q.codelq.droppableLen,
		HolderCount:     len(s.holders),
		Dropping:        s.q.codelq.dropping,
		DropCount:       s.q.codelq.count,
		CurrentInterval: s.q.codelq.lockedCurrentInterval(),
	}
}

// Release releases the slot. exc is an optional error that caused the release
// (passed to release callbacks). Release is idempotent.
func (u *SafeUnlock) Release(exc ...error) error {
	u.once.Do(func() {
		var excValue error
		if len(exc) > 0 {
			excValue = exc[0]
		}
		u.err = u.s.release(u.req, excValue)
	})
	return u.err
}

func (s *Snake) release(req *Request, excValue error) error {
	s.mu.Lock()
	// Release is idempotent and can race a context-cancel release for the same
	// req. The loser sees the req already gone from holders and no-ops.
	if _, ok := s.holders[req]; !ok {
		s.mu.Unlock()
		return nil
	}
	delete(s.holders, req)
	s.lockedObserveHolderCount()
	s.lockedCompleteAndShed(req)
	s.lockedTryGrantOne()
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	s.mu.Unlock()

	s.runReleaseCBs(excValue)
	return nil
}

func (s *Snake) releaseOnCancel(req *Request) {
	s.mu.Lock()
	delete(s.holders, req)
	s.lockedObserveHolderCount()
	s.lockedCompleteAndShed(req)
	s.lockedTryGrantOne()
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	s.mu.Unlock()
}

// lockedCompleteAndShed unlinks the released request and, when an episode is
// active, sheds stale requests synchronously at this dequeue point using a
// fresh clock — so shedding tracks target continuously instead of waiting on
// the (possibly late) backstop timer, and runs before granting the next waiter
// so we promote the freshest survivor. The lockedNeedsAdvance guard keeps the
// healthy path free of both the advance call and the clock read.
func (s *Snake) lockedCompleteAndShed(req *Request) {
	s.q.lockedComplete(req)
	if s.q.lockedNeedsAdvance() {
		s.q.lockedDequeue()
	}
}

func (s *Snake) lockedGrant(req *Request) {
	s.holders[req] = struct{}{}
	s.lockedObserveHolderCount()
	s.q.lockedOnGrant(req)
	now := s.clockFunc()
	s.lockedAccrueDropping(now)
	s.sojourn.Add(now - req.codelqEnqueuedAtNs)
	req.signal(grantSentinel)
}

func (s *Snake) lockedObserveHolderCount() {
	s.holderCount.Add(int64(len(s.holders)))
}

func (s *Snake) lockedObserveDropping() {
	dropping := !s.q.lockedIsHealthy()
	if dropping == (s.droppingSinceNs != 0) {
		return
	}
	s.lockedAccrueDropping(s.clockFunc())
}

func (s *Snake) lockedAccrueDropping(now int64) {
	dropping := !s.q.lockedIsHealthy()
	switch {
	case dropping && s.droppingSinceNs == 0:
		s.droppingSinceNs = now
	case !dropping && s.droppingSinceNs != 0:
		s.droppingNanos += now - s.droppingSinceNs
		s.droppingSinceNs = 0
	}
}

func (s *Snake) lockedTryGrantOne() {
	if !s.hasCapacity() {
		return
	}
	next := s.q.lockedFirstWaiting()
	if next != nil {
		s.lockedGrant(next)
	}
}

// runReleaseCBs executes release callbacks outside the mutex.
func (s *Snake) runReleaseCBs(excValue error) {
	for _, cb := range s.cfg.ReleaseCBs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("loadshed: panic in release callback for %s: %v", s.cfg.Name, r)
				}
			}()
			cb(excValue)
		}()
	}
}

func (s *Snake) priority(priority float64) float64 {
	if s.cfg.LoadsheddingAllowed != nil && !s.cfg.LoadsheddingAllowed() {
		return PriorityUndroppable
	}
	return priority
}

func (s *Snake) acquireError() error {
	s.shedCount.Add(1)
	if s.cfg.AcquireError != nil {
		return s.cfg.AcquireError()
	}
	return &DroppedRequestError{}
}

// ShedCount returns the cumulative number of requests this Snake has shed.
// Context cancellations are not counted — only gate-driven drops.
func (s *Snake) ShedCount() int64 {
	return s.shedCount.Load()
}

func (s *Snake) DroppingNanos() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := s.droppingNanos
	if s.droppingSinceNs != 0 {
		total += s.clockFunc() - s.droppingSinceNs
	}
	return total
}

// --- timer management (must be called with s.mu held) ---

// backstopFloorNs is the minimum delay for the drop timer. Shedding is now
// driven synchronously from the release (dequeue) path, so the timer only needs
// to backstop a stuck/quiet queue — one with a droppable backlog but no release
// traffic to advance it. Flooring the arm delay well above the CoDel control
// interval keeps the timer coarse (far fewer time.AfterFunc re-arms, so less
// runtime-timer-lock churn) without affecting the real-time drop rate, which
// releases now pace.
const backstopFloorNs = int64(5 * time.Millisecond)

func (s *Snake) lockedScheduleDropTimer(delayNs int64) {
	if s.dropTimerArmed {
		return
	}
	s.dropTimerArmed = true
	if delayNs < backstopFloorNs {
		delayNs = backstopFloorNs
	}
	s.dropTimerExpectedNs = s.clockFunc() + delayNs
	delay := time.Duration(delayNs) * time.Nanosecond
	s.dropTimer = time.AfterFunc(delay, s.runDropTimer)
}

func (s *Snake) lockedStopDropTimer() {
	if !s.dropTimerArmed {
		return
	}
	s.dropTimerArmed = false
	s.dropTimer.Stop()
}

func (s *Snake) runDropTimer() {
	s.mu.Lock()
	if !s.dropTimerArmed {
		s.mu.Unlock()
		return
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
	s.q.lockedRunTimer()
	s.interval.Add(s.q.lockedCurrentInterval())
	s.dropCount.Add(int64(s.q.lockedCount()))
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	s.mu.Unlock()
}
