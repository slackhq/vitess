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
	"strconv"
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

		// IdleGatingEnabled, when non-nil and returning true, withholds grants
		// from requests at or above the MinConcurrency floor until an idle
		// signal arrives via TryGrantIdle. When nil or false, grants happen on
		// capacity exactly as if these hooks were absent.
		IdleGatingEnabled func() bool

		// MinConcurrency is the floor below which requests are granted
		// immediately regardless of the idle gate, keeping a minimum number of
		// requests in flight. Nil means the floor is unbounded (every request
		// is below the floor), which disables idle gating.
		MinConcurrency func() int

		// OnGatedWaiter is invoked (outside the mutex) when a request becomes
		// grantable but is held back solely by the idle gate. It signals an
		// external idle granter to attempt TryGrantIdle.
		OnGatedWaiter func()
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
		// shedByPriority breaks shedCount down by the shed request's priority label
		// (the caller's original query priority: "0" most important .. "100" least,
		// "overflow"), so operators can see whether the gate is correctly shedding
		// low-priority traffic first rather than eating high-priority requests. Nil
		// until PublishStats registers it (tests and the benchmark build a Snake
		// without it); the shed path nil-checks. Its sum equals shedCount.
		shedByPriority *stats.CountersWithMultiLabels
		// acquireByPriority counts every Acquire, labeled by the same caller
		// priority as shedByPriority, so shed rate per priority class can be
		// computed exactly (shedByPriority / acquireByPriority) rather than from
		// assumed offered-load weights. Nil until PublishStats registers it.
		acquireByPriority *stats.CountersWithMultiLabels

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
	if cfg.IdleGatingEnabled != nil && cfg.OnGatedWaiter == nil {
		// Idle gating cannot be honored without a notifier to wake an idle
		// granter, so it will be ignored (grant-on-capacity). Warn rather than
		// silently strand gated waiters.
		log.Printf("loadshed: snake %s has IdleGatingEnabled but no OnGatedWaiter; idle gating disabled", cfg.Name)
	}
	s := &Snake{
		cfg:          cfg,
		clockFunc:    defaultClock,
		holders:      make(map[*Request]struct{}),
		sojourn:      stats.NewHistogram("", "", loadshedBucketCutoffs),
		queueLen:     stats.NewHistogram("", "", lengthBucketCutoffs),
		droppableLen: stats.NewHistogram("", "", lengthBucketCutoffs),
		holderCount:  stats.NewHistogram("", "", holderBucketCutoffs),
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

// belowFloor reports whether the current holder count is below the configured
// minimum concurrency floor. A nil MinConcurrency means there is no floor, so
// every request counts as below it (idle gating disabled).
func (s *Snake) belowFloor() bool {
	if s.cfg.MinConcurrency == nil {
		return true
	}
	return len(s.holders) < s.cfg.MinConcurrency()
}

// idleGated reports whether a request that has capacity must nonetheless wait
// for an idle grant. This is true only when idle gating is enabled and the
// holder count is at or above the floor.
//
// OnGatedWaiter is required: without a notifier there is no idle granter to
// eventually grant a gated waiter, so honoring the gate would strand it (never
// granted, never notified). In that case the gate does not engage and requests
// are granted on capacity as usual.
func (s *Snake) idleGated() bool {
	return s.cfg.OnGatedWaiter != nil && s.cfg.IdleGatingEnabled != nil && s.cfg.IdleGatingEnabled() && !s.belowFloor()
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

	if s.acquireByPriority != nil {
		s.acquireByPriority.Add([]string{shedPriorityLabel(priority)}, 1)
	}

	s.mu.Lock()
	req := s.q.lockedEnqueue(valveID, priority)
	if valveID != "" {
		s.lockedObserveValveDepth(valveID)
	}

	if s.hasCapacity() && req.codelqElem != nil && !s.idleGated() {
		s.lockedGrant(req)
		s.lockedObserveLengths()
		s.mu.Unlock()
		return &SafeUnlock{s: s, req: req}, nil
	}

	// Enqueue-advance: a non-granted arrival drives the CoDel control law itself,
	// so shedding tracks load even when releases are sparse or the backstop timer
	// fires late. lockedRunTimer only MARKS drops; take the pending rejections and
	// send them after unlocking s.mu so the goready storm stays off the lock.
	pending := s.lockedEnqueueAdvance()
	s.lockedObserveLengths()
	s.lockedObserveDropping()

	// The request has capacity but is held back solely by the idle gate. Notify
	// the idle granter (outside the mutex) so it can grant on the next idle.
	notifyGated := s.hasCapacity() && req.codelqElem != nil && s.idleGated()

	s.mu.Unlock()
	for _, p := range pending {
		p.sendSignal()
	}

	if notifyGated {
		s.notifyGatedWaiter()
	}

	select {
	case val := <-req.signalChan:
		if val != grantSentinel {
			return nil, s.acquireError(req.priority)
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
	notifyGated := s.lockedTryGrantOne()
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	pending := s.q.lockedTakePendingSignals()
	s.mu.Unlock()

	// Deliver drop rejections after releasing s.mu so the goready storm does not
	// serialize grants/arrivals behind the batch.
	for _, r := range pending {
		r.sendSignal()
	}
	if notifyGated {
		s.notifyGatedWaiter()
	}
	s.runReleaseCBs(excValue)
	return nil
}

func (s *Snake) releaseOnCancel(req *Request) {
	s.mu.Lock()
	delete(s.holders, req)
	s.lockedObserveHolderCount()
	s.lockedCompleteAndShed(req)
	notifyGated := s.lockedTryGrantOne()
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	pending := s.q.lockedTakePendingSignals()
	s.mu.Unlock()
	for _, r := range pending {
		r.sendSignal()
	}
	if notifyGated {
		s.notifyGatedWaiter()
	}
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
		s.q.lockedRunTimer()
	}
}

// lockedEnqueueAdvance runs the CoDel control-law advance on every non-granted
// enqueue so an arrival can drive shedding, not just the release path and the
// backstop timer — the drop cadence then tracks load even when releases are
// sparse or the timer fires late. Must hold s.mu. lockedRunTimer only MARKS
// drops; the pending rejections are returned so the caller sends them AFTER
// releasing s.mu (draining the goready storm off the lock).
func (s *Snake) lockedEnqueueAdvance() []*Request {
	s.q.lockedRunTimer()
	s.interval.Add(s.q.lockedCurrentInterval())
	s.dropCount.Add(int64(s.q.lockedCount()))
	return s.q.lockedTakePendingSignals()
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

// lockedTryGrantOne grants the next waiter when capacity is available and the
// idle gate does not apply (gating disabled or below the floor). When a waiter
// exists but is held back solely by the idle gate, it grants nothing and
// returns true so the caller can notify the idle granter after releasing the
// mutex.
func (s *Snake) lockedTryGrantOne() (notifyGated bool) {
	if !s.hasCapacity() {
		return false
	}
	next := s.q.lockedFirstWaiting()
	if next == nil {
		return false
	}
	if s.idleGated() {
		return true
	}
	s.lockedGrant(next)
	return false
}

// TryGrantIdle grants a single waiting request if capacity is available. It is
// called by the idle granter when its SCHED_IDLE thread is scheduled, which is
// itself the proof that the CPU is idle. It reports whether a grant was made,
// so the granter can attempt to drain further waiters one reschedule at a time.
func (s *Snake) TryGrantIdle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasCapacity() {
		return false
	}
	next := s.q.lockedFirstWaiting()
	if next == nil {
		return false
	}
	s.lockedGrant(next)
	return true
}

// notifyGatedWaiter signals the idle granter, if configured, that a request is
// waiting solely on the idle gate. The callback must not block.
func (s *Snake) notifyGatedWaiter() {
	if s.cfg.OnGatedWaiter != nil {
		s.cfg.OnGatedWaiter()
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

func (s *Snake) acquireError(priority float64) error {
	s.shedCount.Add(1)
	if s.shedByPriority != nil {
		s.shedByPriority.Add([]string{shedPriorityLabel(priority)}, 1)
	}
	if s.cfg.AcquireError != nil {
		return s.cfg.AcquireError()
	}
	return &DroppedRequestError{}
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
	pending := s.q.lockedTakePendingSignals()
	s.mu.Unlock()
	// Deliver drop rejections after releasing s.mu so the goready storm does not
	// serialize grants/arrivals behind the batch.
	for _, r := range pending {
		r.sendSignal()
	}
}
