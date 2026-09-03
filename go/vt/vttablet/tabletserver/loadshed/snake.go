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
	// Mode selects which load-shedding mechanism is active.
	Mode string

	// SnakeConfig configures a Snake. Functions are used to allow dynamic runtime
	// tuning.
	SnakeConfig struct {
		Name         string
		CoDel        CoDelConfig
		Capacity     func() int
		Mode         func() Mode
		AcquireError func() error
		ReleaseCBs   []func(error)
	}

	lockedTimer struct {
		timer      *time.Timer
		armed      bool
		generation uint64
	}

	// Snake is a CoDel-based load-shedding gate with dynamic capacity. Up to
	// Capacity() concurrent holders are allowed. Acquire requests are either
	// granted or dropped, each within a timely manner.
	Snake[T any] struct {
		mu sync.Mutex

		q         *ValvedCoDelQueue[T]
		holders   map[*Request[T]]struct{}
		dropTimer lockedTimer
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

		initialTargetShadow         initialTargetShadowTracker
		initialTargetShadowRequired *stats.Histogram
		initialTargetShadowCensored atomic.Int64
		initialTargetShadowTimer    lockedTimer

		droppingNanos   atomic.Int64
		droppingSinceNs int64
	}

	// SafeUnlock is a handle for releasing a slot. Only the goroutine that
	// acquired the slot should call Release. Release is idempotent.
	SafeUnlock[T any] struct {
		s    *Snake[T]
		req  *Request[T]
		once sync.Once
		err  error
	}
)

const (
	ModeOff     Mode = "off"
	ModeShadow  Mode = "shadow"
	ModeEnabled Mode = "enabled"
)

func (t *lockedTimer) arm(delay time.Duration, callback func(uint64)) {
	if t.armed {
		return
	}
	t.generation++
	generation := t.generation
	t.armed = true
	t.timer = time.AfterFunc(delay, func() { callback(generation) })
}

func (t *lockedTimer) stop() {
	if !t.armed {
		return
	}
	t.armed = false
	t.generation++
	t.timer.Stop()
}

func (t *lockedTimer) consume(generation uint64) bool {
	if !t.armed || generation != t.generation {
		return false
	}
	t.armed = false
	return true
}

var epoch = time.Now()

func defaultClock() int64 {
	return time.Since(epoch).Nanoseconds()
}

// NewSnake creates a new CoDel-based load-shedding gate.
func NewSnake[T any](cfg SnakeConfig) *Snake[T] {
	s := &Snake[T]{
		cfg:                         cfg,
		clockFunc:                   defaultClock,
		holders:                     make(map[*Request[T]]struct{}),
		sojourn:                     stats.NewHistogram("", "", loadshedBucketCutoffs),
		queueLen:                    stats.NewHistogram("", "", lengthBucketCutoffs),
		droppableLen:                stats.NewHistogram("", "", lengthBucketCutoffs),
		holderCount:                 stats.NewHistogram("", "", holderBucketCutoffs),
		interval:                    stats.NewHistogram("", "", intervalBucketCutoffs),
		dropCount:                   stats.NewHistogram("", "", lengthBucketCutoffs),
		timerLag:                    stats.NewHistogram("", "", loadshedBucketCutoffs),
		valveDepth:                  stats.NewHistogram("", "", lengthBucketCutoffs),
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

func (s *Snake[T]) capacity() int {
	if s.cfg.Capacity == nil {
		return 1
	}
	return max(s.cfg.Capacity(), 1)
}

func (s *Snake[T]) hasCapacity() bool {
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
func (s *Snake[T]) Acquire(ctx context.Context, valveID string, priority float64) (*SafeUnlock[T], error) {
	priority = s.priority(priority)

	if s.acquireByPriority != nil {
		s.acquireByPriority.Add([]string{shedPriorityLabel(priority)}, 1)
	}

	s.mu.Lock()
	req := s.q.lockedEnqueue(valveID, priority)
	if valveID != "" {
		s.lockedObserveValveDepth(valveID)
	}

	if s.hasCapacity() && req.codelqElem != nil {
		s.lockedGrant(req)
		s.lockedObserveLengths()
		s.mu.Unlock()
		return &SafeUnlock[T]{s: s, req: req}, nil
	}

	s.lockedObserveInitialTargetShadow(nil)
	s.lockedStartInitialTargetShadow(req)

	// Enqueue-advance: a non-granted arrival drives the CoDel control law itself,
	// so shedding tracks load even when releases are sparse or the backstop timer
	// fires late. lockedRunTimer only MARKS drops; take the pending rejections and
	// send them after unlocking s.mu so the goready storm stays off the lock.
	pending := s.lockedEnqueueAdvance()
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	s.mu.Unlock()
	for _, p := range pending {
		p.sendSignal()
	}

	select {
	case val := <-req.signalChan:
		if val != grantSentinel {
			return nil, s.acquireError(req.priority)
		}
		return &SafeUnlock[T]{s: s, req: req}, nil

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
				s.lockedObserveInitialTargetShadow(nil)
				s.lockedObserveLengths()
				s.lockedObserveDropping()
				s.mu.Unlock()
			}
		}
		return nil, ctx.Err()
	}
}

// IsHealthy reports whether the CoDel queue is healthy.
func (s *Snake[T]) IsHealthy() bool {
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
func (s *Snake[T]) Stats() SnakeStats {
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
func (u *SafeUnlock[T]) Release(exc ...error) error {
	u.once.Do(func() {
		var excValue error
		if len(exc) > 0 {
			excValue = exc[0]
		}
		u.err = u.s.release(u.req, excValue)
	})
	return u.err
}

func (s *Snake[T]) release(req *Request[T], excValue error) error {
	s.mu.Lock()
	// Release is idempotent and can race a context-cancel release for the same
	// req. The loser sees the req already gone from holders and no-ops.
	if _, ok := s.holders[req]; !ok {
		s.mu.Unlock()
		return nil
	}
	delete(s.holders, req)
	s.lockedObserveHolderCount()
	s.lockedReleaseAndShed(req)
	s.lockedTryGrantOne()
	s.lockedObserveInitialTargetShadow(nil)
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	pending := s.q.lockedTakePendingSignals()
	s.mu.Unlock()

	// Deliver drop rejections after releasing s.mu so the goready storm does not
	// serialize grants/arrivals behind the batch.
	for _, r := range pending {
		r.sendSignal()
	}
	s.runReleaseCBs(excValue)
	return nil
}

func (s *Snake[T]) releaseOnCancel(req *Request[T]) {
	s.mu.Lock()
	delete(s.holders, req)
	s.lockedObserveHolderCount()
	s.lockedReleaseAndShed(req)
	s.lockedTryGrantOne()
	s.lockedObserveInitialTargetShadow(nil)
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	pending := s.q.lockedTakePendingSignals()
	s.mu.Unlock()
	for _, r := range pending {
		r.sendSignal()
	}
}

// lockedReleaseAndShed releases the request and advances CoDel before granting
// the next waiter when there is active shedding work.
func (s *Snake[T]) lockedReleaseAndShed(req *Request[T]) {
	s.q.lockedRelease(req)
	if s.q.lockedNeedsAdvance() {
		s.lockedRunCoDelTimer()
	}
}

// lockedEnqueueAdvance runs the CoDel control-law advance on every non-granted
// enqueue so an arrival can drive shedding, not just the release path and the
// backstop timer — the drop cadence then tracks load even when releases are
// sparse or the timer fires late. Must hold s.mu. lockedRunTimer only MARKS
// drops; the pending rejections are returned so the caller sends them AFTER
// releasing s.mu (draining the goready storm off the lock).
func (s *Snake[T]) lockedEnqueueAdvance() []*Request[T] {
	if s.lockedRunCoDelTimer() {
		s.interval.Add(s.q.lockedCurrentInterval())
		s.dropCount.Add(int64(s.q.lockedCount()))
	}
	return s.q.lockedTakePendingSignals()
}

func (s *Snake[T]) lockedGrant(req *Request[T]) {
	s.holders[req] = struct{}{}
	s.lockedObserveHolderCount()
	now := s.clockFunc()
	sojournNs := now - req.codelqEnqueuedAtNs
	s.q.lockedOnGrant(req)
	s.lockedObserveInitialTargetShadowAt(now, &sojournNs)
	s.lockedAccrueDropping(now)
	s.sojourn.Add(sojournNs)
	req.signal(grantSentinel)
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
	nowNs := s.clockFunc()
	if s.initialTargetShadow.start(startedAtNs) {
		s.lockedScheduleInitialTargetShadowTimer(
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
	}
	if outcome.completed {
		s.lockedStopInitialTargetShadowTimer()
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
	s.lockedStopInitialTargetShadowTimer()
}

func (s *Snake[T]) lockedRunCoDelTimer() bool {
	mode := s.mode()
	if s.initialTargetShadow.active || s.initialTargetShadow.waitingForDrain {
		if mode != ModeShadow {
			s.lockedLeaveInitialTargetShadow(s.clockFunc())
		} else {
			s.lockedObserveInitialTargetShadowAt(s.clockFunc(), nil)
		}
	}
	enabled := mode == ModeEnabled
	s.q.lockedRunTimerIf(enabled)
	return enabled
}

// RefreshMode applies a runtime mode change to existing queue state.
func (s *Snake[T]) RefreshMode() {
	s.mu.Lock()
	defer s.mu.Unlock()

	mode := s.mode()
	if mode != ModeShadow {
		s.lockedLeaveInitialTargetShadow(s.clockFunc())
	}
	s.q.lockedRefreshMode(mode == ModeEnabled)
	s.lockedObserveDropping()
}

func (s *Snake[T]) lockedObserveHolderCount() {
	s.holderCount.Add(int64(len(s.holders)))
}

func (s *Snake[T]) lockedObserveDropping() {
	dropping := !s.q.lockedIsHealthy()
	if dropping == (s.droppingSinceNs != 0) {
		return
	}
	s.lockedAccrueDropping(s.clockFunc())
}

func (s *Snake[T]) lockedAccrueDropping(now int64) {
	dropping := !s.q.lockedIsHealthy()
	switch {
	case dropping && s.droppingSinceNs == 0:
		s.droppingSinceNs = now
	case !dropping && s.droppingSinceNs != 0:
		s.droppingNanos.Add(now - s.droppingSinceNs)
		s.droppingSinceNs = 0
	}
}

func (s *Snake[T]) lockedTryGrantOne() {
	if !s.hasCapacity() {
		return
	}
	next := s.q.lockedFirstWaiting()
	if next != nil {
		s.lockedGrant(next)
	}
}

// runReleaseCBs executes release callbacks outside the mutex.
func (s *Snake[T]) runReleaseCBs(excValue error) {
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

func (s *Snake[T]) priority(priority float64) float64 {
	return priority
}

func (s *Snake[T]) mode() Mode {
	if s.cfg.Mode == nil {
		return ModeEnabled
	}
	return s.cfg.Mode()
}

func (s *Snake[T]) acquireError(priority float64) error {
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
func (s *Snake[T]) ShedCount() int64 {
	return s.shedCount.Load()
}

func (s *Snake[T]) DroppingNanos() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := s.droppingNanos.Load()
	if s.droppingSinceNs != 0 {
		total += s.clockFunc() - s.droppingSinceNs
	}
	return total
}

// --- timer management (must be called with s.mu held) ---

func (s *Snake[T]) lockedScheduleDropTimer(delayNs int64) {
	if s.dropTimer.armed {
		return
	}
	s.dropTimerExpectedNs = s.clockFunc() + delayNs
	s.dropTimer.arm(time.Duration(delayNs)*time.Nanosecond, s.runDropTimer)
}

func (s *Snake[T]) lockedStopDropTimer() {
	s.dropTimer.stop()
}

func (s *Snake[T]) lockedScheduleInitialTargetShadowTimer(delayNs int64) {
	s.initialTargetShadowTimer.arm(time.Duration(delayNs), s.runInitialTargetShadowTimer)
}

func (s *Snake[T]) lockedStopInitialTargetShadowTimer() {
	s.initialTargetShadowTimer.stop()
}

func (s *Snake[T]) runInitialTargetShadowTimer(generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.initialTargetShadowTimer.consume(generation) {
		return
	}
	s.lockedObserveInitialTargetShadow(nil)
}

func (s *Snake[T]) runDropTimer(generation uint64) {
	s.mu.Lock()
	if !s.dropTimer.consume(generation) {
		s.mu.Unlock()
		return
	}
	if s.lockedRunCoDelTimer() {
		// Record how late this fire is versus when it was scheduled. Under CPU
		// contention the normal-priority timer goroutine can fire well past its
		// deadline, which delays shedding; this surfaces that lag.
		if lag := s.clockFunc() - s.dropTimerExpectedNs; lag > 0 {
			s.timerLag.Add(lag)
		} else {
			s.timerLag.Add(0)
		}
		s.interval.Add(s.q.lockedCurrentInterval())
		s.dropCount.Add(int64(s.q.lockedCount()))
	}
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
