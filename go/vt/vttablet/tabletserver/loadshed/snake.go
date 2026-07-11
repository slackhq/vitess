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
	"runtime"
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

		// PerCPUIntake gates routing contended arrivals through per-CPU staging
		// shards (off s.mu) that are batch-merged into the CoDel queue, to reduce
		// mutex contention. When non-nil the shards are always allocated, but they
		// are only used while the func returns true — so the path is live-toggleable
		// (e.g. via /debug/env) with no thread/alloc churn. Nil or returning false ⇒
		// the healthy fast path is unchanged.
		PerCPUIntake func() bool

		// YieldOnGrant, when it returns true, makes a releasing goroutine call
		// runtime.Gosched() right after it grants the next waiter (and drops
		// s.mu), handing the P to the just-woken grantee — which is already in the
		// scheduler's runnext slot from the grant signal — before the releaser
		// returns up the stack to write its gRPC response. This favors starting
		// the next request over finishing the response write of the one that just
		// released. Only helps under contention; nil/false ⇒ no yield.
		YieldOnGrant func() bool
	}

	// Snake is a CoDel-based load-shedding gate with dynamic capacity. Up to
	// Capacity() concurrent holders are allowed. Acquire requests are either
	// granted or dropped, each within a timely manner.
	//
	// Granted requests stay in the CoDel queue as undroppable until Release,
	// preserving the queue's system-pressure signal for accurate shedding.
	Snake struct {
		mu sync.Mutex

		// intake stages arriving requests in per-CPU shards, off s.mu, when the
		// gate is contended. A merge (under s.mu) admits them into q in arrival
		// order. Allocated when SnakeConfig.PerCPUIntake is non-nil; whether it is
		// actually used is gated per-request on intakeEnabled().
		intake *intake

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
		// underfillCount increments each time a release frees a slot but there is
		// no waiter to grant it to, so the slot goes idle (the queue underfilled
		// the semaphore). A high rate suggests the gate is shedding/draining too
		// aggressively and starving the downstream pool of work.
		underfillCount atomic.Int64

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

		// slotIdleNanos is the time-integral of unfilled capacity: sum over time
		// of (capacity - holders) * elapsed. It measures backend concurrency left
		// idle by the gate — zero when saturated, growing whenever a slot sits
		// free. It is the signal for "is the gate keeping the backend busy": a
		// grant→run optimization (e.g. YieldOnGrant) that fills freed slots sooner
		// should reduce it. slotAccrualNs is the clock of the last accrual.
		slotIdleNanos int64
		slotAccrualNs int64
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
	if cfg.PerCPUIntake != nil {
		s.intake = newIntake()
	}
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

	req, granted := s.admit(valveID, priority)
	if granted {
		return &SafeUnlock{s: s, req: req}, nil
	}

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
		//     granted, release; otherwise cancel it.
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
				// Signal the request cancelled. If it is still staged in the
				// intake (codelqElem == nil and not yet merged), marking it
				// signaled makes the merge skip it — no shard scan needed. If it
				// is already in the CoDel queue, remove it there.
				if req.codelqElem != nil {
					s.q.lockedCancel(req)
				} else if req.signaledValue == nil {
					req.signal(&DroppedRequestError{})
				}
				s.lockedObserveLengths()
				s.lockedObserveDropping()
				s.mu.Unlock()
			}
		}
		return nil, ctx.Err()
	}
}

// admit places an arriving request into the gate and reports whether it was
// granted inline (fast path). When not granted, the returned request is either
// enqueued in the CoDel queue or staged in the per-CPU intake, and the caller
// waits on req.signalChan.
//
// Fast path (intake disabled, or intake empty and capacity available): take
// s.mu, enqueue, and grant inline — unchanged ~180ns behavior. Slow path
// (intake enabled and contended): stamp the arrival time and stage into the
// per-CPU intake off s.mu, triggering a merge if the CoDel queue currently has
// no waiter (so an idle→first-arrival still advances).
func (s *Snake) admit(valveID string, priority float64) (req *Request, granted bool) {
	if !s.intakeEnabled() {
		return s.admitLocked(valveID, priority)
	}

	// Intake enabled. Try the inline fast path only when there is no staged
	// backlog — otherwise staging keeps global arrival order intact.
	if s.intake.pendingLen() == 0 {
		s.mu.Lock()
		if s.intake.pendingLen() == 0 && s.hasCapacity() {
			r := s.q.lockedEnqueue(valveID, priority)
			if valveID != "" {
				s.lockedObserveValveDepth(valveID)
			}
			if r.codelqElem != nil {
				s.lockedGrant(r)
				s.lockedObserveLengths()
				s.mu.Unlock()
				return r, true
			}
			// Parked behind a same-valve droppable entry: leave it enqueued and
			// wait, same as the non-intake slow path.
			s.lockedObserveLengths()
			s.lockedObserveDropping()
			s.mu.Unlock()
			return r, false
		}
		s.mu.Unlock()
	}

	// Contended: stage into the per-CPU intake, off s.mu.
	r := newRequest(priority)
	r.valveID = valveID
	r.codelqEnqueuedAtNs = s.clockFunc()
	s.intake.push(r)

	// If the CoDel queue has no waiter to serve, nothing will trigger a merge
	// via release — merge now so this arrival becomes eligible.
	s.mu.Lock()
	if s.q.lockedFirstWaiting() == nil {
		s.lockedMerge()
	}
	s.mu.Unlock()
	return r, false
}

// admitLocked is the original enqueue+maybe-grant path, used when intake is
// disabled.
func (s *Snake) admitLocked(valveID string, priority float64) (req *Request, granted bool) {
	s.mu.Lock()
	r := s.q.lockedEnqueue(valveID, priority)
	if valveID != "" {
		s.lockedObserveValveDepth(valveID)
	}
	if s.hasCapacity() && r.codelqElem != nil {
		s.lockedGrant(r)
		s.lockedObserveLengths()
		s.mu.Unlock()
		return r, true
	}
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	s.mu.Unlock()
	return r, false
}

// intakeEnabled reports whether new arrivals should be routed through the
// per-CPU intake. The shards must be allocated (PerCPUIntake non-nil at
// construction) and the runtime toggle must currently return true. Draining
// already-staged requests on release is NOT gated on this — see
// lockedFinishRelease — so toggling off never strands staged arrivals.
func (s *Snake) intakeEnabled() bool {
	return s.intake != nil && s.cfg.PerCPUIntake()
}

// lockedMerge drains the per-CPU intake and admits the staged requests into the
// CoDel queue in arrival-time order. Used by the (cold) stage-into-empty path in
// admit, where the drain runs under s.mu. The hot release path instead drains
// off s.mu and calls lockedAdmitStaged directly. Must hold s.mu.
func (s *Snake) lockedMerge() {
	if s.intake == nil {
		return
	}
	s.lockedAdmitStaged(s.intake.drain())
}

// lockedAdmitStaged admits already-drained staged requests into the CoDel queue
// in arrival order, preserving each request's original sojourn. Already-signaled
// (cancelled) staged requests are skipped — their valve accounting was never
// applied. Must hold s.mu. The drain (shard sweep + sort) is the caller's job,
// so it can run off s.mu; only this cheap admit loop needs the lock.
func (s *Snake) lockedAdmitStaged(staged []*Request) {
	for _, r := range staged {
		if r.signaledValue != nil {
			continue
		}
		s.q.lockedMergeExisting(r)
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
	UnderfillCount  int64
	SlotIdleNanos   int64
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
		UnderfillCount:  s.underfillCount.Load(),
		SlotIdleNanos:   s.lockedSlotIdleNanos(),
	}
}

// lockedSlotIdleNanos returns slotIdleNanos including the currently-open
// interval. Must hold s.mu.
func (s *Snake) lockedSlotIdleNanos() int64 {
	total := s.slotIdleNanos
	if s.slotAccrualNs != 0 {
		if idle := s.capacity() - len(s.holders); idle > 0 {
			total += int64(idle) * (s.clockFunc() - s.slotAccrualNs)
		}
	}
	return total
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
	s.lockedAccrueSlotIdle(s.clockFunc())
	delete(s.holders, req)
	s.lockedObserveHolderCount()
	s.lockedCompleteAndShed(req)
	granted := s.lockedFinishRelease()

	s.maybeYieldOnGrant(granted)
	s.runReleaseCBs(excValue)
	return nil
}

func (s *Snake) releaseOnCancel(req *Request) {
	s.mu.Lock()
	if _, ok := s.holders[req]; ok {
		s.lockedAccrueSlotIdle(s.clockFunc())
	}
	delete(s.holders, req)
	s.lockedObserveHolderCount()
	s.lockedCompleteAndShed(req)
	granted := s.lockedFinishRelease()
	s.maybeYieldOnGrant(granted)
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

// lockedFinishRelease completes the release tail while holding s.mu: it folds
// any staged intake arrivals into the CoDel queue when no waiter remains, grants
// the next waiter, records observations, and unlocks. Must hold s.mu on entry;
// s.mu is released on return.
//
// The merge is gated on there being no CoDel waiter to grant — otherwise we
// serve the existing FIFO head and defer the (batchable) merge until the queue
// actually drains, keeping merges rare instead of running on every release. When
// a merge is due, the drain (shard sweep + O(n log n) sort) runs OFF s.mu: we
// drop the lock, drain, then re-take it only for the cheap admit loop. The
// drained requests are ours exclusively (drain removed them from the shards), so
// admitting them after re-locking cannot strand or double-process them even if
// another releaser interleaves.
// lockedFinishRelease returns whether it granted the next waiter, so the caller
// can optionally yield the P to the just-woken grantee (see YieldOnGrant).
func (s *Snake) lockedFinishRelease() (granted bool) {
	if s.intake != nil && s.intake.pendingLen() > 0 && s.q.lockedFirstWaiting() == nil {
		s.mu.Unlock()
		staged := s.intake.drain()
		s.mu.Lock()
		s.lockedAdmitStaged(staged)
	}
	granted = s.lockedTryGrantOne()
	s.lockedObserveLengths()
	s.lockedObserveDropping()
	s.mu.Unlock()
	return granted
}

// maybeYieldOnGrant hands this P to the just-granted waiter (already in the
// scheduler's runnext slot from the grant signal) when the release granted a
// waiter and YieldOnGrant is enabled, so the next request starts running before
// this goroutine returns up the stack to write its response. Must be called
// after s.mu is released.
func (s *Snake) maybeYieldOnGrant(granted bool) {
	if granted && s.cfg.YieldOnGrant != nil && s.cfg.YieldOnGrant() {
		runtime.Gosched()
	}
}

func (s *Snake) lockedGrant(req *Request) {
	now := s.clockFunc()
	// Accrue idle-slot time for the interval that just ended before this grant
	// fills a slot (uses the pre-grant holder count).
	s.lockedAccrueSlotIdle(now)
	s.holders[req] = struct{}{}
	s.lockedObserveHolderCount()
	s.q.lockedOnGrant(req)
	s.lockedAccrueDropping(now)
	s.sojourn.Add(now - req.codelqEnqueuedAtNs)
	req.signal(grantSentinel)
}

func (s *Snake) lockedObserveHolderCount() {
	s.holderCount.Add(int64(len(s.holders)))
}

// lockedAccrueSlotIdle folds the elapsed time since the last accrual into
// slotIdleNanos, weighted by the capacity that was unfilled over that span
// (capacity - current holders). Call it with the holder set as it was for the
// elapsed interval, i.e. BEFORE adding/removing a holder. Must hold s.mu.
func (s *Snake) lockedAccrueSlotIdle(now int64) {
	if s.slotAccrualNs != 0 {
		if idle := s.capacity() - len(s.holders); idle > 0 {
			s.slotIdleNanos += int64(idle) * (now - s.slotAccrualNs)
		}
	}
	s.slotAccrualNs = now
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

// lockedTryGrantOne grants the next waiter if there is capacity and one is
// waiting; it reports whether a grant was issued.
func (s *Snake) lockedTryGrantOne() (granted bool) {
	if !s.hasCapacity() {
		return false
	}
	next := s.q.lockedFirstWaiting()
	if next != nil {
		s.lockedGrant(next)
		return true
	}
	// A slot is free but no request is waiting to take it: the slot goes idle.
	// Count this underfill so the load test can correlate idle downstream
	// capacity with the gate draining faster than arrivals refill it.
	s.underfillCount.Add(1)
	return false
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

// UnderfillCount returns the cumulative number of times a released slot found
// no waiter to grant and went idle.
func (s *Snake) UnderfillCount() int64 {
	return s.underfillCount.Load()
}

// SlotIdleNanos returns the cumulative time-integral of unfilled capacity
// (sum of (capacity-holders)*elapsed) — backend concurrency the gate left idle.
// Includes the currently-open interval up to now.
func (s *Snake) SlotIdleNanos() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lockedSlotIdleNanos()
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

func (s *Snake) lockedScheduleDropTimer(delayNs int64) {
	if s.dropTimerArmed {
		return
	}
	s.dropTimerArmed = true
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
