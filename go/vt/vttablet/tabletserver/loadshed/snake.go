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
	"fmt"
	"log"
	"sync"
	"time"
)

type (
	// SnakeConfig configures a Snake. Functions are used to allow dynamic runtime
	// tuning.
	SnakeConfig struct {
		Name                string
		CoDel               CoDelConfig
		MaxAge              func() time.Duration
		LoadsheddingAllowed func() bool
		AcquireError        func() error
		ReleaseCBs          []func(error)
	}

	// Snake is a CoDel-based load-shedding gate. Acquire requests are either
	// granted or dropped, each within a timely manner.
	//
	// Callers must use defer unlock.Release() to ensure the gate is always
	// released.
	Snake struct {
		mu sync.Mutex

		q              *SelfContentionAwareCoDelQueue
		holder         *Request
		lockNonce      uint64
		dropTimer      *time.Timer
		dropTimerArmed bool
		maxAgeTimer    *time.Timer
		cfg            SnakeConfig
		clockFunc      func() int64
	}

	// SafeUnlock is a handle for releasing a gate. Only the goroutine that
	// acquired the gate should call Release. Release is idempotent.
	SafeUnlock struct {
		s     *Snake
		nonce uint64
		once  sync.Once
		err   error
	}
)

var epoch = time.Now()

func defaultClock() int64 {
	return time.Since(epoch).Nanoseconds()
}

// NewSnake creates a new CoDel-based load-shedding gate.
func NewSnake(cfg SnakeConfig) *Snake {
	s := &Snake{
		cfg:       cfg,
		clockFunc: defaultClock,
	}
	s.q = newSelfContentionAwareCoDelQueue(cfg.CoDel, defaultClock, s.lockedScheduleDropTimer, s.lockedStopDropTimer)
	return s
}

// Acquire acquires the gate. It blocks until the gate is granted, the request
// is dropped by CoDel, or the context is cancelled. The returned SafeUnlock
// must be released via defer unlock.Release(). valveID controls self-contention
// awareness: requests with the same non-empty ID are serialized through the
// valve so at most one is in the CoDel queue at a time. Pass "" to bypass.
func (s *Snake) Acquire(ctx context.Context, valveID string) (*SafeUnlock, error) {
	priority := s.priority()

	s.mu.Lock()
	// The holder stays at the head of the queue rather than being removed on
	// grant. This preserves CoDel's system-pressure signal: queue length
	// reflects total system load (waiting + executing), not just waiting load.
	// Without this, a slow holder would leave the queue empty, hiding
	// backpressure and preventing CoDel from entering the dropping state.
	// Additionally, aggressive dropping can reduce droppableLen to 0 while the
	// queue is still unhealthy; the holder's presence keeps the queue non-empty
	// so we don't falsely transition to healthy and lose accumulated drop
	// intensity (count), which would force rediscovery from scratch.
	isLocked := s.q.lockedPeek() != nil
	req := s.q.lockedEnqueue(valveID, priority)

	if !isLocked {
		s.lockNonce++
		nonce := s.lockNonce
		s.holder = req
		s.q.lockedMarkNotDroppable(req)
		s.lockedStartMaxAgeTimer(req)
		s.mu.Unlock()
		return &SafeUnlock{s: s, nonce: nonce}, nil
	}

	s.mu.Unlock()

	select {
	case val := <-req.signalChan:
		if val != grantSentinel {
			return nil, s.acquireError()
		}
		s.mu.Lock()
		nonce := s.lockNonce
		s.lockedStartMaxAgeTimer(req)
		s.mu.Unlock()
		return &SafeUnlock{s: s, nonce: nonce}, nil

	case <-ctx.Done():
		// Race: the context may cancel after the grant was already sent to
		// signalChan (or is in-flight via lockedGrantNext). The inner select
		// resolves this: if the signal is already buffered, consume it and
		// release. If not, check under the mutex whether we were granted
		// (holder == req) and wait for the in-flight signal before releasing.
		// Without this double-select, we'd either leak a held lock or cancel
		// a request that was already granted.
		select {
		case val := <-req.signalChan:
			if val == grantSentinel {
				s.releaseInternal()
			}
		default:
			s.mu.Lock()
			if s.holder == req {
				s.mu.Unlock()
				<-req.signalChan
				s.releaseInternal()
			} else {
				s.q.lockedCancel(req)
				s.mu.Unlock()
			}
		}
		return nil, ctx.Err()
	}
}

// IsLocked reports whether the gate is currently held.
func (s *Snake) IsLocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.q.lockedPeek() != nil
}

// IsHealthy reports whether the CoDel queue is healthy.
func (s *Snake) IsHealthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.q.lockedIsHealthy()
}

// Release releases the gate. exc is an optional error that caused the release
// (passed to release callbacks). Release is idempotent.
func (u *SafeUnlock) Release(exc ...error) error {
	u.once.Do(func() {
		var excValue error
		if len(exc) > 0 {
			excValue = exc[0]
		}
		u.err = u.s.release(u.nonce, excValue)
	})
	return u.err
}

func (s *Snake) release(nonce uint64, excValue error) error {
	s.mu.Lock()
	if nonce != s.lockNonce {
		currentNonce := s.lockNonce
		s.mu.Unlock()
		return fmt.Errorf("unauthorized release: nonce %d != %d", nonce, currentNonce)
	}
	s.lockedStopMaxAgeTimer()
	next := s.lockedGrantNext()
	if next != nil {
		next.signal(grantSentinel)
	}
	s.mu.Unlock()

	s.runReleaseCBs(excValue)
	return nil
}

// releaseInternal releases the gate without nonce verification or callbacks.
// Used when the context is cancelled after the gate was already granted.
func (s *Snake) releaseInternal() {
	s.mu.Lock()
	s.lockedStopMaxAgeTimer()
	next := s.lockedGrantNext()
	if next != nil {
		next.signal(grantSentinel)
	}
	s.mu.Unlock()
}

// lockedGrantNext dequeues the current holder, promotes from valve, and grants
// to the next waiter. Returns the next request to signal, or nil.
func (s *Snake) lockedGrantNext() *Request {
	s.q.lockedDequeue()
	next := s.q.lockedPeek()
	if next != nil {
		s.lockNonce++
		s.holder = next
		s.q.lockedMarkNotDroppable(next)
	} else {
		s.holder = nil
	}
	return next
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

func (s *Snake) priority() *float64 {
	if s.cfg.LoadsheddingAllowed != nil && !s.cfg.LoadsheddingAllowed() {
		return newUndroppablePriority()
	}
	return NewPriority(0)
}

func (s *Snake) acquireError() error {
	if s.cfg.AcquireError != nil {
		return s.cfg.AcquireError()
	}
	return &DroppedRequestError{}
}

// --- timer management (must be called with s.mu held) ---

func (s *Snake) lockedScheduleDropTimer(delayNs int64) {
	if s.dropTimerArmed {
		return
	}
	s.dropTimerArmed = true
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
	s.q.lockedRunScheduledDrop()
	s.mu.Unlock()
}

func (s *Snake) lockedStartMaxAgeTimer(holder *Request) {
	if s.cfg.MaxAge == nil {
		return
	}
	maxAge := s.cfg.MaxAge()
	if maxAge <= 0 {
		return
	}
	s.maxAgeTimer = time.AfterFunc(maxAge, func() {
		s.mu.Lock()
		if s.holder != holder {
			s.mu.Unlock()
			return
		}
		nonce := s.lockNonce
		s.mu.Unlock()

		log.Printf("loadshed: snake %s reached max age %v, force-releasing", s.cfg.Name, maxAge)
		s.release(nonce, nil)
	})
}

func (s *Snake) lockedStopMaxAgeTimer() {
	if s.maxAgeTimer != nil {
		s.maxAgeTimer.Stop()
		s.maxAgeTimer = nil
	}
}
