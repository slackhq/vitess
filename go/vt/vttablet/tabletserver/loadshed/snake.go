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
		ContentionID        func() string
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

		sq             *SelfContentionAwareCoDelQueue
		holder         *Request
		lockNonce      uint64
		dropTimer      *time.Timer
		dropTimerArmed bool
		maxAgeTimer    *time.Timer
		maxAgeHolder   *Request
		cfg            SnakeConfig
		clockFunc      func() int64
	}

	// SafeUnlock is a handle for releasing a Snake gate. Only the goroutine that
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
	return NewSnakeWithClock(cfg, defaultClock)
}

// NewSnakeWithClock creates a Snake with an injected clock (for testing).
func NewSnakeWithClock(cfg SnakeConfig, clockFunc func() int64) *Snake {
	s := &Snake{
		cfg:       cfg,
		clockFunc: clockFunc,
	}
	s.sq = newSelfContentionAwareCoDelQueue(cfg.CoDel, clockFunc)
	return s
}

// Acquire acquires the gate. It blocks until the gate is granted, the request
// is dropped by CoDel, or the context is cancelled. The returned SafeUnlock
// must be released via defer unlock.Release().
func (s *Snake) Acquire(ctx context.Context) (*SafeUnlock, error) {
	contentionID := ""
	if s.cfg.ContentionID != nil {
		contentionID = s.cfg.ContentionID()
	}

	priority := s.priority()

	s.mu.Lock()
	isLocked := s.sq.lockedPeek() != nil
	req, needSchedule, delay := s.sq.lockedEnqueue(contentionID, priority)

	if !isLocked {
		s.lockNonce++
		nonce := s.lockNonce
		s.holder = req
		s.sq.lockedMarkNotDroppable(req)
		s.lockedStartMaxAgeTimer(req)
		if needSchedule {
			s.lockedScheduleDropTimer(delay)
		}
		s.mu.Unlock()
		return &SafeUnlock{s: s, nonce: nonce}, nil
	}

	if needSchedule {
		s.lockedScheduleDropTimer(delay)
	}
	s.mu.Unlock()

	// wait for grant or drop
	select {
	case err := <-req.done:
		if err != nil {
			return nil, s.acquireError()
		}
		s.mu.Lock()
		nonce := s.lockNonce
		s.lockedStartMaxAgeTimer(req)
		s.mu.Unlock()
		return &SafeUnlock{s: s, nonce: nonce}, nil

	case <-ctx.Done():
		// Race: the request may have been granted AND the context cancelled
		// simultaneously. Go's select picks non-deterministically.
		select {
		case err := <-req.done:
			if err == nil {
				// Gate was granted but context cancelled. Release immediately
				// to avoid orphaning the gate.
				s.releaseInternal()
			}
			// if err != nil: was dropped, nothing to do
		default:
			// Not yet signaled. Cancel from queue.
			s.mu.Lock()
			s.sq.lockedCancel(contentionID, req)
			s.mu.Unlock()
		}
		return nil, ctx.Err()
	}
}

// IsLocked reports whether the gate is currently held.
func (s *Snake) IsLocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sq.lockedPeek() != nil
}

// IsHealthy reports whether the CoDel queue is healthy.
func (s *Snake) IsHealthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sq.lockedIsHealthy()
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
	s.mu.Unlock()

	// run release callbacks without mutex held
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

	s.mu.Lock()
	s.sq.lockedDequeue() // removes current holder, promotes from valve
	next := s.sq.lockedPeek()
	if next != nil {
		s.lockNonce++
		s.holder = next
		s.sq.lockedMarkNotDroppable(next)
	} else {
		s.holder = nil
	}
	s.mu.Unlock()

	if next != nil {
		next.signal(nil)
	}
	return nil
}

// releaseInternal releases the gate without nonce verification or callbacks.
// Used when the context is cancelled after the gate was already granted.
func (s *Snake) releaseInternal() {
	s.mu.Lock()
	s.lockedStopMaxAgeTimer()
	s.sq.lockedDequeue()
	next := s.sq.lockedPeek()
	if next != nil {
		s.lockNonce++
		s.holder = next
		s.sq.lockedMarkNotDroppable(next)
	} else {
		s.holder = nil
	}
	s.mu.Unlock()

	if next != nil {
		next.signal(nil)
	}
}

func (s *Snake) priority() *float64 {
	if s.cfg.LoadsheddingAllowed != nil && !s.cfg.LoadsheddingAllowed() {
		return PriorityUndroppable
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

func (s *Snake) runDropTimer() {
	s.mu.Lock()
	if !s.dropTimerArmed {
		s.mu.Unlock()
		return
	}
	s.dropTimerArmed = false

	reschedule, delayNs := s.sq.lockedRunScheduledDrop()
	if reschedule {
		s.lockedScheduleDropTimer(delayNs)
	}
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
	s.maxAgeHolder = holder
	s.maxAgeTimer = time.AfterFunc(maxAge, func() {
		s.mu.Lock()
		if s.maxAgeHolder != holder {
			s.mu.Unlock()
			return // stale timer
		}
		nonce := s.lockNonce
		s.mu.Unlock()

		log.Printf("loadshed: snake %s reached max age %v, force-releasing", s.cfg.Name, maxAge)
		s.release(nonce, nil)
	})
}

func (s *Snake) lockedStopMaxAgeTimer() {
	s.maxAgeHolder = nil
	if s.maxAgeTimer != nil {
		s.maxAgeTimer.Stop()
		s.maxAgeTimer = nil
	}
}
