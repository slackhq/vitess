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
	"errors"
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
		Capacity            func() int
		MaxAge              func() time.Duration
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

		q              *SelfContentionAwareCoDelQueue
		inFlight       int
		holders        map[*Request]struct{}
		maxAgeTimers   map[*Request]*time.Timer
		dropTimer      *time.Timer
		dropTimerArmed bool
		cfg            SnakeConfig
		clockFunc      func() int64
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
		maxAgeTimers: make(map[*Request]*time.Timer),
	}
	s.q = newSelfContentionAwareCoDelQueue(cfg.CoDel, defaultClock, s.lockedScheduleDropTimer)
	return s
}

func (s *Snake) capacity() int {
	if s.cfg.Capacity == nil {
		return 1
	}
	c := s.cfg.Capacity()
	if c < 1 {
		return 1
	}
	return c
}

// Acquire acquires a slot. It blocks until a slot is granted, the request
// is dropped by CoDel, or the context is cancelled. The returned SafeUnlock
// must be released via defer unlock.Release(). valveID controls self-contention
// awareness: requests with the same non-empty ID are serialized through the
// valve so at most one is in the CoDel queue at a time. Pass "" to bypass.
func (s *Snake) Acquire(ctx context.Context, valveID string) (*SafeUnlock, error) {
	priority := s.priority()

	s.mu.Lock()
	req := s.q.lockedEnqueue(valveID, priority)

	if s.inFlight < s.capacity() {
		s.lockedGrant(req)
		s.mu.Unlock()
		return &SafeUnlock{s: s, req: req}, nil
	}

	s.mu.Unlock()

	select {
	case err := <-req.result:
		if err != nil {
			return nil, s.acquireError()
		}
		return &SafeUnlock{s: s, req: req}, nil

	case <-ctx.Done():
		select {
		case err := <-req.result:
			if err == nil {
				s.releaseInternal(req)
			}
		default:
			s.mu.Lock()
			if _, granted := s.holders[req]; granted {
				s.mu.Unlock()
				s.releaseInternal(req)
			} else {
				s.q.lockedCancel(req)
				s.mu.Unlock()
			}
		}
		return nil, ctx.Err()
	}
}

// IsLocked reports whether any slot is currently held.
func (s *Snake) IsLocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inFlight > 0
}

// InFlight reports the number of currently held slots.
func (s *Snake) InFlight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inFlight
}

// IsHealthy reports whether the CoDel queue is healthy.
func (s *Snake) IsHealthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.q.lockedIsHealthy()
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
	if _, ok := s.holders[req]; !ok {
		s.mu.Unlock()
		return errors.New("unauthorized release: request not in holders set")
	}
	delete(s.holders, req)
	s.lockedStopMaxAgeTimer(req)
	s.q.lockedComplete(req)
	s.inFlight--
	s.lockedTryGrantNext()
	s.mu.Unlock()

	s.runReleaseCBs(excValue)
	return nil
}

// releaseInternal releases a slot without holder verification or callbacks.
// Used when the context is cancelled after the slot was already granted.
func (s *Snake) releaseInternal(req *Request) {
	s.mu.Lock()
	delete(s.holders, req)
	s.lockedStopMaxAgeTimer(req)
	s.q.lockedComplete(req)
	s.inFlight--
	s.lockedTryGrantNext()
	s.mu.Unlock()
}

func (s *Snake) lockedGrant(req *Request) {
	s.inFlight++
	s.holders[req] = struct{}{}
	s.q.lockedMarkNotDroppable(req)
	s.lockedStartMaxAgeTimer(req)
	if !req.isDone() {
		req.signal(nil)
	}
}

func (s *Snake) lockedTryGrantNext() {
	for s.inFlight < s.capacity() {
		// Promote from valves before looking for the next waiter — this
		// ensures valve entries enter the CoDel queue only when capacity
		// is available (grant-time promotion).
		s.q.lockedPromoteAllValves()
		next := s.q.lockedFindFirstWaiting()
		if next == nil {
			break
		}
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

func (s *Snake) runDropTimer() {
	s.mu.Lock()
	if !s.dropTimerArmed {
		s.mu.Unlock()
		return
	}
	s.dropTimerArmed = false
	s.q.lockedRunScheduledDrop()
	s.lockedTryGrantNext()
	s.mu.Unlock()
}

func (s *Snake) lockedStartMaxAgeTimer(req *Request) {
	if s.cfg.MaxAge == nil {
		return
	}
	maxAge := s.cfg.MaxAge()
	if maxAge <= 0 {
		return
	}
	s.maxAgeTimers[req] = time.AfterFunc(maxAge, func() {
		s.mu.Lock()
		if _, ok := s.holders[req]; !ok {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		log.Printf("loadshed: snake %s slot reached max age %v, force-releasing", s.cfg.Name, maxAge)
		s.release(req, nil)
	})
}

func (s *Snake) lockedStopMaxAgeTimer(req *Request) {
	if timer, ok := s.maxAgeTimers[req]; ok {
		timer.Stop()
		delete(s.maxAgeTimers, req)
	}
}
