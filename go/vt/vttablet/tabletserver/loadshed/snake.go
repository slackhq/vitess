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
	// SnakeConfig configures a Snake. Functions are used to allow
	// dynamic runtime tuning.
	SnakeConfig struct {
		Name                string
		CoDel               CoDelConfig
		Capacity            func() int
		MaxAge              func() time.Duration
		LoadsheddingAllowed func() bool
		ContentionID        func() string
		AcquireError        func() error
		ReleaseCBs          []func(error)
	}

	// Snake is a CoDel-based load-shedding gate. Up to Capacity()
	// concurrent holders are allowed. Acquire requests are either granted or
	// dropped, each within a timely manner.
	//
	// Granted requests stay in the CoDel queue as undroppable until Release
	// is called, preserving the queue's system-pressure signal.
	Snake struct {
		mu sync.Mutex

		sq             *SelfContentionAwareCoDelQueue
		inFlight       int
		holders        map[*Request]struct{}
		maxAgeTimers   map[*Request]*time.Timer
		dropTimer      *time.Timer
		dropTimerArmed bool
		cfg            SnakeConfig
		clockFunc      func() int64
	}

	// SafeUnlock is a handle for releasing a slot. Only the
	// goroutine that acquired the slot should call Release. Release is
	// idempotent.
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
	return NewSnakeWithClock(cfg, defaultClock)
}

// NewSnakeWithClock creates a Snake with an injected clock (for testing).
func NewSnakeWithClock(cfg SnakeConfig, clockFunc func() int64) *Snake {
	s := &Snake{
		cfg:          cfg,
		clockFunc:    clockFunc,
		holders:      make(map[*Request]struct{}),
		maxAgeTimers: make(map[*Request]*time.Timer),
	}
	s.sq = newSelfContentionAwareCoDelQueue(cfg.CoDel, clockFunc)
	return s
}

// Acquire acquires a slot. It blocks until a slot is granted, the
// request is dropped by CoDel, or the context is cancelled. The returned
// SafeUnlock must be released via defer unlock.Release().
func (s *Snake) Acquire(ctx context.Context) (*SafeUnlock, error) {
	contentionID := ""
	if s.cfg.ContentionID != nil {
		contentionID = s.cfg.ContentionID()
	}

	priority := s.priority()

	s.mu.Lock()
	req, needSchedule, delay := s.sq.lockedEnqueue(contentionID, priority)

	if s.inFlight < s.cfg.Capacity() {
		s.lockedGrant(req)
		if needSchedule {
			s.lockedScheduleDropTimer(delay)
		}
		s.mu.Unlock()
		return &SafeUnlock{s: s, req: req}, nil
	}

	if needSchedule {
		s.lockedScheduleDropTimer(delay)
	}
	s.mu.Unlock()

	select {
	case err := <-req.done:
		if err != nil {
			return nil, s.acquireError()
		}
		return &SafeUnlock{s: s, req: req}, nil

	case <-ctx.Done():
		select {
		case err := <-req.done:
			if err == nil {
				s.releaseInternal(req)
			}
		default:
			s.mu.Lock()
			if _, granted := s.holders[req]; granted {
				s.mu.Unlock()
				s.releaseInternal(req)
			} else {
				s.sq.lockedCancel(contentionID, req)
				s.mu.Unlock()
			}
		}
		return nil, ctx.Err()
	}
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
	return s.sq.lockedIsHealthy()
}

// Release releases the slot. exc is an optional error that caused
// the release (passed to release callbacks). Release is idempotent.
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
	s.mu.Unlock()

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
	s.sq.lockedComplete(req)
	s.inFlight--
	s.lockedTryGrantNext()
	s.mu.Unlock()

	return nil
}

// releaseInternal releases a slot without holder verification or callbacks.
// Used when the context is cancelled after the slot was already granted.
func (s *Snake) releaseInternal(req *Request) {
	s.mu.Lock()
	delete(s.holders, req)
	s.lockedStopMaxAgeTimer(req)
	s.sq.lockedComplete(req)
	s.inFlight--
	s.lockedTryGrantNext()
	s.mu.Unlock()
}

func (s *Snake) lockedGrant(req *Request) {
	s.inFlight++
	s.holders[req] = struct{}{}
	s.sq.lockedMarkNotDroppable(req)
	s.lockedStartMaxAgeTimer(req)
	if !req.isDone() {
		req.signal(nil)
	}
}

func (s *Snake) lockedTryGrantNext() {
	for s.inFlight < s.cfg.Capacity() {
		next := s.sq.lockedFindFirstWaiting()
		if next == nil {
			break
		}
		s.lockedGrant(next)
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

// --- timer management ---

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
