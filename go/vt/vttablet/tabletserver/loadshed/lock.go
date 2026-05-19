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
	// LockConfig configures a Lock. Functions are used to allow dynamic runtime
	// tuning.
	LockConfig struct {
		Name                string
		CoDel               CoDelConfig
		MaxAge              func() time.Duration
		LoadsheddingAllowed func() bool
		AcquireError        func() error
		ReleaseCBs          []func(error)
	}

	// Lock is a CoDel-based load-shedding lock. Acquire requests are either
	// granted or dropped, each within a timely manner.
	//
	// Callers must use defer unlock.Release() to ensure the lock is always
	// released.
	Lock struct {
		mu sync.Mutex

		q              *SelfContentionAwareCoDelQueue
		holder         *Request
		lockNonce      uint64
		dropTimer      *time.Timer
		dropTimerArmed bool
		maxAgeTimer    *time.Timer
		cfg            LockConfig
		clockFunc      func() int64
	}

	// SafeUnlock is a handle for releasing a lock. Only the goroutine that
	// acquired the lock should call Release. Release is idempotent.
	SafeUnlock struct {
		l     *Lock
		nonce uint64
		once  sync.Once
		err   error
	}
)

var epoch = time.Now()

func defaultClock() int64 {
	return time.Since(epoch).Nanoseconds()
}

// NewLock creates a new CoDel-based load-shedding lock.
func NewLock(cfg LockConfig) *Lock {
	l := &Lock{
		cfg:       cfg,
		clockFunc: defaultClock,
	}
	l.q = newSelfContentionAwareCoDelQueue(cfg.CoDel, defaultClock, l.lockedScheduleDropTimer)
	return l
}

// Acquire acquires the lock. It blocks until the lock is granted, the request
// is dropped by CoDel, or the context is cancelled. The returned SafeUnlock
// must be released via defer unlock.Release(). valveID controls self-contention
// awareness: requests with the same non-empty ID are serialized through the
// valve so at most one is in the CoDel queue at a time. Pass "" to bypass.
func (l *Lock) Acquire(ctx context.Context, valveID string) (*SafeUnlock, error) {
	priority := l.priority()

	l.mu.Lock()
	isLocked := l.q.lockedPeek() != nil
	req := l.q.lockedEnqueue(valveID, priority)

	if !isLocked {
		l.lockNonce++
		nonce := l.lockNonce
		l.holder = req
		l.q.lockedMarkNotDroppable(req)
		l.lockedStartMaxAgeTimer(req)
		l.mu.Unlock()
		return &SafeUnlock{l: l, nonce: nonce}, nil
	}

	l.mu.Unlock()

	select {
	case err := <-req.result:
		if err != nil {
			return nil, l.acquireError()
		}
		l.mu.Lock()
		nonce := l.lockNonce
		l.lockedStartMaxAgeTimer(req)
		l.mu.Unlock()
		return &SafeUnlock{l: l, nonce: nonce}, nil

	case <-ctx.Done():
		select {
		case err := <-req.result:
			if err == nil {
				l.releaseInternal()
			}
		default:
			l.mu.Lock()
			if l.holder == req {
				l.mu.Unlock()
				<-req.result
				l.releaseInternal()
			} else {
				l.q.lockedCancel(req)
				l.mu.Unlock()
			}
		}
		return nil, ctx.Err()
	}
}

// IsLocked reports whether the lock is currently held.
func (l *Lock) IsLocked() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.q.lockedPeek() != nil
}

// IsHealthy reports whether the CoDel queue is healthy.
func (l *Lock) IsHealthy() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.q.lockedIsHealthy()
}

// Release releases the lock. exc is an optional error that caused the release
// (passed to release callbacks). Release is idempotent.
func (u *SafeUnlock) Release(exc ...error) error {
	u.once.Do(func() {
		var excValue error
		if len(exc) > 0 {
			excValue = exc[0]
		}
		u.err = u.l.release(u.nonce, excValue)
	})
	return u.err
}

func (l *Lock) release(nonce uint64, excValue error) error {
	l.mu.Lock()
	if nonce != l.lockNonce {
		currentNonce := l.lockNonce
		l.mu.Unlock()
		return fmt.Errorf("unauthorized release: nonce %d != %d", nonce, currentNonce)
	}
	l.lockedStopMaxAgeTimer()
	next := l.lockedGrantNext()
	l.mu.Unlock()

	if next != nil {
		next.signal(nil)
	}

	l.runReleaseCBs(excValue)
	return nil
}

// releaseInternal releases the lock without nonce verification or callbacks.
// Used when the context is cancelled after the lock was already granted.
func (l *Lock) releaseInternal() {
	l.mu.Lock()
	l.lockedStopMaxAgeTimer()
	next := l.lockedGrantNext()
	l.mu.Unlock()

	if next != nil {
		next.signal(nil)
	}
}

// lockedGrantNext dequeues the current holder, promotes from valve, and grants
// to the next waiter. Returns the next request to signal, or nil.
func (l *Lock) lockedGrantNext() *Request {
	l.q.lockedDequeue()
	next := l.q.lockedPeek()
	if next != nil {
		l.lockNonce++
		l.holder = next
		l.q.lockedMarkNotDroppable(next)
	} else {
		l.holder = nil
	}
	return next
}

// runReleaseCBs executes release callbacks outside the mutex.
// Release callbacks may block or call back into the Lock (e.g., to read
// IsLocked); holding the mutex would deadlock.
func (l *Lock) runReleaseCBs(excValue error) {
	for _, cb := range l.cfg.ReleaseCBs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("loadshed: panic in release callback for %s: %v", l.cfg.Name, r)
				}
			}()
			cb(excValue)
		}()
	}
}

func (l *Lock) priority() *float64 {
	if l.cfg.LoadsheddingAllowed != nil && !l.cfg.LoadsheddingAllowed() {
		return newUndroppablePriority()
	}
	return NewPriority(0)
}

func (l *Lock) acquireError() error {
	if l.cfg.AcquireError != nil {
		return l.cfg.AcquireError()
	}
	return &DroppedRequestError{}
}

// --- timer management (must be called with l.mu held) ---

func (l *Lock) lockedScheduleDropTimer(delayNs int64) {
	if l.dropTimerArmed {
		return
	}
	l.dropTimerArmed = true
	delay := time.Duration(delayNs) * time.Nanosecond
	l.dropTimer = time.AfterFunc(delay, l.runDropTimer)
}

func (l *Lock) runDropTimer() {
	l.mu.Lock()
	if !l.dropTimerArmed {
		l.mu.Unlock()
		return
	}
	l.dropTimerArmed = false
	l.q.lockedRunScheduledDrop()
	l.mu.Unlock()
}

func (l *Lock) lockedStartMaxAgeTimer(holder *Request) {
	if l.cfg.MaxAge == nil {
		return
	}
	maxAge := l.cfg.MaxAge()
	if maxAge <= 0 {
		return
	}
	l.maxAgeTimer = time.AfterFunc(maxAge, func() {
		l.mu.Lock()
		if l.holder != holder {
			l.mu.Unlock()
			return
		}
		nonce := l.lockNonce
		l.mu.Unlock()

		log.Printf("loadshed: lock %s reached max age %v, force-releasing", l.cfg.Name, maxAge)
		l.release(nonce, nil)
	})
}

func (l *Lock) lockedStopMaxAgeTimer() {
	if l.maxAgeTimer != nil {
		l.maxAgeTimer.Stop()
		l.maxAgeTimer = nil
	}
}
