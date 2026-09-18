/*
Copyright 2023 The Vitess Authors.

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

package smartconnpool

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/list"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

type PoolConfig interface {
	LoadshedConfig(string) loadshed.SnakeConfig
}

// waiter represents a client waiting for a connection in the waitlist
type waiter[C Connection] struct {
	// setting is the connection Setting that we'd like, or nil if we'd like a
	// a connection with no Setting applied
	setting *Setting
	// conn is a channel that will receive the connection when it's ready
	conn chan *Pooled[C]
	err  error
	// age is the amount of cycles this client has been on the waitlist
	age uint32
}

type waitlistQueues[C Connection] struct {
	list          list.List[waiter[C]]
	snake         *loadshed.Snake[*list.Element[waiter[C]]]
	mode          func() loadshed.Mode
	activeMode    loadshed.Mode
	transitioning atomic.Bool
}

type waitlist[C Connection] struct {
	nodes sync.Pool
	mu    sync.Mutex

	*waitlistQueues[C]

	// onWait is called when a client gets to the point in which it is waiting for a connection - or the mutex that it needs to grab to wait for a connection.
	onWait func()
	// onWaiterCapReached is called when the waitlist has reached its maximum capacity.
	onWaiterCapReached func()
}

// waitForConn blocks until a connection with the given Setting is returned by another client,
// or until the given context expires.
// If maxWaiters is > 0 and the waitlist already has that many waiters, it returns
// ErrPoolWaiterCapReached immediately without blocking.
// The returned connection may _not_ have the requested Setting. This function can
// also return a `nil` connection even if our context has expired, if the pool has
// forced an expiration of all waiters in the waitlist.
func (wl *waitlist[C]) waitForConn(ctx context.Context, setting *Setting, closeChan <-chan struct{}, maxWaiters uint, dryRun bool) (*Pooled[C], error) {
	elem := wl.nodes.Get().(*list.Element[waiter[C]])
	defer wl.nodes.Put(elem)

	elem.Value = waiter[C]{conn: elem.Value.conn, setting: setting}
	var request *loadshed.Request[*list.Element[waiter[C]]]

	// Fast path: reject early using an atomic read of the list length to avoid
	// contending on the mutex under high query rates. This is racy — the count
	// can change between this check and the lock acquisition — so we re-check
	// under the lock below for correctness. Still, we expect to reject most
	// requests early here when under a heavy load.
	//
	// We do this here rather than further upstream (e.g. in ConnPool.Get) because
	// callers only reach waitForConn after exhausting all other options (idle
	// connections, new connections, settings stacks). There is no point in checking
	// there when those requests can still get a connection without waiting. The cap
	// is just for waiting.
	if wl.aboveWaiterCap(maxWaiters) {
		if !dryRun {
			if wl.onWaiterCapReached != nil {
				wl.onWaiterCapReached()
			}
			return nil, ErrPoolWaiterCapReached
		}
	}

	// If we reach this point, we are waiting, at the very least on the mutex, likely
	// on the connection. So call onWait which takes care of recording the wait.
	if wl.onWait != nil {
		wl.onWait()
	}

	wl.mu.Lock()
	dropped := wl.transitionLocked()
	// Strict check: the list length may have changed since the lockless check
	// above, so we verify again while holding the lock to guarantee the cap is
	// never exceeded.
	if wl.aboveWaiterCap(maxWaiters) {
		if wl.onWaiterCapReached != nil {
			wl.onWaiterCapReached()
		}
		if !dryRun {
			wl.mu.Unlock()
			wl.reject(dropped)
			return nil, ErrPoolWaiterCapReached
		}
	}
	if wl.activeMode == loadshed.ModeOff {
		wl.list.PushBackValue(elem)
	} else {
		var newlyDropped []*list.Element[waiter[C]]
		request, newlyDropped = wl.snake.Enqueue(elem)
		dropped = append(dropped, newlyDropped...)
	}
	wl.mu.Unlock()
	wl.reject(dropped)

	select {
	case <-closeChan:
		// Pool was closed while we were waiting.
		removed := false

		wl.mu.Lock()
		dropped := wl.transitionLocked()
		// Try to find and remove ourselves from the list.
		removed, newlyDropped := wl.cancelLocked(elem, request)
		dropped = append(dropped, newlyDropped...)
		wl.mu.Unlock()
		wl.reject(dropped)

		if removed {
			return nil, ErrConnPoolClosed
		}

		// if we weren't able to remove ourselves from the waitlist, it means
		// another goroutine is trying to hand us a connection
		return <-elem.Value.conn, elem.Value.err

	case <-ctx.Done():
		// Context expired. We need to try to remove ourselves from the waitlist to
		// prevent another goroutine from trying to hand us a connection later on.
		removed := false

		wl.mu.Lock()
		dropped := wl.transitionLocked()
		// Try to find and remove ourselves from the list.
		removed, newlyDropped := wl.cancelLocked(elem, request)
		dropped = append(dropped, newlyDropped...)
		wl.mu.Unlock()
		wl.reject(dropped)

		if removed {
			return nil, context.Cause(ctx)
		}

		// if we weren't able to remove ourselves from the waitlist, it means
		// another goroutine is trying to hand us a connection
		return <-elem.Value.conn, elem.Value.err

	case conn := <-elem.Value.conn:
		return conn, elem.Value.err
	}
}

func (wl *waitlist[C]) aboveWaiterCap(maxWaiters uint) bool {
	return maxWaiters > 0 && wl.waiting() >= int(maxWaiters)
}

func (wl *waitlist[C]) maybeStarvingCount() (maybeStarving int) {
	if wl.waiting() == 0 {
		return
	}

	wl.mu.Lock()
	dropped := wl.transitionLocked()
	if wl.activeMode == loadshed.ModeOff {
		// iterate the waitlist looking for waiters with an expired Context,
		// or remove everything if force is true
		for e := wl.list.Front(); e != nil; e = e.Next() {
			if e.Value.age == 0 {
				maybeStarving++
			}
		}
	} else {
		maybeStarving = wl.snake.CountMatching(func(e *list.Element[waiter[C]]) bool {
			return e.Value.age == 0
		})
	}
	wl.mu.Unlock()
	wl.reject(dropped)

	return
}

// tryReturnConn tries handing over a connection to one of the waiters in the pool.
func (wl *waitlist[D]) tryReturnConn(conn *Pooled[D]) bool {
	// fast path: if there's nobody waiting there's nothing to do
	if !wl.shouldTryReturnConn() {
		return false
	}
	// split the slow path into a separate function to enable inlining
	return wl.tryReturnConnSlow(conn)
}

func (wl *waitlist[C]) shouldTryReturnConn() bool {
	if wl.waiting() != 0 {
		return true
	}
	if wl.transitioning.Load() {
		return true
	}
	return wl.waiting() != 0
}

func (wl *waitlist[D]) tryReturnConnSlow(conn *Pooled[D]) bool {
	const maxAge = 8
	var (
		target      *list.Element[waiter[D]]
		connSetting = conn.Conn.Setting()
	)

	wl.mu.Lock()
	dropped := wl.transitionLocked()
	if wl.activeMode == loadshed.ModeOff {
		target = wl.list.Front()
		// iterate through the waitlist looking for either waiters that have been
		// here too long, or a waiter that is looking exactly for the same Setting
		// as the one we have in our connection.
		for e := target; e != nil; e = e.Next() {
			if e.Value.age > maxAge || e.Value.setting == connSetting {
				target = e
				break
			}
			// this only ages the waiters that are being skipped over: we'll start
			// aging the waiters in the back once they get to the front of the pool.
			// the maxAge of 8 has been set empirically: smaller values cause clients
			// with a specific setting to slightly starve, and aging all the clients
			// in the list every time leads to unfairness when the system is at capacity
			e.Value.age++
		}
		if target != nil {
			wl.list.Remove(target)
		}
	} else {
		// iterate through the waitlist looking for either waiters that have been
		// here too long, or a waiter that is looking exactly for the same Setting
		// as the one we have in our connection.
		var newlyDropped []*list.Element[waiter[D]]
		target, _, newlyDropped = wl.snake.DequeueMatching(func(e *list.Element[waiter[D]]) bool {
			if e.Value.age > maxAge || e.Value.setting == connSetting {
				return true
			}
			// this only ages the waiters that are being skipped over: we'll start
			// aging the waiters in the back once they get to the front of the pool.
			// the maxAge of 8 has been set empirically: smaller values cause clients
			// with a specific setting to slightly starve, and aging all the clients
			// in the list every time leads to unfairness when the system is at capacity
			e.Value.age++
			return false
		})
		dropped = append(dropped, newlyDropped...)
	}
	wl.mu.Unlock()
	wl.reject(dropped)

	// maybe there isn't anybody to hand over the connection to, because we've
	// raced with another client returning another connection
	if target == nil {
		return false
	}

	// if we have a target to return the connection to, simply write the connection
	// into the waiter's channel.
	target.Value.conn <- conn
	// Allow the goroutine waiting on the channel to start running _now_.
	runtime.Gosched()

	return true
}

func (wl *waitlist[C]) cancelLocked(elem *list.Element[waiter[C]], request *loadshed.Request[*list.Element[waiter[C]]]) (bool, []*list.Element[waiter[C]]) {
	if wl.activeMode == loadshed.ModeOff {
		for current := wl.list.Front(); current != nil; current = current.Next() {
			if current == elem {
				wl.list.Remove(elem)
				return true, nil
			}
		}
		return false, nil
	}
	if request != nil {
		removed, dropped := wl.snake.Cancel(request)
		if removed {
			return true, dropped
		}
	}
	return wl.snake.CancelMatching(func(candidate *list.Element[waiter[C]]) bool {
		return candidate == elem
	})
}

func (wl *waitlist[C]) reject(waiters []*list.Element[waiter[C]]) {
	for _, elem := range waiters {
		elem.Value.err = ErrPoolLoadShed
		elem.Value.conn <- nil
	}
}

func (wl *waitlist[C]) runDropTimer() {
	wl.mu.Lock()
	dropped := wl.transitionLocked()
	if wl.activeMode != loadshed.ModeOff {
		dropped = append(dropped, wl.snake.LockedDropTimerFired()...)
	}
	wl.mu.Unlock()
	wl.reject(dropped)
}

func (wl *waitlist[C]) runShadowTimer() {
	wl.mu.Lock()
	dropped := wl.transitionLocked()
	if wl.activeMode != loadshed.ModeOff {
		wl.snake.LockedShadowTimerFired()
	}
	wl.mu.Unlock()
	wl.reject(dropped)
}

func (wl *waitlist[C]) transitionLocked() []*list.Element[waiter[C]] {
	mode := wl.mode()
	if mode == wl.activeMode {
		return nil
	}

	movesQueues := (wl.activeMode == loadshed.ModeOff) != (mode == loadshed.ModeOff)
	if movesQueues {
		wl.transitioning.Store(true)
		defer wl.transitioning.Store(false)
	}

	var dropped []*list.Element[waiter[C]]
	switch {
	case wl.activeMode == loadshed.ModeOff && mode != loadshed.ModeOff:
		for elem := wl.list.Front(); elem != nil; {
			next := elem.Next()
			wl.list.Remove(elem)
			_, newlyDropped := wl.snake.EnqueueExisting(elem)
			dropped = append(dropped, newlyDropped...)
			elem = next
		}
	case wl.activeMode != loadshed.ModeOff && mode == loadshed.ModeOff:
		for _, elem := range wl.snake.Drain() {
			wl.list.PushBackValue(elem)
		}
	}
	wl.activeMode = mode
	return dropped
}

func (wl *waitlist[C]) init(poolName string, config PoolConfig) {
	wl.nodes.New = func() any {
		return &list.Element[waiter[C]]{
			Value: waiter[C]{conn: make(chan *Pooled[C], 1)},
		}
	}

	snakeConfig := loadshed.SnakeConfig{}
	if config != nil {
		snakeConfig = config.LoadshedConfig(poolName)
	}
	if snakeConfig.Mode == nil {
		snakeConfig.Mode = func() loadshed.Mode { return loadshed.ModeOff }
	}
	snakeConfig.DropTimerFired = wl.runDropTimer
	snakeConfig.ShadowTimerFired = wl.runShadowTimer

	wl.waitlistQueues = &waitlistQueues[C]{
		mode:       snakeConfig.Mode,
		activeMode: snakeConfig.Mode(),
	}
	wl.list.Init()
	wl.snake = loadshed.NewSnake[*list.Element[waiter[C]]](snakeConfig)
}

func (wl *waitlist[C]) waiting() int {
	return wl.list.Len() + wl.snake.Len()
}
