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
	"time"

	"vitess.io/vitess/go/list"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

type (
	PoolConfig interface {
		LoadshedConfig(string) (func() loadshed.Mode, func() time.Duration, func() time.Duration)
	}

	// waiter represents a client waiting for a connection in the waitlist
	waiter[C Connection] struct {
		// setting is the connection Setting that we'd like, or nil if we'd like a
		// a connection with no Setting applied
		setting *Setting
		// conn is a channel that will receive the connection when it's ready
		conn chan *Pooled[C]
		// age is the amount of cycles this client has been on the waitlist
		age uint32
	}

	waitlistQueues[C Connection] struct {
		list          list.List[waiter[C]]
		snake         *loadshed.Snake[*list.Element[waiter[C]]]
		mode          func() loadshed.Mode
		activeMode    loadshed.Mode
		transitioning atomic.Bool
	}

	waitlist[C Connection] struct {
		nodes  sync.Pool
		mu     sync.Mutex
		queues *waitlistQueues[C]

		// onWait is called when a client gets to the point in which it is waiting for a connection - or the mutex that it needs to grab to wait for a connection.
		onWait func()
		// onWaiterCapReached is called when the waitlist has reached its maximum capacity.
		onWaiterCapReached func()
	}
)

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

	conn := elem.Value.conn
	elem.Value = waiter[C]{conn: conn, setting: setting}
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
	wl.transitionLocked()
	// Strict check: the list length may have changed since the lockless check
	// above, so we verify again while holding the lock to guarantee the cap is
	// never exceeded.
	if wl.aboveWaiterCap(maxWaiters) {
		if wl.onWaiterCapReached != nil {
			wl.onWaiterCapReached()
		}
		if !dryRun {
			wl.mu.Unlock()
			return nil, ErrPoolWaiterCapReached
		}
	}
	if wl.queues.activeMode == loadshed.ModeOff {
		wl.queues.list.PushBackValue(elem)
	} else {
		request, _ = wl.queues.snake.Enqueue(elem, "", 0)
	}
	wl.mu.Unlock()

	select {
	case <-closeChan:
		// Pool was closed while we were waiting.
		wl.mu.Lock()
		wl.transitionLocked()
		removed := wl.cancelLocked(elem, request)
		wl.mu.Unlock()

		if removed {
			return nil, ErrConnPoolClosed
		}
		// if we weren't able to remove ourselves from the waitlist, it means
		// another goroutine is trying to hand us a connection
		return <-elem.Value.conn, nil

	case <-ctx.Done():
		// Context expired. We need to try to remove ourselves from the waitlist to
		// prevent another goroutine from trying to hand us a connection later on.
		wl.mu.Lock()
		wl.transitionLocked()
		removed := wl.cancelLocked(elem, request)
		wl.mu.Unlock()

		if removed {
			return nil, context.Cause(ctx)
		}
		// if we weren't able to remove ourselves from the waitlist, it means
		// another goroutine is trying to hand us a connection
		return <-elem.Value.conn, nil

	case conn := <-elem.Value.conn:
		return conn, nil
	}
}

func (wl *waitlist[C]) aboveWaiterCap(maxWaiters uint) bool {
	return maxWaiters > 0 && wl.waiting() >= int(maxWaiters)
}

func (wl *waitlist[C]) maybeStarvingCount() int {
	if wl.waiting() == 0 {
		return 0
	}

	wl.mu.Lock()
	defer wl.mu.Unlock()
	wl.transitionLocked()
	if wl.queues.activeMode == loadshed.ModeOff {
		count := 0
		for elem := wl.queues.list.Front(); elem != nil; elem = elem.Next() {
			if elem.Value.age == 0 {
				count++
			}
		}
		return count
	}
	return wl.queues.snake.CountMatching(func(elem *list.Element[waiter[C]]) bool {
		return elem.Value.age == 0
	})
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
	if wl.queues.transitioning.Load() {
		return true
	}
	return wl.waiting() != 0
}

func (wl *waitlist[D]) tryReturnConnSlow(conn *Pooled[D]) bool {
	const maxAge = 8
	connSetting := conn.Conn.Setting()

	wl.mu.Lock()
	wl.transitionLocked()
	var (
		selected *list.Element[waiter[D]]
		ok       bool
	)
	if wl.queues.activeMode == loadshed.ModeOff {
		target := wl.queues.list.Front()
		for elem := target; elem != nil; elem = elem.Next() {
			if elem.Value.age > maxAge || elem.Value.setting == connSetting {
				target = elem
				break
			}
			elem.Value.age++
		}
		if target != nil {
			wl.queues.list.Remove(target)
			selected = target
			ok = true
		}
	} else {
		// iterate through the waitlist looking for either waiters that have been
		// here too long, or a waiter that is looking exactly for the same Setting
		// as the one we have in our connection.
		selected, ok, _ = wl.queues.snake.DequeueMatching(func(elem *list.Element[waiter[D]]) bool {
			if elem.Value.age > maxAge || elem.Value.setting == connSetting {
				return true
			}
			// this only ages the waiters that are being skipped over: we'll start
			// aging the waiters in the back once they get to the front of the pool.
			// the maxAge of 8 has been set empirically: smaller values cause clients
			// with a specific setting to slightly starve, and aging all the clients
			// in the list every time leads to unfairness when the system is at capacity
			elem.Value.age++
			return false
		})
	}
	wl.mu.Unlock()

	// maybe there isn't anybody to hand over the connection to, because we've
	// raced with another client returning another connection
	if !ok {
		return false
	}

	// if we have a target to return the connection to, simply write the connection
	// into the waiter's channel.
	selected.Value.conn <- conn
	// Allow the goroutine waiting on the channel to start running _now_.
	runtime.Gosched()
	return true
}

func (wl *waitlist[C]) cancelLocked(elem *list.Element[waiter[C]], request *loadshed.Request[*list.Element[waiter[C]]]) bool {
	if wl.queues.activeMode == loadshed.ModeOff {
		for current := wl.queues.list.Front(); current != nil; current = current.Next() {
			if current == elem {
				wl.queues.list.Remove(elem)
				return true
			}
		}
		return false
	}
	if request != nil {
		removed, _ := wl.queues.snake.Cancel(request)
		if removed {
			return true
		}
	}
	removed, _ := wl.queues.snake.CancelMatching(func(candidate *list.Element[waiter[C]]) bool {
		return candidate == elem
	})
	return removed
}

func (wl *waitlist[C]) transitionLocked() {
	mode := wl.queues.mode()
	if mode == wl.queues.activeMode {
		return
	}

	movesQueues := (wl.queues.activeMode == loadshed.ModeOff) != (mode == loadshed.ModeOff)
	if movesQueues {
		wl.queues.transitioning.Store(true)
		defer wl.queues.transitioning.Store(false)
	}

	switch {
	case wl.queues.activeMode == loadshed.ModeOff && mode != loadshed.ModeOff:
		for elem := wl.queues.list.Front(); elem != nil; {
			next := elem.Next()
			wl.queues.list.Remove(elem)
			wl.queues.snake.EnqueueExisting(elem, "", loadshed.PriorityUndroppable)
			elem = next
		}
	case wl.queues.activeMode != loadshed.ModeOff && mode == loadshed.ModeOff:
		for _, elem := range wl.queues.snake.Drain() {
			wl.queues.list.PushBackValue(elem)
		}
	}
	wl.queues.activeMode = mode
}

func (wl *waitlist[C]) init(poolName string, config PoolConfig) {
	wl.nodes.New = func() any {
		return &list.Element[waiter[C]]{
			Value: waiter[C]{conn: make(chan *Pooled[C])},
		}
	}

	mode := func() loadshed.Mode { return loadshed.ModeOff }
	target := func() time.Duration { return time.Second }
	interval := func() time.Duration { return time.Second }
	if config != nil {
		mode, target, interval = config.LoadshedConfig(poolName)
	}

	wl.queues = &waitlistQueues[C]{
		mode:       mode,
		activeMode: mode(),
	}
	wl.queues.list.Init()
	wl.queues.snake = loadshed.NewSnake[*list.Element[waiter[C]]](loadshed.SnakeConfig{
		Mode: mode,
		CoDel: loadshed.CoDelConfig{
			IntervalNs:     func() int64 { return interval().Nanoseconds() },
			TargetNs:       func() int64 { return target().Nanoseconds() },
			Exponent:       func() float64 { return 1 },
			MinDropDelayNs: func() int64 { return int64(100 * time.Millisecond) },
		},
	})
}

func (wl *waitlist[C]) waiting() int {
	return wl.queues.list.Len() + wl.queues.snake.Len()
}
