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
	"time"

	"vitess.io/vitess/go/list"
)

// waiter represents a client waiting for a connection in the waitlist
type waiter[C Connection] struct {
	// setting is the connection Setting that we'd like, or nil if we'd like a
	// a connection with no Setting applied
	setting *Setting
	// conn is a channel that will receive the connection when it's ready
	conn chan *Pooled[C]
	// age is the amount of cycles this client has been on the waitlist
	age uint32
	// enqueuedAt is the monotonic time at which this waiter joined the list,
	// used to measure sojourn for CoDel load shedding.
	enqueuedAt time.Duration
	// shed is set by the CoDel head-drop path when this waiter is evicted rather
	// than handed a connection; the receiver maps it to ErrPoolLoadShed. It
	// disambiguates a shed from the pool-forced-expiration case, which also
	// delivers a nil connection.
	shed bool
}

type waitlist[C Connection] struct {
	nodes sync.Pool
	mu    sync.Mutex
	list  list.List[waiter[C]]
	// onWait is called when a client gets to the point in which it is waiting for a connection - or the mutex that it needs to grab to wait for a connection.
	onWait func()
	// onWaiterCapReached is called when the waitlist has reached its maximum capacity.
	onWaiterCapReached func()
	// codel is the pool-level CoDel load shedder, or nil if shedding is disabled.
	codel *codelState
}

// waitForConn blocks until a connection with the given Setting is returned by another client,
// or until the given context expires.
// If maxWaiters is > 0 and the waitlist already has that many waiters, it returns
// ErrPoolWaiterCapReached immediately without blocking.
// The returned connection may _not_ have the requested Setting. This function can
// also return a `nil` connection even if our context has expired, if the pool has
// forced an expiration of all waiters in the waitlist.
func (wl *waitlist[C]) waitForConn(ctx context.Context, setting *Setting, closeChan <-chan struct{}, maxWaiters uint) (*Pooled[C], error) {
	elem := wl.nodes.Get().(*list.Element[waiter[C]])
	defer wl.nodes.Put(elem)

	elem.Value = waiter[C]{conn: elem.Value.conn, setting: setting}

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
		if wl.onWaiterCapReached != nil {
			wl.onWaiterCapReached()
		}
		return nil, ErrPoolWaiterCapReached
	}

	// If we reach this point, we are waiting, at the very least on the mutex, likely
	// on the connection. So call onWait which takes care of recording the wait.
	if wl.onWait != nil {
		wl.onWait()
	}

	wl.mu.Lock()
	// Strict check: the list length may have changed since the lockless check
	// above, so we verify again while holding the lock to guarantee the cap is
	// never exceeded.
	if wl.aboveWaiterCap(maxWaiters) {
		wl.mu.Unlock()
		if wl.onWaiterCapReached != nil {
			wl.onWaiterCapReached()
		}
		return nil, ErrPoolWaiterCapReached
	}
	if wl.codel != nil {
		elem.Value.enqueuedAt = monotonicNow()
	}
	wl.list.PushBackValue(elem)
	wl.mu.Unlock()

	select {
	case <-closeChan:
		// Pool was closed while we were waiting.
		removed := false

		wl.mu.Lock()
		// Try to find and remove ourselves from the list.
		for e := wl.list.Front(); e != nil; e = e.Next() {
			if e == elem {
				wl.list.Remove(elem)
				removed = true
				break
			}
		}
		wl.mu.Unlock()

		if removed {
			return nil, ErrConnPoolClosed
		}

		// if we weren't able to remove ourselves from the waitlist, it means
		// another goroutine is trying to hand us a connection (or shed us)
		conn := <-elem.Value.conn
		if elem.Value.shed {
			return nil, ErrPoolLoadShed
		}
		return conn, nil

	case <-ctx.Done():
		// Context expired. We need to try to remove ourselves from the waitlist to
		// prevent another goroutine from trying to hand us a connection later on.
		removed := false

		wl.mu.Lock()
		// Try to find and remove ourselves from the list.
		for e := wl.list.Front(); e != nil; e = e.Next() {
			if e == elem {
				wl.list.Remove(elem)
				removed = true
				break
			}
		}
		wl.mu.Unlock()

		if removed {
			return nil, context.Cause(ctx)
		}

		// if we weren't able to remove ourselves from the waitlist, it means
		// another goroutine is trying to hand us a connection (or shed us)
		conn := <-elem.Value.conn
		if elem.Value.shed {
			return nil, ErrPoolLoadShed
		}
		return conn, nil

	case conn := <-elem.Value.conn:
		if elem.Value.shed {
			return nil, ErrPoolLoadShed
		}
		return conn, nil
	}
}

func (wl *waitlist[C]) aboveWaiterCap(maxWaiters uint) bool {
	return maxWaiters > 0 && wl.list.Len() >= int(maxWaiters)
}

func (wl *waitlist[C]) maybeStarvingCount() (maybeStarving int) {
	if wl.list.Len() == 0 {
		return
	}

	wl.mu.Lock()
	defer wl.mu.Unlock()

	// iterate the waitlist looking for waiters with an expired Context,
	// or remove everything if force is true
	for e := wl.list.Front(); e != nil; e = e.Next() {
		if e.Value.age == 0 {
			maybeStarving++
		}
	}

	return
}

// tryReturnConn tries handing over a connection to one of the waiters in the pool.
func (wl *waitlist[D]) tryReturnConn(conn *Pooled[D]) bool {
	// fast path: if there's nobody waiting there's nothing to do
	if wl.list.Len() == 0 {
		return false
	}
	// split the slow path into a separate function to enable inlining
	return wl.tryReturnConnSlow(conn)
}

func (wl *waitlist[D]) tryReturnConnSlow(conn *Pooled[D]) bool {
	const maxAge = 8
	var (
		target      *list.Element[waiter[D]]
		connSetting = conn.Conn.Setting()
		shed        []*list.Element[waiter[D]]
	)

	wl.mu.Lock()
	// CoDel head-drop: when the queue's realized sojourn has stayed above target
	// for a full interval, evict stale head-of-line waiters (the oldest, most
	// likely to have burned their deadline) so the returned connection goes to a
	// fresher waiter. The connection itself is untouched — only which waiter
	// receives it changes. Evicted waiters are signaled after the lock is
	// released. Note this only runs when a connection is returned, so a total
	// stall with no returns will not shed; the waiter cap is the backstop there.
	if wl.codel != nil {
		now := int64(monotonicNow())
		for front := wl.list.Front(); front != nil; front = wl.list.Front() {
			sojourn := now - int64(front.Value.enqueuedAt)
			if !wl.codel.overTarget(sojourn, now) || !wl.codel.dropDue(now) {
				break
			}
			wl.list.Remove(front)
			front.Value.shed = true
			shed = append(shed, front)
		}
	}

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
	wl.mu.Unlock()

	// Signal evicted waiters outside the lock: each already carries shed=true, so
	// a nil send resolves to ErrPoolLoadShed on the receiving side. Doing this off
	// the lock keeps the wakeup storm out of the critical section.
	for _, s := range shed {
		s.Value.conn <- nil
	}

	// maybe there isn't anybody to hand over the connection to, because we've
	// raced with another client returning another connection (or shed everyone)
	if target == nil {
		return len(shed) > 0
	}

	// if we have a target to return the connection to, simply write the connection
	// into the waiter's channel.
	target.Value.conn <- conn
	// Allow the goroutine waiting on the channel to start running _now_.
	runtime.Gosched()

	return true
}

func (wl *waitlist[C]) init() {
	wl.nodes.New = func() any {
		return &list.Element[waiter[C]]{
			Value: waiter[C]{conn: make(chan *Pooled[C])},
		}
	}
	wl.list.Init()
}

func (wl *waitlist[C]) waiting() int {
	return wl.list.Len()
}
