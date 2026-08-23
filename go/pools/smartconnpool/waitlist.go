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

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

type PoolConfig interface {
	LoadshedConfig(string) (func() bool, func() time.Duration, func() time.Duration)
}

// waiter represents a client waiting for a connection in the waitlist
type waiter[C Connection] struct {
	// conn is a channel that will receive the connection when it's ready
	conn    chan *Pooled[C]
	request *loadshed.Request[*waiter[C]]
	err     error
}

type waitlist[C Connection] struct {
	nodes sync.Pool
	mu    sync.Mutex
	snake *loadshed.Snake[*waiter[C]]

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
func (wl *waitlist[C]) waitForConn(ctx context.Context, setting *Setting, closeChan <-chan struct{}, maxWaiters uint, valveID string, priority float64) (*Pooled[C], error) {
	elem := wl.nodes.Get().(*waiter[C])
	defer wl.nodes.Put(elem)

	conn := elem.conn
	*elem = waiter[C]{conn: conn}

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
	request, dropped := wl.snake.Enqueue(elem, valveID, snakePriority(priority))
	elem.request = request
	wl.syncTimer()
	wl.mu.Unlock()
	wl.reject(dropped)

	select {
	case <-closeChan:
		// Pool was closed while we were waiting.
		wl.mu.Lock()
		// Try to find and remove ourselves from the list.
		removed, dropped := wl.snake.Cancel(elem.request)
		wl.syncTimer()
		wl.mu.Unlock()
		wl.reject(dropped)

		if removed {
			return nil, ErrConnPoolClosed
		}

		// if we weren't able to remove ourselves from the waitlist, it means
		// another goroutine is trying to hand us a connection
		return <-elem.conn, elem.err

	case <-ctx.Done():
		// Context expired. We need to try to remove ourselves from the waitlist to
		// prevent another goroutine from trying to hand us a connection later on.
		wl.mu.Lock()
		// Try to find and remove ourselves from the list.
		removed, dropped := wl.snake.Cancel(elem.request)
		wl.syncTimer()
		wl.mu.Unlock()
		wl.reject(dropped)

		if removed {
			return nil, context.Cause(ctx)
		}

		// if we weren't able to remove ourselves from the waitlist, it means
		// another goroutine is trying to hand us a connection
		return <-elem.conn, elem.err

	case conn := <-elem.conn:
		return conn, elem.err
	}
}

func (wl *waitlist[C]) aboveWaiterCap(maxWaiters uint) bool {
	return maxWaiters > 0 && wl.snake.Len() >= int(maxWaiters)
}

func snakePriority(priority float64) float64 {
	if priority == loadshed.PriorityUndroppable {
		return priority
	}
	return float64(sqlparser.MaxPriorityValue) - priority
}

func (wl *waitlist[C]) maybeStarvingCount() int {
	return wl.snake.Len()
}

// tryReturnConn tries handing over a connection to one of the waiters in the pool.
func (wl *waitlist[D]) tryReturnConn(conn *Pooled[D]) bool {
	// fast path: if there's nobody waiting there's nothing to do
	if wl.snake.Len() == 0 {
		return false
	}
	// split the slow path into a separate function to enable inlining
	return wl.tryReturnConnSlow(conn)
}

func (wl *waitlist[D]) tryReturnConnSlow(conn *Pooled[D]) bool {
	wl.mu.Lock()
	waiter, ok, dropped := wl.snake.Dequeue()
	wl.syncTimer()
	wl.mu.Unlock()
	wl.reject(dropped)

	// maybe there isn't anybody to hand over the connection to, because we've
	// raced with another client returning another connection
	if !ok {
		return false
	}

	// if we have a target to return the connection to, simply write the connection
	// into the waiter's channel.
	waiter.conn <- conn
	// Allow the goroutine waiting on the channel to start running _now_.
	runtime.Gosched()

	return true
}

func (wl *waitlist[C]) reject(waiters []*waiter[C]) {
	for _, waiter := range waiters {
		waiter.err = ErrPoolLoadShed
		waiter.conn <- nil
	}
}

func (wl *waitlist[C]) syncTimer() {
	if delay, ok := wl.snake.LockedTimerUpdate(); ok {
		time.AfterFunc(delay, wl.runDropTimer)
	}
}

func (wl *waitlist[C]) runDropTimer() {
	wl.mu.Lock()
	dropped := wl.snake.LockedDropTimerFired()
	wl.syncTimer()
	wl.mu.Unlock()
	wl.reject(dropped)
}

func (wl *waitlist[C]) init(poolName string, config PoolConfig) {
	wl.nodes.New = func() any {
		return &waiter[C]{conn: make(chan *Pooled[C], 1)}
	}

	enabled := func() bool { return false }
	target := func() time.Duration { return time.Second }
	interval := func() time.Duration { return time.Second }
	if config != nil {
		enabled, target, interval = config.LoadshedConfig(poolName)
	}

	wl.snake = loadshed.NewSnake[*waiter[C]](loadshed.SnakeConfig{
		LoadsheddingAllowed: enabled,
		CoDel: loadshed.CoDelConfig{
			IntervalNs:     func() int64 { return interval().Nanoseconds() },
			TargetNs:       func() int64 { return target().Nanoseconds() },
			Exponent:       func() float64 { return 1 },
			MinDropDelayNs: func() int64 { return int64(100 * time.Millisecond) },
		},
	})
}

func (wl *waitlist[C]) registerStats(exporter *servenv.Exporter, poolName string) {
	var statsName string
	switch poolName {
	case "ConnPool":
		statsName = "SnakeOltpRead"
	case "TransactionPool":
		statsName = "SnakeDml"
	case "FoundRowsPool":
		statsName = "SnakeDmlFoundRows"
	}
	if statsName != "" {
		loadshed.PublishStats(exporter, statsName, wl.snake)
	}
}

func (wl *waitlist[C]) waiting() int {
	return wl.snake.Len()
}
