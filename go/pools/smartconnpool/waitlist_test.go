/*
Copyright 2024 The Vitess Authors.

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/list"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

type testPoolConfig struct{}

func (testPoolConfig) LoadshedConfig(string) (func() loadshed.Mode, func() time.Duration, func() time.Duration) {
	return func() loadshed.Mode { return loadshed.ModeEnabled },
		func() time.Duration { return time.Millisecond },
		func() time.Duration { return time.Millisecond }
}

type mutableTestPoolConfig struct {
	mode atomic.Value
}

func newMutableTestPoolConfig(mode loadshed.Mode) *mutableTestPoolConfig {
	config := &mutableTestPoolConfig{}
	config.mode.Store(mode)
	return config
}

func (c *mutableTestPoolConfig) LoadshedConfig(string) (func() loadshed.Mode, func() time.Duration, func() time.Duration) {
	return func() loadshed.Mode { return c.mode.Load().(loadshed.Mode) },
		func() time.Duration { return time.Second },
		func() time.Duration { return time.Second }
}

func (c *mutableTestPoolConfig) setMode(mode loadshed.Mode) {
	c.mode.Store(mode)
}

func enqueueSnakeWaiter(wl *waitlist[*TestConn], value waiter[*TestConn]) *list.Element[waiter[*TestConn]] {
	elem := &list.Element[waiter[*TestConn]]{Value: value}
	wl.queues.snake.Enqueue(elem, "", loadshed.PriorityUndroppable)
	return elem
}

func TestWaitlistPoolCloseWithMultipleWaiters(t *testing.T) {
	wait := waitlist[*TestConn]{}
	wait.init("", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	poolClose := make(chan struct{})

	waiterCount := 2
	expireCount := atomic.Int32{}

	for i := 0; i < waiterCount; i++ {
		go func() {
			_, err := wait.waitForConn(ctx, nil, poolClose, 0, "", loadshed.PriorityUndroppable, false)

			if err != nil {
				expireCount.Add(1)
			}
		}()
	}

	close(poolClose)

	// Wait for the context to expire
	<-ctx.Done()

	// Wait for the notified goroutines to finish
	timeout := time.After(1 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for expireCount.Load() != int32(waiterCount) {
		select {
		case <-timeout:
			require.Failf(t, "Timed out waiting for all waiters to expire", "Wanted %d, got %d", waiterCount, expireCount.Load())
		case <-ticker.C:
			// try again
		}
	}

	assert.Equal(t, int32(waiterCount), expireCount.Load())
}

func TestWaitlistWaiterCap(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("", nil)

	poolClose := make(chan struct{})

	const maxWaiters = 3

	errs := make(chan error, maxWaiters)
	for i := 1; i <= maxWaiters; i++ {
		go func() {
			_, err := wl.waitForConn(context.Background(), nil, poolClose, maxWaiters, "valve", loadshed.PriorityUndroppable, false)
			errs <- err
		}()

		assert.Eventually(t, func() bool {
			return wl.waiting() == i
		}, time.Second, 5*time.Millisecond)
	}

	_, err := wl.waitForConn(context.Background(), nil, poolClose, maxWaiters, "valve", loadshed.PriorityUndroppable, false)
	assert.ErrorIs(t, err, ErrPoolWaiterCapReached)
	assert.Equal(t, maxWaiters, wl.waiting())

	close(poolClose)

	for i := 0; i < maxWaiters; i++ {
		assert.NotErrorIs(t, <-errs, ErrPoolWaiterCapReached)
	}
}

func TestWaitlistShedsQueuedRequests(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("ConnPool", testPoolConfig{})

	poolClose := make(chan struct{})
	errs := make(chan error, 6)
	var waiting atomic.Int32
	wl.onWait = func() {
		waiting.Add(1)
	}

	for range 6 {
		go func() {
			_, err := wl.waitForConn(context.Background(), nil, poolClose, 0, "", 0, false)
			errs <- err
		}()
	}

	require.Eventually(t, func() bool {
		return waiting.Load() == 6
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, <-errs, ErrPoolLoadShed)

	close(poolClose)
	for range 5 {
		<-errs
	}
}

func TestWaitlistLegacyPreservesSettingAffinityAndAging(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("", nil)

	foo := &list.Element[waiter[*TestConn]]{
		Value: waiter[*TestConn]{setting: sFoo, conn: make(chan *Pooled[*TestConn], 1)},
	}
	wl.queues.list.PushBackValue(foo)
	bar := &list.Element[waiter[*TestConn]]{
		Value: waiter[*TestConn]{setting: sBar, conn: make(chan *Pooled[*TestConn], 1)},
	}
	wl.queues.list.PushBackValue(bar)
	conn := &Pooled[*TestConn]{Conn: &TestConn{setting: sBar}}

	require.True(t, wl.tryReturnConn(conn))
	assert.Same(t, conn, <-bar.Value.conn)
	assert.Equal(t, uint32(1), foo.Value.age)
	assert.Zero(t, wl.maybeStarvingCount(), "foo has already aged")

	foo.Value.age = 9
	bar = &list.Element[waiter[*TestConn]]{
		Value: waiter[*TestConn]{setting: sBar, conn: make(chan *Pooled[*TestConn], 1)},
	}
	wl.queues.list.PushBackValue(bar)

	require.True(t, wl.tryReturnConn(conn))
	assert.Same(t, conn, <-foo.Value.conn)
}

func TestWaitlistSnakePreservesSettingAffinityAndAging(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("ConnPool", testPoolConfig{})

	foo := enqueueSnakeWaiter(&wl, waiter[*TestConn]{setting: sFoo, conn: make(chan *Pooled[*TestConn], 1)})
	bar := enqueueSnakeWaiter(&wl, waiter[*TestConn]{setting: sBar, conn: make(chan *Pooled[*TestConn], 1)})
	conn := &Pooled[*TestConn]{Conn: &TestConn{setting: sBar}}

	require.True(t, wl.tryReturnConn(conn))
	assert.Same(t, conn, <-bar.Value.conn)
	assert.Equal(t, uint32(1), foo.Value.age)
	assert.Zero(t, wl.maybeStarvingCount(), "foo has already aged")

	foo.Value.age = 9
	enqueueSnakeWaiter(&wl, waiter[*TestConn]{setting: sBar, conn: make(chan *Pooled[*TestConn], 1)})

	require.True(t, wl.tryReturnConn(conn))
	assert.Same(t, conn, <-foo.Value.conn)
}

func TestWaitlistSnakePreservesStarvationCount(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("ConnPool", testPoolConfig{})

	enqueueSnakeWaiter(&wl, waiter[*TestConn]{conn: make(chan *Pooled[*TestConn], 1), age: 1})
	enqueueSnakeWaiter(&wl, waiter[*TestConn]{conn: make(chan *Pooled[*TestConn], 1)})

	assert.Equal(t, 1, wl.maybeStarvingCount())
}

func TestWaitlistMovesQueuedRequestsBetweenLegacyAndSnake(t *testing.T) {
	config := newMutableTestPoolConfig(loadshed.ModeOff)
	wl := waitlist[*TestConn]{}
	wl.init("ConnPool", config)

	poolClose := make(chan struct{})
	errs := make(chan error, 3)
	for range 3 {
		go func() {
			_, err := wl.waitForConn(context.Background(), nil, poolClose, 0, "valve", loadshed.PriorityUndroppable, false)
			errs <- err
		}()
	}

	require.Eventually(t, func() bool {
		return wl.waiting() == 3
	}, time.Second, time.Millisecond)
	assert.Equal(t, 3, wl.queues.list.Len())
	assert.Zero(t, wl.queues.snake.Len())

	config.setMode(loadshed.ModeShadow)
	assert.Equal(t, 3, wl.maybeStarvingCount())
	assert.Zero(t, wl.queues.list.Len())
	assert.Equal(t, 3, wl.queues.snake.Len())

	config.setMode(loadshed.ModeOff)
	assert.Equal(t, 3, wl.maybeStarvingCount())
	assert.Equal(t, 3, wl.queues.list.Len())
	assert.Zero(t, wl.queues.snake.Len())

	config.setMode(loadshed.ModeEnabled)
	assert.Equal(t, 3, wl.maybeStarvingCount())
	assert.Zero(t, wl.queues.list.Len())
	assert.Equal(t, 3, wl.queues.snake.Len())

	config.setMode(loadshed.ModeOff)
	assert.Equal(t, 3, wl.maybeStarvingCount())
	assert.Equal(t, 3, wl.queues.list.Len())
	assert.Zero(t, wl.queues.snake.Len())

	close(poolClose)
	for range 3 {
		assert.ErrorIs(t, <-errs, ErrConnPoolClosed)
	}
}

func TestWaitlistTransitionDoesNotHideWaitersFromConnectionHandoff(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("", nil)

	assert.False(t, wl.shouldTryReturnConn())

	wl.queues.transitioning.Store(true)
	assert.True(t, wl.shouldTryReturnConn())

	wl.queues.transitioning.Store(false)
	assert.False(t, wl.shouldTryReturnConn())
}

func TestWaitlistCancellationAcrossQueueTransitions(t *testing.T) {
	tests := []struct {
		name string
		from loadshed.Mode
		to   loadshed.Mode
	}{
		{name: "legacy to snake", from: loadshed.ModeOff, to: loadshed.ModeShadow},
		{name: "snake to legacy", from: loadshed.ModeShadow, to: loadshed.ModeOff},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := newMutableTestPoolConfig(tt.from)
			wl := waitlist[*TestConn]{}
			wl.init("ConnPool", config)

			ctx, cancel := context.WithCancel(context.Background())
			errs := make(chan error, 1)
			go func() {
				_, err := wl.waitForConn(ctx, nil, make(chan struct{}), 0, "valve", loadshed.PriorityUndroppable, false)
				errs <- err
			}()

			require.Eventually(t, func() bool {
				return wl.waiting() == 1
			}, time.Second, time.Millisecond)

			config.setMode(tt.to)
			cancel()
			assert.ErrorIs(t, <-errs, context.Canceled)
			assert.Zero(t, wl.waiting())
			assert.Zero(t, wl.queues.list.Len())
			assert.Zero(t, wl.queues.snake.Len())
		})
	}
}

func TestWaitlistWaiterCapDryRun(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("", nil)

	capReachedCount := atomic.Int32{}
	wl.onWaiterCapReached = func() {
		capReachedCount.Add(1)
	}

	poolClose := make(chan struct{})

	const maxWaiters = 3

	errs := make(chan error, maxWaiters)
	for i := 1; i <= maxWaiters; i++ {
		go func() {
			_, err := wl.waitForConn(context.Background(), nil, poolClose, maxWaiters, "", loadshed.PriorityUndroppable, true)
			errs <- err
		}()

		assert.Eventually(t, func() bool {
			return wl.waiting() == i
		}, time.Second, 5*time.Millisecond)
	}

	// In dryrun mode, exceeding the cap fires the callback but still lets the waiter through
	go func() {
		_, err := wl.waitForConn(context.Background(), nil, poolClose, maxWaiters, "", loadshed.PriorityUndroppable, true)
		errs <- err
	}()

	assert.Eventually(t, func() bool {
		return wl.waiting() == maxWaiters+1
	}, time.Second, 5*time.Millisecond)

	assert.Equal(t, int32(1), capReachedCount.Load())

	close(poolClose)

	for i := 0; i < maxWaiters+1; i++ {
		assert.NotErrorIs(t, <-errs, ErrPoolWaiterCapReached)
	}
}
