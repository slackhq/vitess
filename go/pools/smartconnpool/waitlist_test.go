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

	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

type testPoolConfig struct{}

func (testPoolConfig) LoadshedConfig(string) (func() bool, func() time.Duration, func() time.Duration) {
	return func() bool { return true },
		func() time.Duration { return time.Millisecond },
		func() time.Duration { return time.Millisecond }
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
			_, err := wait.waitForConn(ctx, nil, poolClose, 0, "", loadshed.PriorityUndroppable)

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
			_, err := wl.waitForConn(context.Background(), nil, poolClose, maxWaiters, "valve", loadshed.PriorityUndroppable)
			errs <- err
		}()

		assert.Eventually(t, func() bool {
			return wl.waiting() == i
		}, time.Second, 5*time.Millisecond)
	}

	_, err := wl.waitForConn(context.Background(), nil, poolClose, maxWaiters, "valve", loadshed.PriorityUndroppable)
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
			_, err := wl.waitForConn(context.Background(), nil, poolClose, 0, "", 0)
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

func TestWaitlistPreservesSettingAffinityAndAging(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("", nil)

	foo := &waiter[*TestConn]{setting: sFoo, conn: make(chan *Pooled[*TestConn], 1)}
	wl.snake.Enqueue(foo, "", loadshed.PriorityUndroppable)
	bar := &waiter[*TestConn]{setting: sBar, conn: make(chan *Pooled[*TestConn], 1)}
	wl.snake.Enqueue(bar, "", loadshed.PriorityUndroppable)
	conn := &Pooled[*TestConn]{Conn: &TestConn{setting: sBar}}

	require.True(t, wl.tryReturnConn(conn))
	assert.Same(t, conn, <-bar.conn)
	assert.Equal(t, uint32(1), foo.age)
	assert.Equal(t, 0, wl.maybeStarvingCount())

	foo.age = 9
	bar = &waiter[*TestConn]{setting: sBar, conn: make(chan *Pooled[*TestConn], 1)}
	wl.snake.Enqueue(bar, "", loadshed.PriorityUndroppable)

	require.True(t, wl.tryReturnConn(conn))
	assert.Same(t, conn, <-foo.conn)
}
