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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/list"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

type testPoolConfig struct {
	minDropDelay time.Duration
}

func (c testPoolConfig) LoadshedConfig(string) loadshed.SnakeConfig {
	minDropDelay := c.minDropDelay
	if minDropDelay == 0 {
		minDropDelay = time.Millisecond
	}
	return loadshed.SnakeConfig{
		Mode: func() loadshed.Mode { return loadshed.ModeEnabled },
		CoDel: loadshed.CoDelConfig{
			IntervalNs:        func() int64 { return time.Millisecond.Nanoseconds() },
			InitialIntervalNs: func() int64 { return time.Millisecond.Nanoseconds() },
			TargetNs:          func() int64 { return time.Millisecond.Nanoseconds() },
			InitialTargetNs:   func() int64 { return time.Millisecond.Nanoseconds() },
			Exponent:          func() float64 { return 1 },
			MinDropDelayNs:    func() int64 { return minDropDelay.Nanoseconds() },
		},
	}
}

type mutableTestPoolConfig struct {
	mode atomic.Value
}

func newMutableTestPoolConfig(mode loadshed.Mode) *mutableTestPoolConfig {
	config := &mutableTestPoolConfig{}
	config.mode.Store(mode)
	return config
}

func (c *mutableTestPoolConfig) LoadshedConfig(string) loadshed.SnakeConfig {
	return loadshed.SnakeConfig{
		Mode: func() loadshed.Mode { return c.mode.Load().(loadshed.Mode) },
		CoDel: loadshed.CoDelConfig{
			IntervalNs:     func() int64 { return time.Second.Nanoseconds() },
			TargetNs:       func() int64 { return time.Second.Nanoseconds() },
			Exponent:       func() float64 { return 1 },
			MinDropDelayNs: func() int64 { return time.Second.Nanoseconds() },
		},
	}
}

func (c *mutableTestPoolConfig) setMode(mode loadshed.Mode) {
	c.mode.Store(mode)
}

func enqueueSnakeWaiter(wl *waitlist[*TestConn], value waiter[*TestConn], valveID string) *list.Element[waiter[*TestConn]] {
	elem := &list.Element[waiter[*TestConn]]{Value: value}
	wl.snake.Enqueue(elem, valveID, loadshed.PriorityUndroppable)
	return elem
}

func TestSnakePriority(t *testing.T) {
	assert.Equal(t, float64(100), snakePriority(0))
	assert.Equal(t, float64(50), snakePriority(50))
	assert.Equal(t, float64(0), snakePriority(100))
	assert.Equal(t, loadshed.PriorityUndroppable, snakePriority(loadshed.PriorityUndroppable))
}

func TestWaitlistPoolCloseWithMultipleWaiters(t *testing.T) {
	wait := waitlist[*TestConn]{}
	wait.init("", nil)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	poolClose := make(chan struct{})
	const waiterCount = 2
	expireCount := atomic.Int32{}

	for range waiterCount {
		go func() {
			_, err := wait.waitForConn(ctx, nil, poolClose, 0, "", loadshed.PriorityUndroppable, false)

			if err != nil {
				expireCount.Add(1)
			}
		}()
	}

	close(poolClose)
	<-ctx.Done()

	assert.Eventually(t, func() bool {
		return expireCount.Load() == waiterCount
	}, 30*time.Second, 10*time.Millisecond)
}

func TestWaitlistOffUsesLegacyQueue(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("", nil)

	poolClose := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		_, err := wl.waitForConn(t.Context(), nil, poolClose, 0, "", 0, false)
		errs <- err
	}()

	require.Eventually(t, func() bool {
		return wl.waiting() == 1
	}, 30*time.Second, time.Millisecond)
	assert.Equal(t, 1, wl.list.Len())
	assert.Zero(t, wl.snake.Len())

	close(poolClose)
	assert.ErrorIs(t, <-errs, ErrConnPoolClosed)
}

func TestWaitlistValveGroupingControlsShedding(t *testing.T) {
	tests := []struct {
		name      string
		valveIDs  []string
		wantDrops int
	}{
		{name: "same valve", valveIDs: []string{"a", "a", "a", "a", "a", "a"}},
		{name: "distinct valves", valveIDs: []string{"a", "b", "c", "d", "e", "f"}, wantDrops: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wl := waitlist[*TestConn]{}
			wl.init("ConnPool", testPoolConfig{minDropDelay: time.Second})
			ctx, cancel := context.WithCancel(t.Context())
			results := make(chan error, len(tt.valveIDs))
			for _, valveID := range tt.valveIDs {
				go func() {
					_, err := wl.waitForConn(ctx, nil, make(chan struct{}), 0, valveID, 0, false)
					results <- err
				}()
			}
			require.Eventually(t, func() bool {
				return wl.waiting() == len(tt.valveIDs)
			}, time.Second, time.Millisecond)

			time.Sleep(2 * time.Millisecond)
			wl.runDropTimer()
			for range tt.wantDrops {
				assert.ErrorIs(t, <-results, ErrPoolLoadShed)
			}
			assert.Equal(t, int64(tt.wantDrops), wl.snake.ShedCount())

			cancel()
			for range len(tt.valveIDs) - tt.wantDrops {
				assert.ErrorIs(t, <-results, context.Canceled)
			}
			assert.Zero(t, wl.waiting())
		})
	}
}

func TestWaitlistWaiterCap(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("", nil)

	poolClose := make(chan struct{})
	const maxWaiters = 3

	errs := make(chan error, maxWaiters)
	for i := 1; i <= maxWaiters; i++ {
		go func() {
			_, err := wl.waitForConn(t.Context(), nil, poolClose, maxWaiters, "", loadshed.PriorityUndroppable, false)
			errs <- err
		}()

		assert.Eventually(t, func() bool {
			return wl.waiting() == i
		}, 30*time.Second, 5*time.Millisecond)
	}

	_, err := wl.waitForConn(t.Context(), nil, poolClose, maxWaiters, "", loadshed.PriorityUndroppable, false)
	assert.ErrorIs(t, err, ErrPoolWaiterCapReached)
	assert.Equal(t, maxWaiters, wl.waiting())

	close(poolClose)
	for range maxWaiters {
		assert.NotErrorIs(t, <-errs, ErrPoolWaiterCapReached)
	}
}

func TestWaitlistLegacyPreservesSettingAffinityAndAging(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("", nil)

	foo := &list.Element[waiter[*TestConn]]{
		Value: waiter[*TestConn]{setting: sFoo, conn: make(chan *Pooled[*TestConn], 1)},
	}
	wl.list.PushBackValue(foo)
	bar := &list.Element[waiter[*TestConn]]{
		Value: waiter[*TestConn]{setting: sBar, conn: make(chan *Pooled[*TestConn], 1)},
	}
	wl.list.PushBackValue(bar)
	conn := &Pooled[*TestConn]{Conn: &TestConn{setting: sBar}}

	require.True(t, wl.tryReturnConn(conn))
	assert.Same(t, conn, <-bar.Value.conn)
	assert.Equal(t, uint32(1), foo.Value.age)
	assert.Zero(t, wl.maybeStarvingCount())

	foo.Value.age = 9
	bar = &list.Element[waiter[*TestConn]]{
		Value: waiter[*TestConn]{setting: sBar, conn: make(chan *Pooled[*TestConn], 1)},
	}
	wl.list.PushBackValue(bar)

	require.True(t, wl.tryReturnConn(conn))
	assert.Same(t, conn, <-foo.Value.conn)
}

func TestWaitlistSnakePreservesSettingAffinityAndAging(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("ConnPool", testPoolConfig{})

	foo := enqueueSnakeWaiter(&wl, waiter[*TestConn]{setting: sFoo, conn: make(chan *Pooled[*TestConn], 1)}, "")
	bar := enqueueSnakeWaiter(&wl, waiter[*TestConn]{setting: sBar, conn: make(chan *Pooled[*TestConn], 1)}, "")
	conn := &Pooled[*TestConn]{Conn: &TestConn{setting: sBar}}

	require.True(t, wl.tryReturnConn(conn))
	assert.Same(t, conn, <-bar.Value.conn)
	assert.Equal(t, uint32(1), foo.Value.age)
	assert.Zero(t, wl.maybeStarvingCount())

	foo.Value.age = 9
	enqueueSnakeWaiter(&wl, waiter[*TestConn]{setting: sBar, conn: make(chan *Pooled[*TestConn], 1)}, "")

	require.True(t, wl.tryReturnConn(conn))
	assert.Same(t, conn, <-foo.Value.conn)
}

func TestWaitlistSnakePreservesStarvationCount(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("ConnPool", testPoolConfig{})

	enqueueSnakeWaiter(&wl, waiter[*TestConn]{conn: make(chan *Pooled[*TestConn], 1), age: 1}, "")
	enqueueSnakeWaiter(&wl, waiter[*TestConn]{conn: make(chan *Pooled[*TestConn], 1)}, "")

	assert.Equal(t, 1, wl.maybeStarvingCount())
}

func TestWaitlistValvePromotionOrder(t *testing.T) {
	type namedWaiter struct {
		name string
		elem *list.Element[waiter[*TestConn]]
	}

	wl := waitlist[*TestConn]{}
	wl.init("ConnPool", testPoolConfig{minDropDelay: time.Second})
	waiters := make([]namedWaiter, 0, 5)
	for _, item := range []struct {
		name    string
		valveID string
	}{
		{name: "a1", valveID: "a"},
		{name: "a2", valveID: "a"},
		{name: "a3", valveID: "a"},
		{name: "b1", valveID: "b"},
		{name: "c1", valveID: "c"},
	} {
		elem := enqueueSnakeWaiter(&wl, waiter[*TestConn]{conn: make(chan *Pooled[*TestConn], 1)}, item.valveID)
		waiters = append(waiters, namedWaiter{name: item.name, elem: elem})
	}

	for _, want := range []string{"a1", "b1", "c1", "a2", "a3"} {
		conn := &Pooled[*TestConn]{Conn: &TestConn{}}
		require.True(t, wl.tryReturnConn(conn))
		gotName := ""
		for _, candidate := range waiters {
			select {
			case got := <-candidate.elem.Value.conn:
				gotName = candidate.name
				assert.Same(t, conn, got)
			default:
			}
		}
		assert.Equal(t, want, gotName)
	}
	assert.Zero(t, wl.waiting())
}

func TestWaitlistValveCancellationPromotesNext(t *testing.T) {
	type result struct {
		id   int
		conn *Pooled[*TestConn]
		err  error
	}

	for _, cancelIndex := range []int{0, 1} {
		t.Run([]string{"representative", "pending"}[cancelIndex], func(t *testing.T) {
			wl := waitlist[*TestConn]{}
			wl.init("ConnPool", newMutableTestPoolConfig(loadshed.ModeShadow))
			results := make(chan result, 3)
			cancels := make([]context.CancelFunc, 3)
			for i := range 3 {
				ctx, cancel := context.WithCancel(t.Context())
				cancels[i] = cancel
				go func() {
					conn, err := wl.waitForConn(ctx, nil, make(chan struct{}), 0, "valve", 0, false)
					results <- result{id: i, conn: conn, err: err}
				}()
				require.Eventually(t, func() bool {
					return wl.waiting() == i+1
				}, time.Second, time.Millisecond)
			}

			cancels[cancelIndex]()
			cancelled := <-results
			assert.Equal(t, cancelIndex, cancelled.id)
			assert.ErrorIs(t, cancelled.err, context.Canceled)

			for _, want := range [][]int{{1, 2}, {0, 2}}[cancelIndex] {
				conn := &Pooled[*TestConn]{Conn: &TestConn{}}
				require.True(t, wl.tryReturnConn(conn))
				granted := <-results
				assert.Equal(t, want, granted.id)
				assert.Same(t, conn, granted.conn)
				assert.NoError(t, granted.err)
			}
			assert.Zero(t, wl.waiting())
		})
	}
}

func TestWaitlistShedsQueuedRequests(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("ConnPool", testPoolConfig{})

	poolClose := make(chan struct{})
	t.Cleanup(func() {
		close(poolClose)
	})

	errs := make(chan error, 6)
	var waiting atomic.Int32
	wl.onWait = func() {
		waiting.Add(1)
	}
	for range 6 {
		go func() {
			_, err := wl.waitForConn(t.Context(), nil, poolClose, 0, "", 0, false)
			errs <- err
		}()
	}

	require.Eventually(t, func() bool {
		return waiting.Load() == 6
	}, 30*time.Second, time.Millisecond)
	select {
	case err := <-errs:
		require.ErrorIs(t, err, ErrPoolLoadShed)
	case <-time.After(30 * time.Second):
		require.Fail(t, "timed out waiting for Snake to shed a waiter")
	}
}

func TestWaitlistShedsLowestPrioritiesAndPreservesUndroppable(t *testing.T) {
	type result struct {
		priority float64
		err      error
	}

	wl := waitlist[*TestConn]{}
	wl.init("ConnPool", testPoolConfig{minDropDelay: time.Second})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	results := make(chan result, 7)
	for _, priority := range []float64{0, 20, 40, 60, 80, 100, loadshed.PriorityUndroppable} {
		go func() {
			_, err := wl.waitForConn(ctx, nil, make(chan struct{}), 0, "", priority, false)
			results <- result{priority: priority, err: err}
		}()
	}
	require.Eventually(t, func() bool {
		return wl.waiting() == 7
	}, time.Second, time.Millisecond)

	time.Sleep(2 * time.Millisecond)
	wl.runDropTimer()

	dropped := make([]float64, 0, 2)
	for range 2 {
		result := <-results
		require.ErrorIs(t, result.err, ErrPoolLoadShed)
		dropped = append(dropped, result.priority)
	}
	assert.ElementsMatch(t, []float64{80, 100}, dropped)

	cancel()
	for range 5 {
		result := <-results
		assert.ErrorIs(t, result.err, context.Canceled)
	}
	assert.Zero(t, wl.waiting())
}

func TestWaitlistDropTimerAndCancellationRace(t *testing.T) {
	for range 50 {
		wl := waitlist[*TestConn]{}
		wl.init("ConnPool", testPoolConfig{minDropDelay: time.Second})

		ctx, cancel := context.WithCancel(t.Context())
		errs := make(chan error, 8)
		for range 8 {
			go func() {
				_, err := wl.waitForConn(ctx, nil, make(chan struct{}), 0, "", loadshed.PriorityUndroppable, false)
				errs <- err
			}()
		}
		require.Eventually(t, func() bool {
			return wl.waiting() == 8
		}, time.Second, time.Millisecond)

		time.Sleep(2 * time.Millisecond)
		var race sync.WaitGroup
		race.Add(2)
		go func() {
			defer race.Done()
			cancel()
		}()
		go func() {
			defer race.Done()
			wl.runDropTimer()
		}()
		race.Wait()

		for range 8 {
			err := <-errs
			assert.True(t, errors.Is(err, context.Canceled) || errors.Is(err, ErrPoolLoadShed), "unexpected error: %v", err)
		}
		assert.Zero(t, wl.waiting())
	}
}

func TestWaitlistMovesQueuedRequestsBetweenLegacyAndSnake(t *testing.T) {
	config := newMutableTestPoolConfig(loadshed.ModeOff)
	wl := waitlist[*TestConn]{}
	wl.init("ConnPool", config)

	poolClose := make(chan struct{})
	errs := make(chan error, 3)
	for range 3 {
		go func() {
			_, err := wl.waitForConn(t.Context(), nil, poolClose, 0, "", loadshed.PriorityUndroppable, false)
			errs <- err
		}()
	}

	require.Eventually(t, func() bool {
		return wl.waiting() == 3
	}, 30*time.Second, time.Millisecond)
	assert.Equal(t, 3, wl.list.Len())
	assert.Zero(t, wl.snake.Len())

	config.setMode(loadshed.ModeShadow)
	assert.Equal(t, 3, wl.maybeStarvingCount())
	assert.Zero(t, wl.list.Len())
	assert.Equal(t, 3, wl.snake.Len())

	config.setMode(loadshed.ModeOff)
	assert.Equal(t, 3, wl.maybeStarvingCount())
	assert.Equal(t, 3, wl.list.Len())
	assert.Zero(t, wl.snake.Len())

	config.setMode(loadshed.ModeEnabled)
	assert.Equal(t, 3, wl.maybeStarvingCount())
	assert.Zero(t, wl.list.Len())
	assert.Equal(t, 3, wl.snake.Len())

	config.setMode(loadshed.ModeOff)
	assert.Equal(t, 3, wl.maybeStarvingCount())
	assert.Equal(t, 3, wl.list.Len())
	assert.Zero(t, wl.snake.Len())

	close(poolClose)
	for range 3 {
		assert.ErrorIs(t, <-errs, ErrConnPoolClosed)
	}
}

func TestWaitlistTransitionDoesNotHideWaitersFromConnectionHandoff(t *testing.T) {
	wl := waitlist[*TestConn]{}
	wl.init("", nil)

	assert.False(t, wl.shouldTryReturnConn())
	wl.transitioning.Store(true)
	assert.True(t, wl.shouldTryReturnConn())
	wl.transitioning.Store(false)
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

			ctx, cancel := context.WithCancel(t.Context())
			errs := make(chan error, 1)
			go func() {
				_, err := wl.waitForConn(ctx, nil, make(chan struct{}), 0, "", loadshed.PriorityUndroppable, false)
				errs <- err
			}()

			require.Eventually(t, func() bool {
				return wl.waiting() == 1
			}, 30*time.Second, time.Millisecond)

			config.setMode(tt.to)
			cancel()
			assert.ErrorIs(t, <-errs, context.Canceled)
			assert.Zero(t, wl.waiting())
			assert.Zero(t, wl.list.Len())
			assert.Zero(t, wl.snake.Len())
		})
	}
}

func TestWaitlistSnakeCancelVsConnectionHandoff(t *testing.T) {
	type waitResult struct {
		conn *Pooled[*TestConn]
		err  error
	}

	for range 1000 {
		wl := waitlist[*TestConn]{}
		wl.init("ConnPool", newMutableTestPoolConfig(loadshed.ModeShadow))

		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan waitResult, 1)
		go func() {
			conn, err := wl.waitForConn(ctx, nil, make(chan struct{}), 0, "", loadshed.PriorityUndroppable, false)
			result <- waitResult{conn: conn, err: err}
		}()

		require.Eventually(t, func() bool {
			return wl.waiting() == 1
		}, time.Second, time.Millisecond)

		conn := &Pooled[*TestConn]{Conn: &TestConn{}}
		start := make(chan struct{})
		handoff := make(chan bool, 1)
		cancelled := make(chan struct{})
		go func() {
			<-start
			handoff <- wl.tryReturnConn(conn)
		}()
		go func() {
			<-start
			cancel()
			close(cancelled)
		}()
		close(start)

		<-cancelled
		handedOff := <-handoff
		got := <-result
		if handedOff {
			assert.Same(t, conn, got.conn)
			assert.NoError(t, got.err)
		} else {
			assert.Nil(t, got.conn)
			assert.ErrorIs(t, got.err, context.Canceled)
		}
		assert.Zero(t, wl.waiting())
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

	errs := make(chan error, maxWaiters+1)
	for i := 1; i <= maxWaiters; i++ {
		go func() {
			_, err := wl.waitForConn(t.Context(), nil, poolClose, maxWaiters, "", loadshed.PriorityUndroppable, true)
			errs <- err
		}()

		assert.Eventually(t, func() bool {
			return wl.waiting() == i
		}, 30*time.Second, 5*time.Millisecond)
	}

	go func() {
		_, err := wl.waitForConn(t.Context(), nil, poolClose, maxWaiters, "", loadshed.PriorityUndroppable, true)
		errs <- err
	}()

	assert.Eventually(t, func() bool {
		return wl.waiting() == maxWaiters+1
	}, 30*time.Second, 5*time.Millisecond)
	assert.Equal(t, int32(1), capReachedCount.Load())

	close(poolClose)
	for range maxWaiters + 1 {
		assert.NotErrorIs(t, <-errs, ErrPoolWaiterCapReached)
	}
}
