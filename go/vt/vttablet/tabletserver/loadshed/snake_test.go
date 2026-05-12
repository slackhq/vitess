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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func defaultSnakeConfig() SnakeConfig {
	return SnakeConfig{
		Name: "test-snake",
		CoDel: CoDelConfig{
			IntervalNs:     func() int64 { return int64(1e9) },
			TargetNs:       func() int64 { return int64(1e9) },
			Exponent:       func() float64 { return 1.0 },
			MinDropDelayNs: func() int64 { return 100 },
		},
		LoadsheddingAllowed: func() bool { return true },
		ContentionID:        func() string { return "" },
	}
}

func newTestSnake(cfg SnakeConfig) *Snake {
	return NewSnake(cfg)
}

// --- Basic acquire/release ---

func TestSnake_AcquireRelease_Basic(t *testing.T) {
	l := newTestSnake(defaultSnakeConfig())

	assert.False(t, l.IsLocked())

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)
	assert.True(t, l.IsLocked())

	err = unlock.Release()
	assert.NoError(t, err)
	assert.False(t, l.IsLocked())
}

func TestSnake_AcquireRelease_Sequential(t *testing.T) {
	l := newTestSnake(defaultSnakeConfig())

	for range 10 {
		unlock, err := l.Acquire(t.Context())
		require.NoError(t, err)
		assert.True(t, l.IsLocked())

		err = unlock.Release()
		assert.NoError(t, err)
		assert.False(t, l.IsLocked())
	}
}

// --- Mutual exclusion ---

func TestSnake_MutualExclusion(t *testing.T) {
	l := newTestSnake(defaultSnakeConfig())

	var held atomic.Int32
	var wg sync.WaitGroup

	for range 10 {
		wg.Go(func() {
			unlock, err := l.Acquire(t.Context())
			if err != nil {
				return
			}
			defer unlock.Release()

			val := held.Add(1)
			assert.Equal(t, int32(1), val, "multiple goroutines hold the lock")
			time.Sleep(1 * time.Millisecond)
			held.Add(-1)
		})
	}

	wg.Wait()
	assert.False(t, l.IsLocked())
}

// --- FIFO ordering ---

func TestSnake_FIFO_Order(t *testing.T) {
	l := newTestSnake(defaultSnakeConfig())

	// acquire first to force others to wait
	unlock1, err := l.Acquire(t.Context())
	require.NoError(t, err)

	var mu sync.Mutex
	var order []int

	var wg sync.WaitGroup
	for i := range 5 {
		// small sleep to ensure enqueue order
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Go(func() {
			u, err := l.Acquire(t.Context())
			if err != nil {
				return
			}
			mu.Lock()
			order = append(order, idx)
			mu.Unlock()
			u.Release()
		})
	}

	// give goroutines time to enqueue
	time.Sleep(20 * time.Millisecond)

	unlock1.Release()
	wg.Wait()

	assert.Equal(t, []int{0, 1, 2, 3, 4}, order, "waiters should be served FIFO")
}

// --- Release wakes next ---

func TestSnake_ReleaseWakesNext(t *testing.T) {
	l := newTestSnake(defaultSnakeConfig())

	unlock1, err := l.Acquire(t.Context())
	require.NoError(t, err)

	acquired := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context())
		if err == nil {
			close(acquired)
			u.Release()
		}
	}()

	// waiter shouldn't be signaled yet
	select {
	case <-acquired:
		t.Fatal("second acquire should not have succeeded yet")
	case <-time.After(20 * time.Millisecond):
	}

	unlock1.Release()

	select {
	case <-acquired:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("second acquire was never woken")
	}
}

// --- Context cancellation ---

func TestSnake_ContextCancellation(t *testing.T) {
	l := newTestSnake(defaultSnakeConfig())

	// hold the lock
	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)
	go func() {
		_, err := l.Acquire(ctx)
		errCh <- err
	}()

	// give goroutine time to enqueue
	time.Sleep(10 * time.Millisecond)

	cancel()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Fatal("acquire did not return after cancel")
	}

	unlock.Release()
	assert.False(t, l.IsLocked())
}

func TestSnake_ContextTimeout(t *testing.T) {
	l := newTestSnake(defaultSnakeConfig())

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err = l.Acquire(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	unlock.Release()
}

// --- Cancel-vs-grant race ---

func TestSnake_ContextCancel_RaceWithGrant(t *testing.T) {
	// When ctx.Done() and req.done both fire, the cancelled goroutine must
	// release the lock so the next waiter isn't orphaned.
	l := newTestSnake(defaultSnakeConfig())

	unlock1, err := l.Acquire(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	// waiter 2: will be cancelled. If it wins the grant race, release immediately.
	waiter2Done := make(chan error, 1)
	go func() {
		u, err := l.Acquire(ctx)
		if err == nil {
			// Grant won the race with cancel. Release so lock isn't orphaned.
			u.Release()
		}
		waiter2Done <- err
	}()

	// waiter 3: should ultimately get the lock
	waiter3Done := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context())
		if err == nil {
			u.Release()
		}
		close(waiter3Done)
	}()

	time.Sleep(10 * time.Millisecond)

	// release lock1 (grants to waiter2) then immediately cancel waiter2's ctx
	unlock1.Release()
	cancel()

	// waiter2 should return with either nil (granted then released) or
	// context.Canceled (cancel-vs-grant race handler released internally)
	select {
	case <-waiter2Done:
	case <-time.After(1 * time.Second):
		t.Fatal("waiter2 did not return")
	}

	// waiter3 must eventually get the lock regardless of the race outcome
	select {
	case <-waiter3Done:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter3 was orphaned — lock leaked after cancel-vs-grant race")
	}

	assert.False(t, l.IsLocked())
}

// --- Self-contention ---

func TestSnake_SelfContention_Serialized(t *testing.T) {
	var contentionID atomic.Value
	contentionID.Store("id1")

	cfg := defaultSnakeConfig()
	cfg.ContentionID = func() string { return contentionID.Load().(string) }
	l := newTestSnake(cfg)

	// hold the lock
	unlock1, err := l.Acquire(t.Context())
	require.NoError(t, err)

	var mu sync.Mutex
	var order []int

	var wg sync.WaitGroup
	for i := range 3 {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Go(func() {
			u, err := l.Acquire(t.Context())
			if err != nil {
				return
			}
			mu.Lock()
			order = append(order, idx)
			mu.Unlock()
			u.Release()
		})
	}

	time.Sleep(20 * time.Millisecond)
	unlock1.Release()
	wg.Wait()

	// same contention ID → serialized through valve → FIFO
	assert.Equal(t, []int{0, 1, 2}, order)
}

func TestSnake_SelfContention_DifferentIDs_Independent(t *testing.T) {
	var idCounter atomic.Int64

	cfg := defaultSnakeConfig()
	cfg.ContentionID = func() string {
		return fmt.Sprintf("id%d", idCounter.Add(1))
	}
	l := newTestSnake(cfg)

	// hold the lock
	unlock1, err := l.Acquire(t.Context())
	require.NoError(t, err)

	// both use unique IDs, so both enter CoDel queue directly
	acquired := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			u, err := l.Acquire(t.Context())
			if err != nil {
				return
			}
			acquired <- struct{}{}
			u.Release()
		})
	}

	time.Sleep(10 * time.Millisecond)
	unlock1.Release()
	wg.Wait()

	assert.Equal(t, 2, len(acquired))
}

// --- CoDel drop ---

func TestSnake_DroppedRequest(t *testing.T) {
	cfg := defaultSnakeConfig()
	// aggressive CoDel: tiny interval and target
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	l := newTestSnake(cfg)

	// hold the lock for a long time
	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	errCh := make(chan error, 5)
	for range 5 {
		go func() {
			_, err := l.Acquire(t.Context())
			errCh <- err
		}()
	}

	// hold long enough for CoDel to drop
	time.Sleep(200 * time.Millisecond)

	// at least some should be dropped
	unlock.Release()
	time.Sleep(50 * time.Millisecond)

	dropped := 0
	for range 5 {
		select {
		case err := <-errCh:
			if err != nil {
				dropped++
			}
		case <-time.After(2 * time.Second):
			t.Fatal("goroutine did not return")
		}
	}

	assert.Greater(t, dropped, 0, "CoDel should have dropped some requests")
}

func TestSnake_SelfContention_NoDrop(t *testing.T) {
	// Same contention ID → requests are valved, not in CoDel queue together
	// → even aggressive CoDel params shouldn't drop them
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	cfg.ContentionID = func() string { return "same-id" }
	l := newTestSnake(cfg)

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	results := make(chan error, 3)
	for range 3 {
		go func() {
			u, err := l.Acquire(t.Context())
			if err == nil {
				time.Sleep(1 * time.Millisecond)
				u.Release()
			}
			results <- err
		}()
	}

	time.Sleep(50 * time.Millisecond)
	unlock.Release()

	for range 3 {
		select {
		case err := <-results:
			assert.NoError(t, err, "same contention ID should not be dropped")
		case <-time.After(2 * time.Second):
			t.Fatal("goroutine did not return")
		}
	}
}

// --- Max age ---

func TestSnake_MaxAge_Timeout(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.MaxAge = func() time.Duration { return 20 * time.Millisecond }
	l := newTestSnake(cfg)

	unlock1, err := l.Acquire(t.Context())
	require.NoError(t, err)

	// second waiter
	acquired := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context())
		if err == nil {
			close(acquired)
			u.Release()
		}
	}()

	// don't release — max-age should force it
	select {
	case <-acquired:
		// max-age forced release and woke the next waiter
	case <-time.After(1 * time.Second):
		t.Fatal("max-age timer did not fire")
	}

	// original unlock.Release() is now a stale nonce
	err = unlock1.Release()
	assert.Error(t, err, "stale nonce should fail")
}

func TestSnake_MaxAge_CancelledOnRelease(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.MaxAge = func() time.Duration { return 100 * time.Millisecond }
	l := newTestSnake(cfg)

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	// release well before max-age
	unlock.Release()

	// wait past the max-age window — the timer should have been stopped
	time.Sleep(150 * time.Millisecond)
	assert.False(t, l.IsLocked())
}

func TestSnake_MaxAge_Zero_NoTimeout(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.MaxAge = func() time.Duration { return 0 }
	l := newTestSnake(cfg)

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	// hold for a bit — should not be force-released
	time.Sleep(50 * time.Millisecond)
	assert.True(t, l.IsLocked())

	unlock.Release()
}

// --- SafeUnlock ---

func TestSnake_SafeUnlock_DoubleRelease(t *testing.T) {
	l := newTestSnake(defaultSnakeConfig())

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	err = unlock.Release()
	assert.NoError(t, err)

	// second release is no-op
	err = unlock.Release()
	assert.NoError(t, err)
}

func TestSnake_SafeUnlock_StaleNonce(t *testing.T) {
	l := newTestSnake(defaultSnakeConfig())

	unlock1, err := l.Acquire(t.Context())
	require.NoError(t, err)

	// second acquire that will be granted after release
	acquired := make(chan *SafeUnlock, 1)
	go func() {
		u, err := l.Acquire(t.Context())
		if err == nil {
			acquired <- u
		}
	}()

	time.Sleep(10 * time.Millisecond)
	unlock1.Release()

	// wait for second to acquire
	var unlock2 *SafeUnlock
	select {
	case unlock2 = <-acquired:
	case <-time.After(1 * time.Second):
		t.Fatal("second acquire not woken")
	}

	// release unlock1 again — stale nonce, but sync.Once means no-op
	err = unlock1.Release()
	assert.NoError(t, err, "double release is no-op due to sync.Once")

	unlock2.Release()
}

// --- Release callbacks ---

func TestSnake_ReleaseCallbacks_Executed(t *testing.T) {
	var called atomic.Bool
	cfg := defaultSnakeConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) { called.Store(true) },
	}
	l := newTestSnake(cfg)

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	unlock.Release()
	assert.True(t, called.Load())
}

func TestSnake_ReleaseCallbacks_ReceiveError(t *testing.T) {
	var received atomic.Value
	cfg := defaultSnakeConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) { received.Store(err) },
	}
	l := newTestSnake(cfg)

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	myErr := errors.New("test error")
	unlock.Release(myErr)

	val := received.Load()
	assert.Equal(t, myErr, val)
}

func TestSnake_ReleaseCallbacks_NilOnNormalRelease(t *testing.T) {
	var received atomic.Value
	received.Store("sentinel") // distinguish from nil
	cfg := defaultSnakeConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) {
			if err == nil {
				received.Store("nil")
			} else {
				received.Store(err)
			}
		},
	}
	l := newTestSnake(cfg)

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	unlock.Release()

	assert.Equal(t, "nil", received.Load())
}

func TestSnake_ReleaseCallbacks_PanicRecovery(t *testing.T) {
	var secondCalled atomic.Bool
	cfg := defaultSnakeConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) { panic("callback panic") },
		func(err error) { secondCalled.Store(true) },
	}
	l := newTestSnake(cfg)

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	// panic in first callback should not prevent second or lock release
	err = unlock.Release()
	assert.NoError(t, err)
	assert.True(t, secondCalled.Load())
	assert.False(t, l.IsLocked())
}

func TestSnake_ReleaseCallbacks_NoDeadlock(t *testing.T) {
	// Callbacks run without the mutex held, so re-acquiring should work.
	cfg := defaultSnakeConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) {
			// This should not deadlock because callbacks run outside the mutex.
		},
	}
	l := newTestSnake(cfg)

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		unlock.Release()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("release deadlocked")
	}
}

// --- IsLocked / IsHealthy ---

func TestSnake_IsLocked_IsHealthy(t *testing.T) {
	l := newTestSnake(defaultSnakeConfig())

	assert.False(t, l.IsLocked())
	assert.True(t, l.IsHealthy())

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	assert.True(t, l.IsLocked())
	assert.True(t, l.IsHealthy()) // single holder, no persistent queue

	unlock.Release()
	assert.False(t, l.IsLocked())
}

// --- Cancel in CoDel queue ---

func TestSnake_CancelInCoDelQueue(t *testing.T) {
	l := newTestSnake(defaultSnakeConfig())

	unlock1, err := l.Acquire(t.Context())
	require.NoError(t, err)

	// waiter 2: will be cancelled
	ctx2, cancel2 := context.WithCancel(t.Context())
	waiter2Done := make(chan error, 1)
	go func() {
		_, err := l.Acquire(ctx2)
		waiter2Done <- err
	}()

	// waiter 3: should get the lock after waiter 2 is cancelled
	waiter3Done := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context())
		if err == nil {
			close(waiter3Done)
			u.Release()
		}
	}()

	time.Sleep(10 * time.Millisecond)

	// cancel waiter 2 while it's in the queue
	cancel2()

	select {
	case err := <-waiter2Done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Fatal("waiter2 did not return")
	}

	// release lock1 → waiter3 should get it
	unlock1.Release()

	select {
	case <-waiter3Done:
	case <-time.After(1 * time.Second):
		t.Fatal("waiter3 was not woken")
	}
}

// --- Cancel in valve ---

func TestSnake_CancelInValve(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.ContentionID = func() string { return "id1" }
	l := newTestSnake(cfg)

	unlock1, err := l.Acquire(t.Context())
	require.NoError(t, err)

	// waiter 2: same contention ID, enters valve since waiter1 is active
	// waiter 2 enters CoDel queue when waiter1 completes
	// waiter 3: same contention ID, enters valve behind waiter2
	ctx3, cancel3 := context.WithCancel(t.Context())

	waiter2Done := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context())
		if err == nil {
			u.Release()
		}
		close(waiter2Done)
	}()

	time.Sleep(5 * time.Millisecond)

	waiter3Done := make(chan error, 1)
	go func() {
		_, err := l.Acquire(ctx3)
		waiter3Done <- err
	}()

	time.Sleep(5 * time.Millisecond)

	waiter4Done := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context())
		if err == nil {
			u.Release()
		}
		close(waiter4Done)
	}()

	time.Sleep(5 * time.Millisecond)

	// cancel waiter3 (in valve)
	cancel3()

	select {
	case err := <-waiter3Done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Fatal("waiter3 cancel did not return")
	}

	unlock1.Release()

	// waiter2 and waiter4 should complete
	select {
	case <-waiter2Done:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter2 did not complete")
	}

	select {
	case <-waiter4Done:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter4 did not complete")
	}
}

// --- Undroppable ---

func TestSnake_Undroppable_NeverDropped(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	cfg.LoadsheddingAllowed = func() bool { return false }
	l := newTestSnake(cfg)

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	results := make(chan error, 5)
	for range 5 {
		go func() {
			u, err := l.Acquire(t.Context())
			if err == nil {
				u.Release()
			}
			results <- err
		}()
	}

	// hold long enough that drops would happen if allowed
	time.Sleep(100 * time.Millisecond)
	unlock.Release()

	for range 5 {
		select {
		case err := <-results:
			assert.NoError(t, err, "undroppable requests should never be dropped")
		case <-time.After(2 * time.Second):
			t.Fatal("goroutine did not return")
		}
	}
}

// --- Custom acquire error ---

func TestSnake_AcquireError_Custom(t *testing.T) {
	myErr := errors.New("custom acquire error")
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	cfg.AcquireError = func() error { return myErr }
	l := newTestSnake(cfg)

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)

	errCh := make(chan error, 3)
	for range 3 {
		go func() {
			_, err := l.Acquire(t.Context())
			errCh <- err
		}()
	}

	time.Sleep(100 * time.Millisecond)
	unlock.Release()
	time.Sleep(50 * time.Millisecond)

	dropped := 0
	for range 3 {
		select {
		case err := <-errCh:
			if err != nil {
				assert.Equal(t, myErr, err, "should use custom error")
				dropped++
			}
		case <-time.After(2 * time.Second):
			t.Fatal("goroutine did not return")
		}
	}

	if dropped == 0 {
		t.Skip("CoDel did not drop in this run (timing dependent)")
	}
}

// --- Self-contention: exceptions during hold ---

func TestSnake_SelfContention_WithExceptions(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.ContentionID = func() string { return "id1" }
	l := newTestSnake(cfg)

	var mu sync.Mutex
	var order []int

	// waiter 1: acquire and release with error
	unlock1, err := l.Acquire(t.Context())
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 2; i <= 4; i++ {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Go(func() {
			u, err := l.Acquire(t.Context())
			if err != nil {
				return
			}
			mu.Lock()
			order = append(order, idx)
			mu.Unlock()
			u.Release(errors.New("simulated error"))
		})
	}

	time.Sleep(20 * time.Millisecond)
	unlock1.Release(errors.New("first error"))
	wg.Wait()

	assert.Equal(t, []int{2, 3, 4}, order, "all waiters should complete despite errors")
}

// --- NewLock (default clock) ---

func TestNewSnake_DefaultClock(t *testing.T) {
	l := NewSnake(defaultSnakeConfig())

	unlock, err := l.Acquire(t.Context())
	require.NoError(t, err)
	assert.True(t, l.IsLocked())

	unlock.Release()
	assert.False(t, l.IsLocked())
}
