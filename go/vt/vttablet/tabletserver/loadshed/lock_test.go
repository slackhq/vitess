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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func defaultLockConfig() LockConfig {
	return LockConfig{
		Name: "test-lock",
		CoDel: CoDelConfig{
			IntervalNs:     func() int64 { return int64(1e9) },
			TargetNs:       func() int64 { return int64(1e9) },
			Exponent:       func() float64 { return 1.0 },
			MinDropDelayNs: func() int64 { return 100 },
		},
		LoadsheddingAllowed: func() bool { return true },
	}
}

func newTestLock(cfg LockConfig) *Lock {
	return NewLock(cfg)
}

// --- Basic acquire/release ---

func TestLock_AcquireRelease_Basic(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	assert.False(t, l.IsLocked())

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)
	assert.True(t, l.IsLocked())

	err = unlock.Release()
	assert.NoError(t, err)
	assert.False(t, l.IsLocked())
}

func TestLock_AcquireRelease_Sequential(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	for range 10 {
		unlock, err := l.Acquire(t.Context(), "")
		require.NoError(t, err)
		assert.True(t, l.IsLocked())

		err = unlock.Release()
		assert.NoError(t, err)
		assert.False(t, l.IsLocked())
	}
}

// --- Mutual exclusion ---

func TestLock_MutualExclusion(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	var held atomic.Int32
	var wg sync.WaitGroup

	for range 10 {
		wg.Go(func() {
			unlock, err := l.Acquire(t.Context(), "")
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

func TestLock_FIFO_Order(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	// acquire first to force others to wait
	unlock1, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	var mu sync.Mutex
	var order []int

	var wg sync.WaitGroup
	for i := range 5 {
		// small sleep to ensure enqueue order
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Go(func() {
			u, err := l.Acquire(t.Context(), "")
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

func TestLock_ReleaseWakesNext(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	unlock1, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	acquired := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context(), "")
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

func TestLock_ContextCancellation(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	// hold the lock
	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)
	go func() {
		_, err := l.Acquire(ctx, "")
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

func TestLock_ContextTimeout(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err = l.Acquire(ctx, "")
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	unlock.Release()
}

// --- Cancel-vs-grant race ---

func TestLock_ContextCancel_RaceWithGrant(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	unlock1, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	waiter2Done := make(chan error, 1)
	go func() {
		u, err := l.Acquire(ctx, "")
		if err == nil {
			u.Release()
		}
		waiter2Done <- err
	}()

	waiter3Done := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context(), "")
		if err == nil {
			u.Release()
		}
		close(waiter3Done)
	}()

	time.Sleep(10 * time.Millisecond)

	unlock1.Release()
	cancel()

	select {
	case <-waiter2Done:
	case <-time.After(1 * time.Second):
		t.Fatal("waiter2 did not return")
	}

	select {
	case <-waiter3Done:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter3 was orphaned — lock leaked after cancel-vs-grant race")
	}

	assert.False(t, l.IsLocked())
}

// --- Self-contention ---

func TestLock_SelfContention_Serialized(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	// hold the lock
	unlock1, err := l.Acquire(t.Context(), "id1")
	require.NoError(t, err)

	var mu sync.Mutex
	var order []int

	var wg sync.WaitGroup
	for i := range 3 {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Go(func() {
			u, err := l.Acquire(t.Context(), "id1")
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

	// same valve ID → serialized through valve → FIFO
	assert.Equal(t, []int{0, 1, 2}, order)
}

func TestLock_SelfContention_DifferentIDs_Independent(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	// hold the lock
	unlock1, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	// both use unique IDs, so both enter CoDel queue directly
	acquired := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		valveID := string(rune('a' + i))
		wg.Go(func() {
			u, err := l.Acquire(t.Context(), valveID)
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

func TestLock_DroppedRequest(t *testing.T) {
	cfg := defaultLockConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	l := newTestLock(cfg)

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	errCh := make(chan error, 5)
	for range 5 {
		go func() {
			_, err := l.Acquire(t.Context(), "")
			errCh <- err
		}()
	}

	time.Sleep(200 * time.Millisecond)

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

func TestLock_SelfContention_NoDrop(t *testing.T) {
	cfg := defaultLockConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	l := newTestLock(cfg)

	unlock, err := l.Acquire(t.Context(), "same-id")
	require.NoError(t, err)

	results := make(chan error, 3)
	for range 3 {
		go func() {
			u, err := l.Acquire(t.Context(), "same-id")
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
			assert.NoError(t, err, "same valve ID should not be dropped")
		case <-time.After(2 * time.Second):
			t.Fatal("goroutine did not return")
		}
	}
}

// --- Max age ---

func TestLock_MaxAge_Timeout(t *testing.T) {
	cfg := defaultLockConfig()
	cfg.MaxAge = func() time.Duration { return 20 * time.Millisecond }
	l := newTestLock(cfg)

	unlock1, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	acquired := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context(), "")
		if err == nil {
			close(acquired)
			u.Release()
		}
	}()

	select {
	case <-acquired:
	case <-time.After(1 * time.Second):
		t.Fatal("max-age timer did not fire")
	}

	err = unlock1.Release()
	assert.Error(t, err, "stale nonce should fail")
}

func TestLock_MaxAge_CancelledOnRelease(t *testing.T) {
	cfg := defaultLockConfig()
	cfg.MaxAge = func() time.Duration { return 100 * time.Millisecond }
	l := newTestLock(cfg)

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	unlock.Release()

	time.Sleep(150 * time.Millisecond)
	assert.False(t, l.IsLocked())
}

func TestLock_MaxAge_Zero_NoTimeout(t *testing.T) {
	cfg := defaultLockConfig()
	cfg.MaxAge = func() time.Duration { return 0 }
	l := newTestLock(cfg)

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	assert.True(t, l.IsLocked())

	unlock.Release()
}

// --- SafeUnlock ---

func TestLock_SafeUnlock_DoubleRelease(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	err = unlock.Release()
	assert.NoError(t, err)

	err = unlock.Release()
	assert.NoError(t, err)
}

func TestLock_SafeUnlock_StaleNonce(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	unlock1, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	acquired := make(chan *SafeUnlock, 1)
	go func() {
		u, err := l.Acquire(t.Context(), "")
		if err == nil {
			acquired <- u
		}
	}()

	time.Sleep(10 * time.Millisecond)
	unlock1.Release()

	var unlock2 *SafeUnlock
	select {
	case unlock2 = <-acquired:
	case <-time.After(1 * time.Second):
		t.Fatal("second acquire not woken")
	}

	err = unlock1.Release()
	assert.NoError(t, err, "double release is no-op due to sync.Once")

	unlock2.Release()
}

// --- Release callbacks ---

func TestLock_ReleaseCallbacks_Executed(t *testing.T) {
	var called atomic.Bool
	cfg := defaultLockConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) { called.Store(true) },
	}
	l := newTestLock(cfg)

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	unlock.Release()
	assert.True(t, called.Load())
}

func TestLock_ReleaseCallbacks_ReceiveError(t *testing.T) {
	var received atomic.Value
	cfg := defaultLockConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) { received.Store(err) },
	}
	l := newTestLock(cfg)

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	myErr := errors.New("test error")
	unlock.Release(myErr)

	val := received.Load()
	assert.Equal(t, myErr, val)
}

func TestLock_ReleaseCallbacks_NilOnNormalRelease(t *testing.T) {
	var received atomic.Value
	received.Store("sentinel")
	cfg := defaultLockConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) {
			if err == nil {
				received.Store("nil")
			} else {
				received.Store(err)
			}
		},
	}
	l := newTestLock(cfg)

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	unlock.Release()

	assert.Equal(t, "nil", received.Load())
}

func TestLock_ReleaseCallbacks_PanicRecovery(t *testing.T) {
	var secondCalled atomic.Bool
	cfg := defaultLockConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) { panic("callback panic") },
		func(err error) { secondCalled.Store(true) },
	}
	l := newTestLock(cfg)

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	err = unlock.Release()
	assert.NoError(t, err)
	assert.True(t, secondCalled.Load())
	assert.False(t, l.IsLocked())
}

func TestLock_ReleaseCallbacks_NoDeadlock(t *testing.T) {
	cfg := defaultLockConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) {},
	}
	l := newTestLock(cfg)

	unlock, err := l.Acquire(t.Context(), "")
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

func TestLock_IsLocked_IsHealthy(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	assert.False(t, l.IsLocked())
	assert.True(t, l.IsHealthy())

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	assert.True(t, l.IsLocked())
	assert.True(t, l.IsHealthy())

	unlock.Release()
	assert.False(t, l.IsLocked())
}

// --- Cancel in CoDel queue ---

func TestLock_CancelInCoDelQueue(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	unlock1, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	ctx2, cancel2 := context.WithCancel(t.Context())
	waiter2Done := make(chan error, 1)
	go func() {
		_, err := l.Acquire(ctx2, "")
		waiter2Done <- err
	}()

	waiter3Done := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context(), "")
		if err == nil {
			close(waiter3Done)
			u.Release()
		}
	}()

	time.Sleep(10 * time.Millisecond)

	cancel2()

	select {
	case err := <-waiter2Done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Fatal("waiter2 did not return")
	}

	unlock1.Release()

	select {
	case <-waiter3Done:
	case <-time.After(1 * time.Second):
		t.Fatal("waiter3 was not woken")
	}
}

// --- Cancel in valve ---

func TestLock_CancelInValve(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	unlock1, err := l.Acquire(t.Context(), "id1")
	require.NoError(t, err)

	ctx3, cancel3 := context.WithCancel(t.Context())

	waiter2Done := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context(), "id1")
		if err == nil {
			u.Release()
		}
		close(waiter2Done)
	}()

	time.Sleep(5 * time.Millisecond)

	waiter3Done := make(chan error, 1)
	go func() {
		_, err := l.Acquire(ctx3, "id1")
		waiter3Done <- err
	}()

	time.Sleep(5 * time.Millisecond)

	waiter4Done := make(chan struct{})
	go func() {
		u, err := l.Acquire(t.Context(), "id1")
		if err == nil {
			u.Release()
		}
		close(waiter4Done)
	}()

	time.Sleep(5 * time.Millisecond)

	cancel3()

	select {
	case err := <-waiter3Done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Fatal("waiter3 cancel did not return")
	}

	unlock1.Release()

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

func TestLock_Undroppable_NeverDropped(t *testing.T) {
	cfg := defaultLockConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	cfg.LoadsheddingAllowed = func() bool { return false }
	l := newTestLock(cfg)

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	results := make(chan error, 5)
	for range 5 {
		go func() {
			u, err := l.Acquire(t.Context(), "")
			if err == nil {
				u.Release()
			}
			results <- err
		}()
	}

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

func TestLock_AcquireError_Custom(t *testing.T) {
	myErr := errors.New("custom acquire error")
	cfg := defaultLockConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	cfg.AcquireError = func() error { return myErr }
	l := newTestLock(cfg)

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)

	errCh := make(chan error, 3)
	for range 3 {
		go func() {
			_, err := l.Acquire(t.Context(), "")
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

func TestLock_SelfContention_WithExceptions(t *testing.T) {
	l := newTestLock(defaultLockConfig())

	var mu sync.Mutex
	var order []int

	unlock1, err := l.Acquire(t.Context(), "id1")
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 2; i <= 4; i++ {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Go(func() {
			u, err := l.Acquire(t.Context(), "id1")
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

func TestNewLock_DefaultClock(t *testing.T) {
	l := NewLock(defaultLockConfig())

	unlock, err := l.Acquire(t.Context(), "")
	require.NoError(t, err)
	assert.True(t, l.IsLocked())

	unlock.Release()
	assert.False(t, l.IsLocked())
}
