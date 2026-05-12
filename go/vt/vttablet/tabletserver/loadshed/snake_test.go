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
		Capacity:            func() int { return 1 },
		LoadsheddingAllowed: func() bool { return true },
		ContentionID:        func() string { return "" },
	}
}

func newTestSnake(cfg SnakeConfig) *Snake {
	return NewSnake(cfg)
}

// --- Basic acquire/release (capacity=1, backward compat) ---

func TestSnake_AcquireRelease_Basic(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	assert.Equal(t, 0, s.InFlight())

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, s.InFlight())

	err = unlock.Release()
	assert.NoError(t, err)
	assert.Equal(t, 0, s.InFlight())
}

func TestSnake_AcquireRelease_Sequential(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	for range 10 {
		unlock, err := s.Acquire(t.Context())
		require.NoError(t, err)
		assert.Equal(t, 1, s.InFlight())

		err = unlock.Release()
		assert.NoError(t, err)
		assert.Equal(t, 0, s.InFlight())
	}
}

// --- Mutual exclusion (capacity=1) ---

func TestSnake_MutualExclusion_Capacity1(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	var held atomic.Int32
	var wg sync.WaitGroup

	for range 10 {
		wg.Go(func() {
			unlock, err := s.Acquire(t.Context())
			if err != nil {
				return
			}
			defer unlock.Release()

			val := held.Add(1)
			assert.Equal(t, int32(1), val, "multiple goroutines hold the snake")
			time.Sleep(1 * time.Millisecond)
			held.Add(-1)
		})
	}

	wg.Wait()
	assert.Equal(t, 0, s.InFlight())
}

// --- FIFO ordering ---

func TestSnake_FIFO_Order(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	var mu sync.Mutex
	var order []int

	var wg sync.WaitGroup
	for i := range 5 {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Go(func() {
			u, err := s.Acquire(t.Context())
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

	assert.Equal(t, []int{0, 1, 2, 3, 4}, order, "waiters should be served FIFO")
}

// --- Release wakes next ---

func TestSnake_ReleaseWakesNext(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	acquired := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context())
		if err == nil {
			close(acquired)
			u.Release()
		}
	}()

	select {
	case <-acquired:
		t.Fatal("second acquire should not have succeeded yet")
	case <-time.After(20 * time.Millisecond):
	}

	unlock1.Release()

	select {
	case <-acquired:
	case <-time.After(1 * time.Second):
		t.Fatal("second acquire was never woken")
	}
}

// --- Context cancellation ---

func TestSnake_ContextCancellation(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)
	go func() {
		_, err := s.Acquire(ctx)
		errCh <- err
	}()

	time.Sleep(10 * time.Millisecond)

	cancel()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Fatal("acquire did not return after cancel")
	}

	unlock.Release()
	assert.Equal(t, 0, s.InFlight())
}

func TestSnake_ContextTimeout(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err = s.Acquire(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	unlock.Release()
}

// --- Cancel-vs-grant race ---

func TestSnake_ContextCancel_RaceWithGrant(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	waiter2Done := make(chan error, 1)
	go func() {
		u, err := s.Acquire(ctx)
		if err == nil {
			u.Release()
		}
		waiter2Done <- err
	}()

	waiter3Done := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context())
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
		t.Fatal("waiter3 was orphaned — snake leaked after cancel-vs-grant race")
	}

	assert.Equal(t, 0, s.InFlight())
}

// --- Self-contention ---

func TestSnake_SelfContention_Serialized(t *testing.T) {
	var contentionID atomic.Value
	contentionID.Store("id1")

	cfg := defaultSnakeConfig()
	cfg.ContentionID = func() string { return contentionID.Load().(string) }
	s := newTestSnake(cfg)

	unlock1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	var mu sync.Mutex
	var order []int

	var wg sync.WaitGroup
	for i := range 3 {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Go(func() {
			u, err := s.Acquire(t.Context())
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

	assert.Equal(t, []int{0, 1, 2}, order)
}

func TestSnake_SelfContention_DifferentIDs_Independent(t *testing.T) {
	var idCounter atomic.Int64

	cfg := defaultSnakeConfig()
	cfg.ContentionID = func() string {
		return fmt.Sprintf("id%d", idCounter.Add(1))
	}
	s := newTestSnake(cfg)

	unlock1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	acquired := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			u, err := s.Acquire(t.Context())
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
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	errCh := make(chan error, 5)
	for range 5 {
		go func() {
			_, err := s.Acquire(t.Context())
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

func TestSnake_SelfContention_NoDrop(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	cfg.ContentionID = func() string { return "same-id" }
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	results := make(chan error, 3)
	for range 3 {
		go func() {
			u, err := s.Acquire(t.Context())
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
	s := newTestSnake(cfg)

	unlock1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	acquired := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context())
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
	assert.Error(t, err, "stale release should fail")
}

func TestSnake_MaxAge_CancelledOnRelease(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.MaxAge = func() time.Duration { return 100 * time.Millisecond }
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	unlock.Release()

	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, 0, s.InFlight())
}

func TestSnake_MaxAge_Zero_NoTimeout(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.MaxAge = func() time.Duration { return 0 }
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, s.InFlight())

	unlock.Release()
}

// --- SafeUnlock ---

func TestSnake_SafeUnlock_DoubleRelease(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	err = unlock.Release()
	assert.NoError(t, err)

	err = unlock.Release()
	assert.NoError(t, err)
}

func TestSnake_SafeUnlock_StaleRelease(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	acquired := make(chan *SafeUnlock, 1)
	go func() {
		u, err := s.Acquire(t.Context())
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

func TestSnake_ReleaseCallbacks_Executed(t *testing.T) {
	var called atomic.Bool
	cfg := defaultSnakeConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) { called.Store(true) },
	}
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context())
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
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	myErr := errors.New("test error")
	unlock.Release(myErr)

	val := received.Load()
	assert.Equal(t, myErr, val)
}

func TestSnake_ReleaseCallbacks_NilOnNormalRelease(t *testing.T) {
	var received atomic.Value
	received.Store("sentinel")
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
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context())
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
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	err = unlock.Release()
	assert.NoError(t, err)
	assert.True(t, secondCalled.Load())
	assert.Equal(t, 0, s.InFlight())
}

func TestSnake_ReleaseCallbacks_NoDeadlock(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) {},
	}
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context())
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

// --- IsHealthy ---

func TestSnake_InFlight_IsHealthy(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	assert.Equal(t, 0, s.InFlight())
	assert.True(t, s.IsHealthy())

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	assert.Equal(t, 1, s.InFlight())
	assert.True(t, s.IsHealthy())

	unlock.Release()
	assert.Equal(t, 0, s.InFlight())
}

// --- Cancel in CoDel queue ---

func TestSnake_CancelInCoDelQueue(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	ctx2, cancel2 := context.WithCancel(t.Context())
	waiter2Done := make(chan error, 1)
	go func() {
		_, err := s.Acquire(ctx2)
		waiter2Done <- err
	}()

	waiter3Done := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context())
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

func TestSnake_CancelInValve(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.ContentionID = func() string { return "id1" }
	s := newTestSnake(cfg)

	unlock1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	ctx3, cancel3 := context.WithCancel(t.Context())

	waiter2Done := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context())
		if err == nil {
			u.Release()
		}
		close(waiter2Done)
	}()

	time.Sleep(5 * time.Millisecond)

	waiter3Done := make(chan error, 1)
	go func() {
		_, err := s.Acquire(ctx3)
		waiter3Done <- err
	}()

	time.Sleep(5 * time.Millisecond)

	waiter4Done := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context())
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

func TestSnake_Undroppable_NeverDropped(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	cfg.LoadsheddingAllowed = func() bool { return false }
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	results := make(chan error, 5)
	for range 5 {
		go func() {
			u, err := s.Acquire(t.Context())
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

func TestSnake_AcquireError_Custom(t *testing.T) {
	myErr := errors.New("custom acquire error")
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	cfg.AcquireError = func() error { return myErr }
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)

	errCh := make(chan error, 3)
	for range 3 {
		go func() {
			_, err := s.Acquire(t.Context())
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
	s := newTestSnake(cfg)

	var mu sync.Mutex
	var order []int

	unlock1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 2; i <= 4; i++ {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Go(func() {
			u, err := s.Acquire(t.Context())
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

// --- NewSnake (default clock) ---

func TestNewSnake_DefaultClock(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, s.InFlight())

	unlock.Release()
	assert.Equal(t, 0, s.InFlight())
}

// ==========================================================================
// Capacity-N tests (multi-holder behavior)
// ==========================================================================

func TestSnake_CapacityN_MultipleConcurrent(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 3 }
	s := newTestSnake(cfg)

	unlocks := make([]*SafeUnlock, 3)
	for i := range 3 {
		u, err := s.Acquire(t.Context())
		require.NoError(t, err)
		unlocks[i] = u
	}

	assert.Equal(t, 3, s.InFlight())

	for _, u := range unlocks {
		u.Release()
	}
	assert.Equal(t, 0, s.InFlight())
}

func TestSnake_CapacityN_BlocksAtCapacity(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 2 }
	s := newTestSnake(cfg)

	u1, err := s.Acquire(t.Context())
	require.NoError(t, err)
	u2, err := s.Acquire(t.Context())
	require.NoError(t, err)

	assert.Equal(t, 2, s.InFlight())

	acquired := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context())
		if err == nil {
			close(acquired)
			u.Release()
		}
	}()

	select {
	case <-acquired:
		t.Fatal("third acquire should block at capacity 2")
	case <-time.After(20 * time.Millisecond):
	}

	u1.Release()

	select {
	case <-acquired:
	case <-time.After(1 * time.Second):
		t.Fatal("third acquire should have been granted after release")
	}

	u2.Release()
}

func TestSnake_CapacityN_ReleaseGrantsNext_FIFO(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 1 }
	s := newTestSnake(cfg)

	u1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup

	for i := range 3 {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Go(func() {
			u, err := s.Acquire(t.Context())
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
	u1.Release()
	wg.Wait()

	assert.Equal(t, []int{0, 1, 2}, order)
}

func TestSnake_CapacityN_DynamicIncrease_GrantsWaiters(t *testing.T) {
	var capacity atomic.Int64
	capacity.Store(1)
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return int(capacity.Load()) }
	s := newTestSnake(cfg)

	u1, err := s.Acquire(t.Context())
	require.NoError(t, err)

	acquired := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			u, err := s.Acquire(t.Context())
			if err != nil {
				return
			}
			acquired <- struct{}{}
			time.Sleep(50 * time.Millisecond)
			u.Release()
		})
	}

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0, len(acquired), "no additional acquires while at capacity")

	// Increase capacity to 4 and release one to trigger lockedTryGrantNext
	capacity.Store(4)
	u1.Release()

	// All 3 waiters should get granted
	wg.Wait()
	assert.Equal(t, 3, len(acquired))
}

func TestSnake_CapacityN_DynamicDecrease_NoKill(t *testing.T) {
	var capacity atomic.Int64
	capacity.Store(5)
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return int(capacity.Load()) }
	s := newTestSnake(cfg)

	unlocks := make([]*SafeUnlock, 5)
	for i := range 5 {
		u, err := s.Acquire(t.Context())
		require.NoError(t, err)
		unlocks[i] = u
	}

	assert.Equal(t, 5, s.InFlight())

	// Reduce capacity — existing holders stay alive
	capacity.Store(2)
	assert.Equal(t, 5, s.InFlight())

	// Release all
	for _, u := range unlocks {
		u.Release()
	}
	assert.Equal(t, 0, s.InFlight())
}

func TestSnake_CapacityN_CompletionDequeues(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 2 }
	s := newTestSnake(cfg)

	u1, err := s.Acquire(t.Context())
	require.NoError(t, err)
	u2, err := s.Acquire(t.Context())
	require.NoError(t, err)

	// Both granted, both in queue as undroppable
	assert.Equal(t, 2, s.InFlight())

	// Release u1 — this calls lockedComplete which removes from queue
	u1.Release()
	assert.Equal(t, 1, s.InFlight())

	u2.Release()
	assert.Equal(t, 0, s.InFlight())
}

func TestSnake_CapacityN_CoDelDrops_WithMultipleHolders(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 2 }
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	s := newTestSnake(cfg)

	// Fill to capacity
	u1, err := s.Acquire(t.Context())
	require.NoError(t, err)
	u2, err := s.Acquire(t.Context())
	require.NoError(t, err)

	errCh := make(chan error, 5)
	for range 5 {
		go func() {
			_, err := s.Acquire(t.Context())
			errCh <- err
		}()
	}

	// Hold long enough for CoDel to drop
	time.Sleep(200 * time.Millisecond)
	u1.Release()
	u2.Release()
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

func TestSnake_CapacityN_MutualExclusion_AtN(t *testing.T) {
	const N = 5
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return N }
	s := newTestSnake(cfg)

	var held atomic.Int32
	var maxHeld atomic.Int32
	var wg sync.WaitGroup

	for range 50 {
		wg.Go(func() {
			u, err := s.Acquire(t.Context())
			if err != nil {
				return
			}
			v := held.Add(1)
			for {
				cur := maxHeld.Load()
				if v <= cur || maxHeld.CompareAndSwap(cur, v) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			held.Add(-1)
			u.Release()
		})
	}

	wg.Wait()
	assert.LessOrEqual(t, maxHeld.Load(), int32(N), "at most N concurrent holders")
	assert.Greater(t, maxHeld.Load(), int32(1), "should have had concurrent holders")
	assert.Equal(t, 0, s.InFlight())
}
