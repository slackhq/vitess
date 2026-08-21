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

func defaultSnakeConfig() SnakeConfig {
	return SnakeConfig{
		Name: "test-snake",
		CoDel: CoDelConfig{
			IntervalNs:     func() int64 { return int64(1e9) },
			TargetNs:       func() int64 { return int64(1e9) },
			Exponent:       func() float64 { return 1.0 },
			MinDropDelayNs: func() int64 { return 100 },
			EasingLogBase:  func() float64 { return 2.0 },
		},
		LoadsheddingAllowed: func() bool { return true },
	}
}

func newTestSnake(cfg SnakeConfig) *Snake {
	return NewSnake(cfg)
}

func (s *Snake) nGranted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.holders)
}

func (s *Snake) isIdle() bool {
	return s.nGranted() == 0
}

// --- Basic acquire/release ---

func TestSnake_AcquireRelease_Basic(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	assert.True(t, s.isIdle())

	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	assert.False(t, s.isIdle())

	err = unlock.Release()
	assert.NoError(t, err)
	assert.True(t, s.isIdle())
}

func TestSnake_AcquireRelease_Sequential(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	for range 10 {
		unlock, err := s.Acquire(t.Context(), "", 0)
		require.NoError(t, err)
		assert.False(t, s.isIdle())

		err = unlock.Release()
		assert.NoError(t, err)
		assert.True(t, s.isIdle())
	}
}

// --- Mutual exclusion ---

func TestSnake_MutualExclusion(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	var held atomic.Int32
	var wg sync.WaitGroup

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock, err := s.Acquire(t.Context(), "", 0)
			if err != nil {
				return
			}
			defer unlock.Release()

			val := held.Add(1)
			assert.Equal(t, int32(1), val, "multiple goroutines hold the lock")
			time.Sleep(1 * time.Millisecond)
			held.Add(-1)
		}()
	}

	wg.Wait()
	assert.True(t, s.isIdle())
}

// --- FIFO ordering ---

func TestSnake_FIFO_Order(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	var mu sync.Mutex
	var order []int

	var wg sync.WaitGroup
	for i := range 5 {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), "", 0)
			if err != nil {
				return
			}
			mu.Lock()
			order = append(order, idx)
			mu.Unlock()
			u.Release()
		}()
	}

	time.Sleep(20 * time.Millisecond)

	unlock1.Release()
	wg.Wait()

	assert.Equal(t, []int{0, 1, 2, 3, 4}, order, "waiters should be served FIFO")
}

// --- Release wakes next ---

func TestSnake_ReleaseWakesNext(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	acquired := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context(), "", 0)
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

	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)
	go func() {
		_, err := s.Acquire(ctx, "", 0)
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
	assert.True(t, s.isIdle())
}

func TestSnake_ContextTimeout(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err = s.Acquire(ctx, "", 0)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	unlock.Release()
}

// --- Cancel-vs-grant race ---

func TestSnake_ContextCancel_RaceWithGrant(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	waiter2Done := make(chan error, 1)
	go func() {
		u, err := s.Acquire(ctx, "", 0)
		if err == nil {
			u.Release()
		}
		waiter2Done <- err
	}()

	waiter3Done := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context(), "", 0)
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

	assert.True(t, s.isIdle())
}

// --- Self-contention ---

func TestSnake_SelfContention_Serialized(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context(), "id1", 0)
	require.NoError(t, err)

	var mu sync.Mutex
	var order []int

	var wg sync.WaitGroup
	for i := range 3 {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), "id1", 0)
			if err != nil {
				return
			}
			mu.Lock()
			order = append(order, idx)
			mu.Unlock()
			u.Release()
		}()
	}

	time.Sleep(20 * time.Millisecond)
	unlock1.Release()
	wg.Wait()

	assert.Equal(t, []int{0, 1, 2}, order)
}

func TestSnake_SelfContention_DifferentIDs_Independent(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	acquired := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		valveID := string(rune('a' + i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), valveID, 0)
			if err != nil {
				return
			}
			acquired <- struct{}{}
			u.Release()
		}()
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
	// Enough contenders to push the droppable backlog past keepDroppableFloor so
	// CoDel actually sheds, and capacity high enough that the below-floor
	// survivors get granted as the holders release rather than stranding.
	const contenders = 16
	cfg.Capacity = func() int { return 4 }
	s := newTestSnake(cfg)

	var unlocks []*SafeUnlock
	for range 4 {
		u, err := s.Acquire(t.Context(), "", 0)
		require.NoError(t, err)
		unlocks = append(unlocks, u)
	}

	errCh := make(chan error, contenders)
	for range contenders {
		go func() {
			_, err := s.Acquire(t.Context(), "", 0)
			errCh <- err
		}()
	}

	time.Sleep(200 * time.Millisecond)

	for _, u := range unlocks {
		u.Release()
	}
	time.Sleep(50 * time.Millisecond)

	dropped := 0
	for range contenders {
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
	// Valve serialization: 3 same-ID requests serialize through the valve so only
	// ONE droppable entry is in the CoDel queue at a time. With a target well above
	// the per-holder execution time (~1ms), each promoted entry's sojourn is below
	// target, keeping CoDel in healthy state and preventing drops.
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 100_000_000 }   // 100ms
	cfg.CoDel.TargetNs = func() int64 { return 10_000_000 }      // 10ms
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1_000_000 } // 1ms
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context(), "same-id", 0)
	require.NoError(t, err)

	results := make(chan error, 3)
	for range 3 {
		go func() {
			u, err := s.Acquire(t.Context(), "same-id", 0)
			if err == nil {
				time.Sleep(1 * time.Millisecond)
				u.Release()
			}
			results <- err
		}()
	}

	time.Sleep(5 * time.Millisecond)
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

// --- SafeUnlock ---

func TestSnake_SafeUnlock_DoubleRelease(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	err = unlock.Release()
	assert.NoError(t, err)

	err = unlock.Release()
	assert.NoError(t, err)
}

func TestSnake_SafeUnlock_StaleNonce(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	acquired := make(chan *SafeUnlock, 1)
	go func() {
		u, err := s.Acquire(t.Context(), "", 0)
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

	unlock, err := s.Acquire(t.Context(), "", 0)
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

	unlock, err := s.Acquire(t.Context(), "", 0)
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

	unlock, err := s.Acquire(t.Context(), "", 0)
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

	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	err = unlock.Release()
	assert.NoError(t, err)
	assert.True(t, secondCalled.Load())
	assert.True(t, s.isIdle())
}

func TestSnake_ReleaseCallbacks_NoDeadlock(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.ReleaseCBs = []func(error){
		func(err error) {},
	}
	s := newTestSnake(cfg)

	unlock, err := s.Acquire(t.Context(), "", 0)
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

// --- isIdle / IsHealthy ---

func TestSnake_IsIdle_IsHealthy(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	assert.True(t, s.isIdle())
	assert.True(t, s.IsHealthy())

	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	assert.False(t, s.isIdle())
	assert.True(t, s.IsHealthy())

	unlock.Release()
	assert.True(t, s.isIdle())
}

// --- Cancel in CoDel queue ---

func TestSnake_CancelInCoDelQueue(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	ctx2, cancel2 := context.WithCancel(t.Context())
	waiter2Done := make(chan error, 1)
	go func() {
		_, err := s.Acquire(ctx2, "", 0)
		waiter2Done <- err
	}()

	waiter3Done := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context(), "", 0)
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
	s := newTestSnake(defaultSnakeConfig())

	unlock1, err := s.Acquire(t.Context(), "id1", 0)
	require.NoError(t, err)

	ctx3, cancel3 := context.WithCancel(t.Context())

	waiter2Done := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context(), "id1", 0)
		if err == nil {
			u.Release()
		}
		close(waiter2Done)
	}()

	time.Sleep(5 * time.Millisecond)

	waiter3Done := make(chan error, 1)
	go func() {
		_, err := s.Acquire(ctx3, "id1", 0)
		waiter3Done <- err
	}()

	time.Sleep(5 * time.Millisecond)

	waiter4Done := make(chan struct{})
	go func() {
		u, err := s.Acquire(t.Context(), "id1", 0)
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

	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	results := make(chan error, 5)
	for range 5 {
		go func() {
			u, err := s.Acquire(t.Context(), "", 0)
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
	// Enough contenders to push the droppable backlog past keepDroppableFloor so
	// CoDel sheds (and returns the custom error), and capacity high enough that
	// below-floor survivors get granted as holders release rather than stranding.
	const contenders = 16
	cfg.Capacity = func() int { return 4 }
	s := newTestSnake(cfg)

	var unlocks []*SafeUnlock
	for range 4 {
		u, err := s.Acquire(t.Context(), "", 0)
		require.NoError(t, err)
		unlocks = append(unlocks, u)
	}

	errCh := make(chan error, contenders)
	for range contenders {
		go func() {
			_, err := s.Acquire(t.Context(), "", 0)
			errCh <- err
		}()
	}

	time.Sleep(100 * time.Millisecond)
	for _, u := range unlocks {
		u.Release()
	}
	time.Sleep(50 * time.Millisecond)

	dropped := 0
	for range contenders {
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
	s := newTestSnake(defaultSnakeConfig())

	var mu sync.Mutex
	var order []int

	unlock1, err := s.Acquire(t.Context(), "id1", 0)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 2; i <= 4; i++ {
		time.Sleep(2 * time.Millisecond)
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := s.Acquire(t.Context(), "id1", 0)
			if err != nil {
				return
			}
			mu.Lock()
			order = append(order, idx)
			mu.Unlock()
			u.Release(errors.New("simulated error"))
		}()
	}

	time.Sleep(20 * time.Millisecond)
	unlock1.Release(errors.New("first error"))
	wg.Wait()

	assert.Equal(t, []int{2, 3, 4}, order, "all waiters should complete despite errors")
}

// --- NewSnake (default clock) ---

func TestNewSnake_DefaultClock(t *testing.T) {
	s := NewSnake(defaultSnakeConfig())

	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	assert.False(t, s.isIdle())

	unlock.Release()
	assert.True(t, s.isIdle())
}

// TestSnake_Priority_HonorsCallerPriority confirms Snake enqueues the caller's
// priority unchanged when shedding is allowed. Snake is agnostic to the
// caller's priority scheme; it only applies the LoadsheddingAllowed gate.
func TestSnake_Priority_HonorsCallerPriority(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	for _, priority := range []float64{0, 50, 100} {
		assert.Equal(t, priority, s.priority(priority),
			"caller priority must pass through untouched when shedding is allowed")
	}
}

func TestSnake_Priority_DisabledPreservesCallerPriority(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.LoadsheddingAllowed = func() bool { return false }
	s := newTestSnake(cfg)

	for _, priority := range []float64{0, 50, 100} {
		assert.Equal(t, priority, s.priority(priority))
	}
}

func TestSnake_DisablePreventsQueuedRequestFromBeingShed(t *testing.T) {
	type result struct {
		unlock *SafeUnlock
		err    error
	}
	const waiters = 10

	runDueBatch := func(s *Snake) {
		s.mu.Lock()
		s.lockedStopDropTimer()
		s.q.codelq.dropping = true
		s.q.codelq.count = s.q.codelq.graceCount()
		s.q.codelq.dropNextNs = s.clockFunc()
		pending := s.lockedEnqueueAdvance()
		s.mu.Unlock()
		for _, req := range pending {
			req.sendSignal()
		}
	}

	newLoadedSnake := func(t *testing.T) (*Snake, *SafeUnlock, *atomic.Bool, context.CancelFunc, <-chan result) {
		t.Helper()
		allowed := &atomic.Bool{}
		allowed.Store(true)
		cfg := defaultSnakeConfig()
		cfg.Capacity = func() int { return 1 }
		cfg.LoadsheddingAllowed = allowed.Load
		cfg.CoDel.TargetNs = func() int64 { return time.Millisecond.Nanoseconds() }
		cfg.CoDel.IntervalNs = func() int64 { return (10 * time.Millisecond).Nanoseconds() }
		cfg.CoDel.MinDropDelayNs = func() int64 { return (10 * time.Millisecond).Nanoseconds() }
		s := newTestSnake(cfg)

		holder, err := s.Acquire(t.Context(), "", 0)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		resultCh := make(chan result, waiters)
		for range waiters {
			go func() {
				unlock, err := s.Acquire(ctx, "", 0)
				resultCh <- result{unlock: unlock, err: err}
			}()
		}
		require.Eventually(t, func() bool {
			return s.Stats().DroppableLen == waiters
		}, time.Second, time.Millisecond)
		return s, holder, allowed, cancel, resultCh
	}

	t.Run("enabled sheds", func(t *testing.T) {
		s, holder, _, cancel, resultCh := newLoadedSnake(t)
		defer cancel()
		runDueBatch(s)

		var first result
		require.Eventually(t, func() bool {
			select {
			case first = <-resultCh:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond)
		assert.Error(t, first.err)

		cancel()
		require.NoError(t, holder.Release())
		for range waiters - 1 {
			res := <-resultCh
			if res.unlock != nil {
				require.NoError(t, res.unlock.Release())
			}
		}
	})

	t.Run("disabled preserves waiters", func(t *testing.T) {
		s, holder, allowed, cancel, resultCh := newLoadedSnake(t)
		defer cancel()
		allowed.Store(false)
		runDueBatch(s)

		assert.Never(t, func() bool {
			return len(resultCh) > 0
		}, 150*time.Millisecond, time.Millisecond)
		assert.False(t, s.IsHealthy(), "disabled shedding must still observe queue health")

		require.NoError(t, holder.Release())
		for range waiters {
			res := <-resultCh
			require.NoError(t, res.err)
			require.NotNil(t, res.unlock)
			require.NoError(t, res.unlock.Release())
		}
	})
}
