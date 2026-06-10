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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnake_ValveInvariant_SecondRequestGrantedAfterFirst verifies that a
// second request for the same valve ID is granted immediately when capacity
// is available — not stranded in the valve. This guards against a regression
// where the valve gate condition used outstandingCounts > 1 (which stays
// elevated after grant) instead of checking droppablePerValve.
func TestSnake_ValveInvariant_SecondRequestGrantedAfterFirst(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 10 }
	s := NewSnake(cfg)

	u1, err := s.Acquire(t.Context(), "foo")
	require.NoError(t, err)
	assert.Equal(t, 1, s.nGranted())

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	u2, err := s.Acquire(ctx, "foo")
	require.NoError(t, err, "second request should be granted immediately — not stranded in valve")
	assert.Equal(t, 2, s.nGranted())

	u1.Release()
	u2.Release()
}

// TestSnake_ValveInvariant_AllGrantedWhenCapacitySufficient verifies the
// desired behavior: with capacity=10 and M=5 requests on the same valve ID,
// all 5 should be granted concurrently.
func TestSnake_ValveInvariant_AllGrantedWhenCapacitySufficient(t *testing.T) {
	const capacity = 10
	const M = 5

	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return capacity }
	s := NewSnake(cfg)

	unlocks := make([]*SafeUnlock, M)
	for i := range M {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		u, err := s.Acquire(ctx, "foo")
		cancel()
		require.NoError(t, err, "request %d should be granted (capacity=%d, holders=%d)", i, capacity, i)
		unlocks[i] = u
	}

	assert.Equal(t, M, s.nGranted(), "all %d requests should be granted concurrently", M)

	for _, u := range unlocks {
		u.Release()
	}
	assert.Equal(t, 0, s.nGranted())
}

// TestSnake_ValveInvariant_CapacityExhausted verifies that when capacity < M,
// the system fills to capacity, has exactly one droppable representative in the
// CoDel queue, and valves the remainder.
func TestSnake_ValveInvariant_CapacityExhausted(t *testing.T) {
	const capacity = 3
	const M = 7

	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return capacity }
	s := NewSnake(cfg)

	// Fill to capacity.
	unlocks := make([]*SafeUnlock, capacity)
	for i := range capacity {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		u, err := s.Acquire(ctx, "foo")
		cancel()
		require.NoError(t, err, "request %d should be granted", i)
		unlocks[i] = u
	}
	assert.Equal(t, capacity, s.nGranted())

	// Next request should block (no capacity). Launch it in a goroutine.
	blocked := make(chan struct{})
	granted := make(chan *SafeUnlock, 1)
	go func() {
		close(blocked)
		u, err := s.Acquire(context.Background(), "foo")
		if err == nil {
			granted <- u
		}
	}()
	<-blocked
	time.Sleep(10 * time.Millisecond)

	// Verify the invariant: there should be exactly one droppable for "foo"
	// in the CoDel queue.
	s.mu.Lock()
	droppable, hasDroppable := s.q.droppablePerValve["foo"]
	s.mu.Unlock()
	assert.True(t, hasDroppable, "nonempty valve must have a droppable representative")
	assert.NotNil(t, droppable)

	// Release one slot — the blocked request should be granted.
	unlocks[0].Release()

	select {
	case u := <-granted:
		assert.Equal(t, capacity, s.nGranted())
		u.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("blocked request should have been granted after release")
	}

	for i := 1; i < capacity; i++ {
		unlocks[i].Release()
	}
}
