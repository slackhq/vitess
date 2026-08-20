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

package smartconnpool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

func newTestSnake(t *testing.T, capacity int) *loadshed.Snake {
	t.Helper()
	return loadshed.NewSnake(loadshed.SnakeConfig{
		Name: "test",
		CoDel: loadshed.CoDelConfig{
			IntervalNs:     func() int64 { return int64(time.Second) },
			TargetNs:       func() int64 { return int64(time.Second) },
			Exponent:       func() float64 { return 1.0 },
			MinDropDelayNs: func() int64 { return 100 },
		},
		Capacity:            func() int { return capacity },
		LoadsheddingAllowed: func() bool { return true },
	})
}

func TestSnakeGate_FastPathIsGated(t *testing.T) {
	var state TestState
	snake := newTestSnake(t, 1)

	p := NewPool(&Config[*TestConn]{
		Capacity: 10,
		Snake:    snake,
	}).Open(newConnector(&state), nil)
	defer p.Close()

	ctx := context.Background()

	held, err := p.GetWithPriority(ctx, nil, 0)
	require.NoError(t, err)

	// The physical pool still has 9 idle connections available, but Snake's
	// own capacity (1) is exhausted — a fresh acquire must be gated (blocked
	// in Snake's queue) even though the underlying pool would grant it
	// immediately on the fast path. A short-lived context stands in for "this
	// acquire never gets in while the slot is held" without waiting for
	// CoDel's real shed timing.
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = p.GetWithPriority(shortCtx, nil, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPoolLoadShed), "expected ErrPoolLoadShed, got %v", err)

	held.Recycle()
}

func TestSnakeGate_RecycleFreesSlot(t *testing.T) {
	var state TestState
	snake := newTestSnake(t, 1)

	p := NewPool(&Config[*TestConn]{
		Capacity: 10,
		Snake:    snake,
	}).Open(newConnector(&state), nil)
	defer p.Close()

	ctx := context.Background()

	held, err := p.GetWithPriority(ctx, nil, 0)
	require.NoError(t, err)

	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = p.GetWithPriority(shortCtx, nil, 0)
	require.Error(t, err, "should be gated while the only slot is held")

	held.Recycle()

	held2, err := p.GetWithPriority(ctx, nil, 0)
	require.NoError(t, err, "slot should be free after Recycle")
	held2.Recycle()
}

func TestSnakeGate_PlainGetIsGated(t *testing.T) {
	var state TestState
	snake := newTestSnake(t, 1)

	p := NewPool(&Config[*TestConn]{
		Capacity:        10,
		Snake:           snake,
		DefaultPriority: 0,
	}).Open(newConnector(&state), nil)
	defer p.Close()

	ctx := context.Background()

	held, err := snake.Acquire(ctx, 0)
	require.NoError(t, err)

	// Plain Get must not bypass the gate: exhausting Snake's capacity via a
	// side-channel Acquire must still block/shed a plain Get call.
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = p.Get(shortCtx, nil)
	require.Error(t, err, "plain Get must be gated identically to GetWithPriority")

	require.NoError(t, held.Release())

	conn, err := p.Get(ctx, nil)
	require.NoError(t, err, "plain Get should succeed once the slot is free")
	conn.Recycle()
}

func TestSnakeGate_UndroppableNeverShed(t *testing.T) {
	var state TestState
	snake := newTestSnake(t, 1)

	p := NewPool(&Config[*TestConn]{
		Capacity: 10,
		Snake:    snake,
	}).Open(newConnector(&state), nil)
	defer p.Close()

	ctx := context.Background()

	held, err := p.GetWithPriority(ctx, nil, 0)
	require.NoError(t, err)

	holdersBefore := snake.Stats().HolderCount

	// An undroppable caller must never be shed, even though the gate's only
	// slot is held — it queues instead of failing immediately. Use a short
	// deadline so a regression (permanently gated/blocked) fails the test
	// instead of hanging.
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		u, err := p.GetWithPriority(context.Background(), nil, loadshed.PriorityUndroppable)
		if err == nil {
			u.Recycle()
		}
		errCh <- err
	}()

	select {
	case <-shortCtx.Done():
	case <-errCh:
		t.Fatal("undroppable acquire should still be queued, not resolved yet")
	}
	assert.Equal(t, holdersBefore, snake.Stats().HolderCount, "undroppable caller should be queued, not granted, while the slot is held")

	held.Recycle()

	select {
	case err := <-errCh:
		assert.NoError(t, err, "undroppable request must never be shed")
	case <-time.After(2 * time.Second):
		t.Fatal("undroppable acquire never resolved after the slot freed")
	}
}
