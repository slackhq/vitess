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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intakeConfig() SnakeConfig {
	cfg := defaultSnakeConfig()
	cfg.PerCPUIntake = true
	return cfg
}

// TestSnakeIntake_FastPathGrant: with intake enabled but uncontended, an
// Acquire is granted inline (fast path), and Release frees it.
func TestSnakeIntake_FastPathGrant(t *testing.T) {
	s := newTestSnake(intakeConfig())
	require.NotNil(t, s.intake)

	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	require.NotNil(t, unlock)
	assert.False(t, s.isIdle())
	require.NoError(t, unlock.Release())
	assert.True(t, s.isIdle())
}

// TestSnakeIntake_StagedThenGranted: while the single slot is held, further
// acquires stage in the intake; releasing the holder must merge and grant a
// staged waiter (no stranding).
func TestSnakeIntake_StagedThenGranted(t *testing.T) {
	s := newTestSnake(intakeConfig()) // capacity 1

	held, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	granted := make(chan *SafeUnlock, 1)
	go func() {
		u, err := s.Acquire(t.Context(), "", 0)
		if err == nil {
			granted <- u
		}
	}()

	// The waiter should be staged (or enqueued), not granted yet.
	select {
	case <-granted:
		t.Fatal("second acquire granted while slot held")
	case <-time.After(50 * time.Millisecond):
	}

	// Release frees the slot; the merge + grant must wake the staged waiter.
	require.NoError(t, held.Release())
	select {
	case u := <-granted:
		require.NotNil(t, u)
		require.NoError(t, u.Release())
	case <-time.After(30 * time.Second):
		t.Fatal("staged waiter was never granted after release (stranded)")
	}
}

// TestSnakeIntake_NoStrandingUnderLoad drives sustained over-capacity through
// the intake path and asserts every request is accounted for (granted or shed)
// with none left hanging — the key correctness property, since the merge only
// fires when no waiter remains / on release.
func TestSnakeIntake_NoStrandingUnderLoad(t *testing.T) {
	cfg := intakeConfig()
	cfg.Capacity = func() int { return 4 }
	// Aggressive CoDel so overload sheds promptly.
	cfg.CoDel.TargetNs = func() int64 { return 500_000 }
	cfg.CoDel.IntervalNs = func() int64 { return 5_000_000 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 100_000 }
	s := NewSnake(cfg)

	const workers = 64
	const perWorker = 50
	var granted, shed atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for range perWorker {
				u, err := s.Acquire(ctx, "", 0)
				if err != nil {
					shed.Add(1)
					continue
				}
				granted.Add(1)
				time.Sleep(time.Millisecond) // hold briefly
				u.Release()
			}
		}()
	}
	wg.Wait()

	total := int64(workers * perWorker)
	assert.Equal(t, total, granted.Load()+shed.Load(),
		"every request must be granted or shed, none stranded")
	assert.Positive(t, granted.Load(), "some requests should be granted")
	assert.Positive(t, shed.Load(), "overload should shed some requests")

	// After the storm, the gate must return to idle with an empty intake.
	assert.Eventually(t, func() bool {
		return s.isIdle() && s.intake.pendingLen() == 0
	}, 30*time.Second, 10*time.Millisecond, "gate should drain to idle with empty intake")
}
