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

// TestEnqueueAdvance_ArrivalsDriveShedding: with EnqueueAdvanceProbability=1 and
// the single slot held (never released), contending arrivals themselves run the
// CoDel advance, so once sojourn exceeds target they are shed — without any
// release or reliance on the backstop timer. Critically, the shed goroutines
// must return (their deferred rejection signals were sent after unlock); a
// missed drain would hang them forever.
func TestEnqueueAdvance_ArrivalsDriveShedding(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	cfg.EnqueueAdvanceProbability = func() float64 { return 1 }
	s := newTestSnake(cfg)

	// Occupy the only slot and never release it.
	holder, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	defer holder.Release()

	// Pile on contenders. Each is a fresh arrival, so with p=1 each enqueue runs
	// the advance; as their sojourn passes target CoDel sheds them. No release
	// happens, so shedding here is arrival-driven.
	errCh := make(chan error, 8)
	for range 8 {
		go func() {
			_, aerr := s.Acquire(t.Context(), "", 0)
			errCh <- aerr
		}()
	}

	dropped := 0
	for range 8 {
		select {
		case aerr := <-errCh:
			if aerr != nil {
				dropped++
			}
		case <-time.After(3 * time.Second):
			t.Fatal("a contending acquire hung — an arrival-advance drop was not woken")
		}
	}
	assert.Positive(t, dropped, "arrival-driven advance should shed contenders with the slot held")
}

// TestEnqueueAdvance_DisabledByDefault: with the knob unset, a non-granted
// enqueue must NOT run the advance — behavior is unchanged from today. A held
// slot plus a short-context contender simply blocks and cancels; nothing is
// shed by an arrival.
func TestEnqueueAdvance_DisabledByDefault(t *testing.T) {
	cfg := defaultSnakeConfig() // EnqueueAdvanceProbability nil
	s := newTestSnake(cfg)

	holder, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)
	defer holder.Release()

	// The shed counter must not move from an arrival while the knob is off. Fire a
	// contender with a context that cancels quickly so the test does not block.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = s.Acquire(ctx, "", 0)
	require.Error(t, err) // ctx cancellation, not a gate shed
	assert.Equal(t, int64(0), s.ShedCount(), "knob off: an arrival must not drive shedding")
}
