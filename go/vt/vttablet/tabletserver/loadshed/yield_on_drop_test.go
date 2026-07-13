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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnake_YieldOnDrop_BehaviorUnchanged: enabling yield-on-drop is a
// scheduling-only hint — a shed request still returns the drop error. The yield
// (runtime.Gosched) only affects when the rejecting goroutine is rescheduled, not
// the outcome. Drives real contention so CoDel sheds, with the flag on.
func TestSnake_YieldOnDrop_BehaviorUnchanged(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Capacity = func() int { return 1 }
	cfg.CoDel.IntervalNs = func() int64 { return 1_000 }
	cfg.CoDel.TargetNs = func() int64 { return 1 }
	cfg.CoDel.MinDropDelayNs = func() int64 { return 1 }
	cfg.YieldOnDrop = func() bool { return true }
	s := newTestSnake(cfg)

	// Hold the only slot, then pile on contenders so CoDel drops some. Each dropped
	// Acquire must still return a non-nil error despite the yield.
	unlock, err := s.Acquire(t.Context(), "", 0)
	require.NoError(t, err)

	errCh := make(chan error, 5)
	for range 5 {
		go func() {
			_, aerr := s.Acquire(t.Context(), "", 0)
			errCh <- aerr
		}()
	}
	time.Sleep(200 * time.Millisecond)
	unlock.Release()

	dropped := 0
	for range 5 {
		select {
		case aerr := <-errCh:
			if aerr != nil {
				dropped++
			}
		case <-time.After(2 * time.Second):
			t.Fatal("goroutine did not return — yield must not hang a shed request")
		}
	}
	require.Positive(t, dropped, "CoDel should have dropped some requests")
	assert.Equal(t, int64(dropped), s.ShedCount(), "every returned error is a counted shed; yield changes scheduling only")
}

// TestSnake_YieldOnDrop_NilConfigNoYield: a nil YieldOnDrop (the default) never
// yields and drops behave exactly as today — zero-value safety.
func TestSnake_YieldOnDrop_NilConfigNoYield(t *testing.T) {
	cfg := defaultSnakeConfig() // YieldOnDrop unset
	s := newTestSnake(cfg)
	// maybeYieldOnDrop must be a no-op with a nil hook (no panic, no yield).
	s.maybeYieldOnDrop()
}
