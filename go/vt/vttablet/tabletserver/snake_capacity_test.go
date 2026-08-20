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

package tabletserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

// acquireN acquires count slots from the snake, failing the test if any acquire
// is shed or blocked. The returned unlocks must be released by the caller.
func acquireN(t *testing.T, s *loadshed.Snake, count int) []*loadshed.SafeUnlock {
	t.Helper()
	unlocks := make([]*loadshed.SafeUnlock, count)
	for i := range count {
		u, err := s.Acquire(t.Context(), 0)
		require.NoErrorf(t, err, "acquire %d of %d should be granted", i+1, count)
		unlocks[i] = u
	}
	return unlocks
}

// acquireBlocks reports whether a single acquire blocks (rather than being
// granted) within a short window. A granted slot is released immediately.
func acquireBlocks(s *loadshed.Snake) bool {
	granted := make(chan *loadshed.SafeUnlock, 1)
	go func() {
		u, err := s.Acquire(context.Background(), 0)
		if err == nil {
			granted <- u
		}
	}()
	select {
	case u := <-granted:
		u.Release()
		return false
	case <-time.After(50 * time.Millisecond):
		return true
	}
}

// TestSnakeCapacity_TracksLivePoolResize verifies that shrinking the oltp-read
// pool at runtime (as /debug/env SetPoolSize does) also lowers the snake's
// effective gate capacity. Before this wiring the snake read the static
// configured size, so a live pool resize left the gate admitting at the old
// ceiling while the smaller pool queued the overflow.
func TestSnakeCapacity_TracksLivePoolResize(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	qe.se.Open()
	qe.Open()
	defer qe.Close()

	require.NotNil(t, qe.snake, "loadshed should be enabled by default in tests")

	// Shrink the live pool below the configured size.
	require.NoError(t, qe.conns.SetCapacity(context.Background(), 2))

	// The gate should now admit exactly the live capacity (2) and block the 3rd.
	unlocks := acquireN(t, qe.snake, 2)
	defer func() {
		for _, u := range unlocks {
			u.Release()
		}
	}()

	assert.True(t, acquireBlocks(qe.snake),
		"acquire beyond the resized pool capacity should block, not be granted")
}

// TestSnakeCapacity_FallsBackToConfigBeforeOpen verifies that before the pool is
// opened — when Pool.Capacity() reports 0 — the snake falls back to the
// configured pool size rather than collapsing to a single holder. A snake built
// from a freshly-constructed (unopened) query engine must still admit up to the
// configured OltpReadPool.Size.
func TestSnakeCapacity_FallsBackToConfigBeforeOpen(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	// Engine constructed but never Open()ed: qe.conns.Capacity() == 0.
	qe := newTestQueryEngine(10*time.Second, true, newDBConfigs(db))
	require.NotNil(t, qe.snake, "loadshed should be enabled by default in tests")

	configured := qe.env.Config().OltpReadPool.Size
	require.Greater(t, configured, 1, "test needs a configured size > 1 to be meaningful")

	// The gate must admit up to the configured size despite the pool being
	// unopened (Capacity() == 0). A naive live-only closure would max(0,1)=1
	// here and block the 2nd acquire.
	unlocks := acquireN(t, qe.snake, configured)
	defer func() {
		for _, u := range unlocks {
			u.Release()
		}
	}()

	assert.True(t, acquireBlocks(qe.snake),
		"acquire beyond the configured size should block")
}
