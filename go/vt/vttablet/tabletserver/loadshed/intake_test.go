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
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntake_PushDrainRoundTrip(t *testing.T) {
	in := newIntake()
	assert.Equal(t, int64(0), in.pendingLen())
	assert.Nil(t, in.drain(), "empty intake drains to nil")

	// Stage requests with monotonically increasing arrival times.
	const n = 100
	for i := range n {
		r := newRequest(0)
		r.codelqEnqueuedAtNs = int64(i)
		in.push(r)
	}
	require.Equal(t, int64(n), in.pendingLen())

	got := in.drain()
	require.Len(t, got, n, "drain returns every staged request")
	assert.Equal(t, int64(0), in.pendingLen(), "pending resets after drain")
	assert.Nil(t, in.drain(), "second drain is empty")

	// Must come back sorted by arrival time.
	assert.True(t, sort.SliceIsSorted(got, func(a, b int) bool {
		return got[a].codelqEnqueuedAtNs < got[b].codelqEnqueuedAtNs
	}), "drained requests are ordered by arrival time")
}

// TestIntake_ConcurrentPush stresses concurrent pushes across CPUs and asserts
// no request is lost and pending stays exact.
func TestIntake_ConcurrentPush(t *testing.T) {
	in := newIntake()
	const workers = 32
	const perWorker = 500

	var seq atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				r := newRequest(0)
				r.codelqEnqueuedAtNs = seq.Add(1)
				in.push(r)
			}
		}()
	}
	wg.Wait()

	total := int64(workers * perWorker)
	require.Equal(t, total, in.pendingLen())

	got := in.drain()
	require.Len(t, got, int(total), "no pushes lost under concurrency")
	assert.True(t, sort.SliceIsSorted(got, func(a, b int) bool {
		return got[a].codelqEnqueuedAtNs < got[b].codelqEnqueuedAtNs
	}), "drain is globally time-ordered even across shards")
}
