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
)

// TestCoDelQueue_Advance_NoSpinWhenDropFails reproduces the production hang: when
// a dropping episode's dropFn cannot actually shed (e.g. keep-last refusing to
// drop the final droppable request, or a droppableLen/index desync), the old
// lockedAdvance incremented count and advanced dropNextNs by an ever-shrinking
// interval every iteration while the head-check kept re-arming dropping. Against
// a real (advancing) clock the loop could not outrun now and spun forever under
// s.mu. The fix: count++/dropNextNs only advance on a successful drop; a failed
// drop falls through to easing, so count stays bounded and dropNextNs marches
// past now — the call terminates.
func TestCoDelQueue_Advance_NoSpinWhenDropFails(t *testing.T) {
	// Real, always-advancing clock — essential: with a frozen test clock the
	// loop trivially exits, masking the bug.
	clockFn := func() int64 { return time.Now().UnixNano() }
	cfg := defaultTestConfig()
	cfg.IntervalNs = func() int64 { return 100_000_000 } // 100ms, prod default
	cfg.Exponent = func() float64 { return 1 }
	keep := true
	cfg.KeepLastDroppable = func() bool { return keep }
	q := newValvedCoDelQueue(cfg, clockFn, func(int64) {}, func() {})

	// A single droppable request keep-last will refuse to drop, with an OLD
	// arrival time so the head-check re-arm condition (head.enqueuedAt <
	// dropNextNs) stays true as dropNextNs advances — the state that made the
	// old loop count++ forever. dropNextNs starts behind now (a gap to close, as
	// after a dequeue-path stall) but after the head's arrival.
	req := newRequest(1)
	req.codelqEnqueuedAtNs = clockFn() - 10_000_000_000 // 10s ago
	q.codelq.lockedAdmit(req)
	q.codelq.dropping = true
	q.codelq.count = 2
	q.codelq.dropNextNs = clockFn() - 5_000_000_000 // 5s behind now, after the head

	done := make(chan struct{})
	go func() {
		q.codelq.lockedRunTimer(q.lockedDropFn())
		close(done)
	}()

	select {
	case <-done:
		// Terminated. count must not have run away, and the last droppable is kept.
		assert.LessOrEqual(t, q.codelq.count, 100, "count must stay bounded when the drop is refused")
		assert.Equal(t, 1, q.codelq.droppableLen, "keep-last preserves the final droppable request")
	case <-time.After(5 * time.Second):
		t.Fatalf("lockedAdvance spun (did not terminate): count=%d dropNextNs=%d droppableLen=%d",
			q.codelq.count, q.codelq.dropNextNs, q.codelq.droppableLen)
	}
}
