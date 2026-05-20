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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSelfAware(clock *testClock) (*SelfContentionAwareCoDelQueue, *testDropTimerRecorder) {
	rec := &testDropTimerRecorder{}
	q := newSelfContentionAwareCoDelQueue(defaultTestConfig(), clock.nowFunc, rec.schedule, rec.stop)
	return q, rec
}

// --- Direct entry tests ---

func TestSelfAware_FirstRequest_DirectEntry(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	req := sq.lockedEnqueue("id1", NewPriority(0))

	assert.NotNil(t, req)
	assert.Equal(t, 1, sq.lockedLen())
	assert.NotNil(t, req.elem, "should be in the CoDel queue (has list element)")
	assert.Equal(t, 1, sq.outstandingCounts["id1"])
}

func TestSelfAware_EmptyValveID_AlwaysDirect(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	r1 := sq.lockedEnqueue("", NewPriority(0))
	r2 := sq.lockedEnqueue("", NewPriority(0))

	assert.NotNil(t, r1.elem, "empty ID always goes to CoDel queue")
	assert.NotNil(t, r2.elem, "empty ID always goes to CoDel queue")
	assert.Equal(t, 2, sq.lockedLen())
}

// --- Valve tests ---

func TestSelfAware_SecondRequest_Valved(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	r1 := sq.lockedEnqueue("id1", NewPriority(0))
	r2 := sq.lockedEnqueue("id1", NewPriority(0))

	assert.NotNil(t, r1.elem, "first enters CoDel queue")
	assert.Nil(t, r2.elem, "second should be in valve (no list element)")
	assert.Equal(t, 1, sq.lockedLen(), "only 1 in CoDel queue")
	assert.Equal(t, 2, sq.outstandingCounts["id1"])
	require.Len(t, sq.pendingRequests["id1"], 1)
	assert.Same(t, r2, sq.pendingRequests["id1"][0])
}

func TestSelfAware_DifferentIDs_Independent(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	r1 := sq.lockedEnqueue("id1", NewPriority(0))
	r2 := sq.lockedEnqueue("id2", NewPriority(0))

	assert.NotNil(t, r1.elem, "id1 in CoDel queue")
	assert.NotNil(t, r2.elem, "id2 in CoDel queue (different ID)")
	assert.Equal(t, 2, sq.lockedLen())
}

func TestSelfAware_FourParallel_SameID(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	r1 := sq.lockedEnqueue("id1", NewPriority(0))
	r2 := sq.lockedEnqueue("id1", NewPriority(0))
	r3 := sq.lockedEnqueue("id1", NewPriority(0))
	r4 := sq.lockedEnqueue("id1", NewPriority(0))

	assert.NotNil(t, r1.elem, "first in CoDel queue")
	assert.Nil(t, r2.elem, "second in valve")
	assert.Nil(t, r3.elem, "third in valve")
	assert.Nil(t, r4.elem, "fourth in valve")

	assert.Equal(t, 1, sq.lockedLen())
	assert.Len(t, sq.pendingRequests["id1"], 3)
	assert.Equal(t, 4, sq.outstandingCounts["id1"])
}

// --- Promotion tests ---

func TestSelfAware_Promotion_OnDequeue(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	sq.lockedEnqueue("id1", NewPriority(0))
	r2 := sq.lockedEnqueue("id1", NewPriority(0))

	assert.Nil(t, r2.elem, "r2 in valve before dequeue")

	d := sq.lockedDequeue()
	assert.NotNil(t, d)

	assert.NotNil(t, r2.elem, "r2 promoted to CoDel queue after dequeue")
	assert.Equal(t, 1, sq.lockedLen())
	assert.Empty(t, sq.pendingRequests["id1"])
	assert.Equal(t, 1, sq.outstandingCounts["id1"])
}

func TestSelfAware_Promotion_OnDrop(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	r1 := sq.lockedEnqueue("id1", NewPriority(0))
	r2 := sq.lockedEnqueue("id1", NewPriority(0))

	sq.lockedDropActive(r1)

	assert.NotNil(t, r2.elem, "r2 promoted after r1 dropped")
	assert.Equal(t, 1, sq.lockedLen())
}

func TestSelfAware_Promotion_OnCancel(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	r1 := sq.lockedEnqueue("id1", NewPriority(0))
	r2 := sq.lockedEnqueue("id1", NewPriority(0))

	sq.lockedCancel(r1)

	assert.NotNil(t, r2.elem, "r2 promoted after r1 cancelled")
	assert.Equal(t, 1, sq.lockedLen())
	assert.Equal(t, 1, sq.outstandingCounts["id1"])
}

// --- Cancel tests ---

func TestSelfAware_CancelInValve(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	r1 := sq.lockedEnqueue("id1", NewPriority(0))
	sq.lockedEnqueue("id1", NewPriority(0))
	r3 := sq.lockedEnqueue("id1", NewPriority(0))
	sq.lockedEnqueue("id1", NewPriority(0))

	sq.lockedCancel(r3)

	assert.NotNil(t, r1.elem, "r1 still in CoDel queue")
	assert.Equal(t, 1, sq.lockedLen())
	// r3 is signaled in place but not removed from the slice until promotion
	assert.Len(t, sq.pendingRequests["id1"], 3)
	// outstanding count is not decremented until clearDone runs during promotion
	assert.Equal(t, 4, sq.outstandingCounts["id1"])
	assert.NotNil(t, r3.signaledValue, "r3 should be signaled")
}

func TestSelfAware_ClearDone_InValve(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	sq.lockedEnqueue("id1", NewPriority(0))
	r2 := sq.lockedEnqueue("id1", NewPriority(0))
	r3 := sq.lockedEnqueue("id1", NewPriority(0))

	// mark r2 as done (cancelled while in valve)
	r2.signal(&DroppedRequestError{})

	// dequeue r1 → promote should skip r2 (done) and promote r3
	sq.lockedDequeue()

	assert.NotNil(t, r3.elem, "r3 promoted (r2 was skipped)")
	assert.Equal(t, 1, sq.lockedLen())
}

func TestSelfAware_CancelInMiddle_EventualPromotion(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	sq.lockedEnqueue("id1", NewPriority(0))       // r1: active in CoDel
	r2 := sq.lockedEnqueue("id1", NewPriority(0)) // r2: valve[0]
	r3 := sq.lockedEnqueue("id1", NewPriority(0)) // r3: valve[1]
	r4 := sq.lockedEnqueue("id1", NewPriority(0)) // r4: valve[2]

	// Cancel r3 in the middle of the valve
	sq.lockedCancel(r3)

	// Dequeue r1 → promotes r2 (r3 is in the middle, not at head)
	sq.lockedDequeue()
	assert.NotNil(t, r2.elem, "r2 promoted")

	// Dequeue r2 → clearDone finds r3 (now at head), skips it, promotes r4
	sq.lockedDequeue()
	assert.NotNil(t, r4.elem, "r4 promoted (r3 skipped)")
	assert.Equal(t, 1, sq.outstandingCounts["id1"])
}

// --- Outstanding count tests ---

func TestSelfAware_OutstandingCount_Lifecycle(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	sq.lockedEnqueue("id1", NewPriority(0))
	assert.Equal(t, 1, sq.outstandingCounts["id1"])

	sq.lockedEnqueue("id1", NewPriority(0))
	assert.Equal(t, 2, sq.outstandingCounts["id1"])

	sq.lockedDequeue() // removes first, promotes second
	assert.Equal(t, 1, sq.outstandingCounts["id1"])

	sq.lockedDequeue() // removes second
	assert.Equal(t, 0, sq.outstandingCounts["id1"])
}

func TestSelfAware_OutstandingCount_SurvivesCancel(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	r1 := sq.lockedEnqueue("id1", NewPriority(0))
	sq.lockedEnqueue("id1", NewPriority(0))
	assert.Equal(t, 2, sq.outstandingCounts["id1"])

	sq.lockedCancel(r1)
	assert.Equal(t, 1, sq.outstandingCounts["id1"])
}

func TestSelfAware_EmptyValve_MapCleanup(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	sq.lockedEnqueue("id1", NewPriority(0))
	sq.lockedEnqueue("id1", NewPriority(0))

	sq.lockedDequeue() // removes first, promotes second
	sq.lockedDequeue() // removes second

	_, exists := sq.pendingRequests["id1"]
	assert.False(t, exists, "empty valve should be removed from map")
}

// --- Peek cleanup tests ---

// TestSelfAware_PeekCleanup_DecrementsOutstandingCount proves that when
// lockedPeek defensively removes a done-with-error request from the CoDel
// queue head, outstanding counts are decremented correctly.
func TestSelfAware_PeekCleanup_DecrementsOutstandingCount(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	r1 := sq.lockedEnqueue("id1", NewPriority(0))
	sq.lockedEnqueue("id1", NewPriority(0))

	assert.Equal(t, 2, sq.outstandingCounts["id1"])

	// Simulate the "impossible" state: signal r1 with error without calling
	// lockedRemove. This leaves elem non-nil, so lockedPeek will find it as
	// isDone() with non-nil outcome and clean it up.
	r1.signal(&DroppedRequestError{})

	// lockedPeek should remove r1 and decrement outstanding count
	result := sq.lockedPeek()
	assert.Nil(t, result, "r2 is in valve not CoDel queue, so peek returns nil after r1 cleanup")
	assert.Equal(t, 1, sq.outstandingCounts["id1"], "outstanding should be decremented for cleaned-up request")
}

// TestSelfAware_PeekCleanup_DecrementsDroppableLen proves that when
// lockedPeek removes a done droppable request, droppableLen is decremented.
func TestSelfAware_PeekCleanup_DecrementsDroppableLen(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	r1 := sq.lockedEnqueue("id1", NewPriority(0))
	r2 := sq.lockedEnqueue("id2", NewPriority(0))

	assert.Equal(t, 2, sq.codelq.droppableLen)

	// Signal r1 without removing it from the list
	r1.signal(&DroppedRequestError{})

	// lockedPeek should clean up r1 and decrement droppableLen
	result := sq.lockedPeek()
	assert.Same(t, r2, result)
	assert.Equal(t, 1, sq.codelq.droppableLen, "droppableLen should be decremented for cleaned-up request")
}

// --- FIFO within contention ---

func TestSelfAware_FIFO_WithinContention(t *testing.T) {
	clock := newTestClock()
	sq, _ := newTestSelfAware(clock)

	r1 := sq.lockedEnqueue("id1", NewPriority(0))
	r2 := sq.lockedEnqueue("id1", NewPriority(0))
	r3 := sq.lockedEnqueue("id1", NewPriority(0))

	d1 := sq.lockedDequeue()
	d2 := sq.lockedDequeue()
	d3 := sq.lockedDequeue()

	assert.Same(t, r1, d1)
	assert.Same(t, r2, d2)
	assert.Same(t, r3, d3)
}
