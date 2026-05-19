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

func newTestSelfAware(clock *testClock) *SelfContentionAwareCoDelQueue {
	return newSelfContentionAwareCoDelQueue(defaultTestConfig(), clock.nowFunc)
}

// --- Direct entry tests ---

func TestSelfAware_FirstRequest_DirectEntry(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	req, _, _ := sq.lockedEnqueue("id1", NewPriority(0))

	assert.NotNil(t, req)
	assert.Equal(t, 1, sq.lockedLen())
	assert.NotNil(t, req.elem, "should be in the CoDel queue (has list element)")
	assert.Equal(t, 1, sq.outstandingCounts["id1"])
}

func TestSelfAware_EmptyContentionID_AlwaysDirect(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	r1, _, _ := sq.lockedEnqueue("", NewPriority(0))
	r2, _, _ := sq.lockedEnqueue("", NewPriority(0))

	assert.NotNil(t, r1.elem, "empty ID always goes to CoDel queue")
	assert.NotNil(t, r2.elem, "empty ID always goes to CoDel queue")
	assert.Equal(t, 2, sq.lockedLen())
}

// --- Valve tests ---

func TestSelfAware_SecondRequest_Valved(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	r1, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	r2, _, _ := sq.lockedEnqueue("id1", NewPriority(0))

	assert.NotNil(t, r1.elem, "first enters CoDel queue")
	assert.Nil(t, r2.elem, "second should be in valve (no list element)")
	assert.Equal(t, 1, sq.lockedLen(), "only 1 in CoDel queue")
	assert.Equal(t, 2, sq.outstandingCounts["id1"])
	require.Len(t, sq.pendingRequests["id1"], 1)
	assert.Same(t, r2, sq.pendingRequests["id1"][0])
}

func TestSelfAware_DifferentIDs_Independent(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	r1, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	r2, _, _ := sq.lockedEnqueue("id2", NewPriority(0))

	assert.NotNil(t, r1.elem, "id1 in CoDel queue")
	assert.NotNil(t, r2.elem, "id2 in CoDel queue (different ID)")
	assert.Equal(t, 2, sq.lockedLen())
}

func TestSelfAware_FourParallel_SameID(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	r1, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	r2, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	r3, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	r4, _, _ := sq.lockedEnqueue("id1", NewPriority(0))

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
	sq := newTestSelfAware(clock)

	sq.lockedEnqueue("id1", NewPriority(0))
	r2, _, _ := sq.lockedEnqueue("id1", NewPriority(0))

	assert.Nil(t, r2.elem, "r2 in valve before dequeue")

	// dequeue r1: should promote r2 from valve to CoDel queue
	d, _, _ := sq.lockedDequeue()
	assert.NotNil(t, d)

	assert.NotNil(t, r2.elem, "r2 promoted to CoDel queue after dequeue")
	assert.Equal(t, 1, sq.lockedLen())
	assert.Empty(t, sq.pendingRequests["id1"])
	assert.Equal(t, 1, sq.outstandingCounts["id1"])
}

func TestSelfAware_Promotion_OnDrop(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	r1, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	r2, _, _ := sq.lockedEnqueue("id1", NewPriority(0))

	// simulate dropping r1 via popElem
	sq.lockedDropActive("id1", r1)

	assert.NotNil(t, r2.elem, "r2 promoted after r1 dropped")
	assert.Equal(t, 1, sq.lockedLen())
}

func TestSelfAware_Promotion_OnCancel(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	r1, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	r2, _, _ := sq.lockedEnqueue("id1", NewPriority(0))

	_, _ = sq.lockedCancel("id1", r1)

	assert.NotNil(t, r2.elem, "r2 promoted after r1 cancelled")
	assert.Equal(t, 1, sq.lockedLen())
	assert.Equal(t, 1, sq.outstandingCounts["id1"])
}

// --- Cancel tests ---

func TestSelfAware_CancelInValve(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	r1, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	_, _, _ = sq.lockedEnqueue("id1", NewPriority(0))
	r3, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	_, _, _ = sq.lockedEnqueue("id1", NewPriority(0))

	// cancel r3 from the valve
	_, _ = sq.lockedCancel("id1", r3)

	assert.NotNil(t, r1.elem, "r1 still in CoDel queue")
	assert.Equal(t, 1, sq.lockedLen())
	assert.Len(t, sq.pendingRequests["id1"], 2) // r2 and r4 remain
	assert.Equal(t, 3, sq.outstandingCounts["id1"])
}

func TestSelfAware_ClearDone_InValve(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	sq.lockedEnqueue("id1", NewPriority(0))
	r2, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	r3, _, _ := sq.lockedEnqueue("id1", NewPriority(0))

	// mark r2 as done (cancelled while in valve)
	r2.signal(&DroppedRequestError{})

	// dequeue r1 → promote should skip r2 (done) and promote r3
	_, _, _ = sq.lockedDequeue()

	assert.NotNil(t, r3.elem, "r3 promoted (r2 was skipped)")
	assert.Equal(t, 1, sq.lockedLen())
}

// --- Outstanding count tests ---

func TestSelfAware_OutstandingCount_Lifecycle(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	sq.lockedEnqueue("id1", NewPriority(0))
	assert.Equal(t, 1, sq.outstandingCounts["id1"])

	sq.lockedEnqueue("id1", NewPriority(0))
	assert.Equal(t, 2, sq.outstandingCounts["id1"])

	_, _, _ = sq.lockedDequeue() // removes first, promotes second
	assert.Equal(t, 1, sq.outstandingCounts["id1"])

	_, _, _ = sq.lockedDequeue() // removes second
	assert.Equal(t, 0, sq.outstandingCounts["id1"])
}

func TestSelfAware_OutstandingCount_SurvivesCancel(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	r1, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	sq.lockedEnqueue("id1", NewPriority(0))
	assert.Equal(t, 2, sq.outstandingCounts["id1"])

	_, _ = sq.lockedCancel("id1", r1)
	assert.Equal(t, 1, sq.outstandingCounts["id1"])
}

func TestSelfAware_EmptyValve_MapCleanup(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	sq.lockedEnqueue("id1", NewPriority(0))
	sq.lockedEnqueue("id1", NewPriority(0))

	_, _, _ = sq.lockedDequeue() // removes first, promotes second
	_, _, _ = sq.lockedDequeue() // removes second

	_, exists := sq.pendingRequests["id1"]
	assert.False(t, exists, "empty valve should be removed from map")
}

// --- FIFO within contention ---

func TestSelfAware_FIFO_WithinContention(t *testing.T) {
	clock := newTestClock()
	sq := newTestSelfAware(clock)

	r1, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	r2, _, _ := sq.lockedEnqueue("id1", NewPriority(0))
	r3, _, _ := sq.lockedEnqueue("id1", NewPriority(0))

	d1, _, _ := sq.lockedDequeue()
	d2, _, _ := sq.lockedDequeue()
	d3, _, _ := sq.lockedDequeue()

	assert.Same(t, r1, d1)
	assert.Same(t, r2, d2)
	assert.Same(t, r3, d3)
}
