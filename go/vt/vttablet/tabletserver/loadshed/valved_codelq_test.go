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

type testValvedCoDelQueue = ValvedCoDelQueue[struct{}]

func newValvedQueue(clock *testClock) (*testValvedCoDelQueue, *testDropTimerRecorder) {
	rec := &testDropTimerRecorder{}
	q := newValvedCoDelQueue[struct{}](defaultTestConfig(), clock.nowFunc, rec.schedule, rec.stop)
	return q, rec
}

func testValvedDequeue(sq *testValvedCoDelQueue) *testRequest {
	req := sq.lockedFirstWaiting()
	if req == nil {
		return nil
	}
	sq.lockedDequeue(req)
	return req
}

// --- Direct entry tests ---

func TestValved_FirstRequest_DirectEntry(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	req := sq.lockedEnqueue("id1", 0)

	assert.NotNil(t, req)
	assert.Equal(t, 1, sq.lockedLen())
	assert.NotNil(t, req.codelqElem, "should be in the CoDel queue (has list element)")
}

func TestValved_EmptyValveID_AlwaysDirect(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	r1 := sq.lockedEnqueue("", 0)
	r2 := sq.lockedEnqueue("", 0)

	assert.NotNil(t, r1.codelqElem, "empty ID always goes to CoDel queue")
	assert.NotNil(t, r2.codelqElem, "empty ID always goes to CoDel queue")
	assert.Equal(t, 2, sq.lockedLen())
}

// --- Valve tests ---

func TestValved_SecondRequest_Valved(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	r1 := sq.lockedEnqueue("id1", 0)
	r2 := sq.lockedEnqueue("id1", 0)

	assert.NotNil(t, r1.codelqElem, "first enters CoDel queue")
	assert.Nil(t, r2.codelqElem, "second should be in valve (no list element)")
	assert.Equal(t, 1, sq.lockedLen(), "only 1 in CoDel queue")
	require.Len(t, sq.valves["id1"], 1)
	assert.Same(t, r2, sq.valves["id1"][0])
}

func TestValved_DifferentIDs_Independent(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	r1 := sq.lockedEnqueue("id1", 0)
	r2 := sq.lockedEnqueue("id2", 0)

	assert.NotNil(t, r1.codelqElem, "id1 in CoDel queue")
	assert.NotNil(t, r2.codelqElem, "id2 in CoDel queue (different ID)")
	assert.Equal(t, 2, sq.lockedLen())
}

func TestValved_FourParallel_SameID(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	r1 := sq.lockedEnqueue("id1", 0)
	r2 := sq.lockedEnqueue("id1", 0)
	r3 := sq.lockedEnqueue("id1", 0)
	r4 := sq.lockedEnqueue("id1", 0)

	assert.NotNil(t, r1.codelqElem, "first in CoDel queue")
	assert.Nil(t, r2.codelqElem, "second in valve")
	assert.Nil(t, r3.codelqElem, "third in valve")
	assert.Nil(t, r4.codelqElem, "fourth in valve")

	assert.Equal(t, 1, sq.lockedLen())
	assert.Len(t, sq.valves["id1"], 3)
}

// --- Promotion tests ---

func TestValved_Promotion_OnDequeue(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	sq.lockedEnqueue("id1", 0)
	r2 := sq.lockedEnqueue("id1", 0)

	assert.Nil(t, r2.codelqElem, "r2 in valve before dequeue")

	d := testValvedDequeue(sq)
	assert.NotNil(t, d)

	assert.NotNil(t, r2.codelqElem, "r2 promoted to CoDel queue after dequeue")
	assert.Equal(t, 1, sq.lockedLen())
	assert.Empty(t, sq.valves["id1"])
}

func TestValved_Promotion_OnDrop(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	r1 := sq.lockedEnqueue("id1", 0)
	r2 := sq.lockedEnqueue("id1", 0)

	sq.lockedDrop(r1)

	assert.NotNil(t, r2.codelqElem, "r2 promoted after r1 dropped")
	assert.Equal(t, 1, sq.lockedLen())
}

func TestValved_Promotion_OnCancel(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	r1 := sq.lockedEnqueue("id1", 0)
	r2 := sq.lockedEnqueue("id1", 0)

	sq.lockedCancel(r1)

	assert.NotNil(t, r2.codelqElem, "r2 promoted after r1 cancelled")
	assert.Equal(t, 1, sq.lockedLen())
}

// --- Cancel tests ---

func TestValved_CancelInValve(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	r1 := sq.lockedEnqueue("id1", 0)
	sq.lockedEnqueue("id1", 0)
	r3 := sq.lockedEnqueue("id1", 0)
	sq.lockedEnqueue("id1", 0)

	sq.lockedCancel(r3)

	assert.NotNil(t, r1.codelqElem, "r1 still in CoDel queue")
	assert.Equal(t, 1, sq.lockedLen())
	assert.Len(t, sq.valves["id1"], 3)
	assert.False(t, r3.queued)
}

func TestValved_ClearDone_InValve(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	sq.lockedEnqueue("id1", 0)
	r2 := sq.lockedEnqueue("id1", 0)
	r3 := sq.lockedEnqueue("id1", 0)

	sq.lockedCancel(r2)

	// dequeue r1 → promote should skip r2 (done) and promote r3
	testValvedDequeue(sq)

	assert.NotNil(t, r3.codelqElem, "r3 promoted (r2 was skipped)")
	assert.Equal(t, 1, sq.lockedLen())
}

func TestValved_CancelInMiddle_EventualPromotion(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	sq.lockedEnqueue("id1", 0)       // r1: active in CoDel
	r2 := sq.lockedEnqueue("id1", 0) // r2: valve[0]
	r3 := sq.lockedEnqueue("id1", 0) // r3: valve[1]
	r4 := sq.lockedEnqueue("id1", 0) // r4: valve[2]

	// Cancel r3 in the middle of the valve
	sq.lockedCancel(r3)

	// Dequeue r1 → promotes r2 (r3 is in the middle, not at head)
	testValvedDequeue(sq)
	assert.NotNil(t, r2.codelqElem, "r2 promoted")

	// Dequeue r2 → clearDone finds r3 (now at head), skips it, promotes r4
	testValvedDequeue(sq)
	assert.NotNil(t, r4.codelqElem, "r4 promoted (r3 skipped)")
}

func TestValved_CancelMultipleConsecutiveAtHead(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	sq.lockedEnqueue("id1", 0)       // r1: active in CoDel
	r2 := sq.lockedEnqueue("id1", 0) // r2: valve[0]
	r3 := sq.lockedEnqueue("id1", 0) // r3: valve[1]
	r4 := sq.lockedEnqueue("id1", 0) // r4: valve[2]
	r5 := sq.lockedEnqueue("id1", 0) // r5: valve[3]

	// Cancel r2 and r3 (the first two in the valve)
	sq.lockedCancel(r2)
	sq.lockedCancel(r3)

	// Dequeue r1 → clearDone should skip both r2 and r3, promote r4
	testValvedDequeue(sq)
	assert.NotNil(t, r4.codelqElem, "r4 promoted (r2 and r3 skipped)")
	assert.Nil(t, r2.codelqElem, "r2 never entered CoDel queue")
	assert.Nil(t, r3.codelqElem, "r3 never entered CoDel queue")

	// Dequeue r4 → promotes r5
	testValvedDequeue(sq)
	assert.NotNil(t, r5.codelqElem, "r5 promoted")
}

func TestValved_AllValveEntriesCancelled(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	sq.lockedEnqueue("id1", 0)       // r1: active in CoDel
	r2 := sq.lockedEnqueue("id1", 0) // r2: valve[0]
	r3 := sq.lockedEnqueue("id1", 0) // r3: valve[1]
	r4 := sq.lockedEnqueue("id1", 0) // r4: valve[2]

	// Cancel everything in the valve
	sq.lockedCancel(r2)
	sq.lockedCancel(r3)
	sq.lockedCancel(r4)

	// Dequeue r1 → clearDone drains the entire valve, nothing to promote
	testValvedDequeue(sq)

	assert.Equal(t, 0, sq.lockedLen(), "CoDel queue empty")
	_, exists := sq.valves["id1"]
	assert.False(t, exists, "valve map entry should be cleaned up")
}

func TestValved_CancelledWaiterDoesNotBypassValve(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	sq.lockedEnqueue("id1", 0)       // r1: active in CoDel
	r2 := sq.lockedEnqueue("id1", 0) // r2: valve[0]

	sq.lockedCancel(r2)

	// A new arrival for the same valve ID remains valved behind r1.
	r3 := sq.lockedEnqueue("id1", 0)
	assert.Nil(t, r3.codelqElem, "r3 should be valved")
	assert.Equal(t, 1, sq.lockedLen(), "still only r1 in CoDel queue")
}

func TestValved_CancelAllThenNewArrival(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	sq.lockedEnqueue("id1", 0)       // r1: active in CoDel
	r2 := sq.lockedEnqueue("id1", 0) // r2: valve[0]
	r3 := sq.lockedEnqueue("id1", 0) // r3: valve[1]

	// Cancel both valve entries
	sq.lockedCancel(r2)
	sq.lockedCancel(r3)

	// Dequeue r1 → clearDone drains valve, queue empties
	testValvedDequeue(sq)
	assert.Equal(t, 0, sq.lockedLen())

	// Fresh arrival for same valve ID should go directly to CoDel (no stale state)
	r4 := sq.lockedEnqueue("id1", 0)
	assert.NotNil(t, r4.codelqElem, "r4 goes directly to CoDel after full cleanup")
}

func TestValved_CancelInterleavedWithPromotions(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	sq.lockedEnqueue("id1", 0)       // r1: active in CoDel
	r2 := sq.lockedEnqueue("id1", 0) // r2: valve[0]
	r3 := sq.lockedEnqueue("id1", 0) // r3: valve[1]
	r4 := sq.lockedEnqueue("id1", 0) // r4: valve[2]
	r5 := sq.lockedEnqueue("id1", 0) // r5: valve[3]
	r6 := sq.lockedEnqueue("id1", 0) // r6: valve[4]

	// Cancel alternating: r3 and r5
	sq.lockedCancel(r3)
	sq.lockedCancel(r5)

	// Dequeue r1 → promotes r2 (head is live)
	testValvedDequeue(sq)
	assert.NotNil(t, r2.codelqElem, "r2 promoted")

	// Dequeue r2 → clearDone hits r3 (cancelled at head), skips it, promotes r4
	testValvedDequeue(sq)
	assert.NotNil(t, r4.codelqElem, "r4 promoted (r3 skipped)")

	// Dequeue r4 → clearDone hits r5 (cancelled at head), skips it, promotes r6
	testValvedDequeue(sq)
	assert.NotNil(t, r6.codelqElem, "r6 promoted (r5 skipped)")
}

func TestValved_MassCancel_OverloadScenario(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	sq.lockedEnqueue("id1", 0) // r0: active in CoDel

	// Simulate overload: 50 requests arrive for same valve ID
	requests := make([]*testRequest, 50)
	for i := range 50 {
		requests[i] = sq.lockedEnqueue("id1", 0)
	}
	assert.Equal(t, 1, sq.lockedLen())

	// Cancel all but the last 5 (simulating context timeouts in overload)
	for i := range 45 {
		sq.lockedCancel(requests[i])
	}

	// Dequeue the active entry → clearDone should efficiently skip the 45
	// cancelled entries at the head and promote the first live one
	testValvedDequeue(sq)
	assert.NotNil(t, requests[45].codelqElem, "first surviving request promoted")

	// Drain remaining 5
	for i := 45; i < 50; i++ {
		d := testValvedDequeue(sq)
		assert.NotNil(t, d)
		if i < 49 {
			assert.NotNil(t, requests[i+1].codelqElem, "next request promoted")
		}
	}

	_, exists := sq.valves["id1"]
	assert.False(t, exists, "valve map cleaned up")
}

func TestValved_CancelFromValve_DoesNotAffectOtherValveIDs(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	// Two valve IDs with parallel requests
	sq.lockedEnqueue("id1", 0)        // id1 active
	r1v := sq.lockedEnqueue("id1", 0) // id1 valve[0]
	sq.lockedEnqueue("id2", 0)        // id2 active
	r2v := sq.lockedEnqueue("id2", 0) // id2 valve[0]

	// Cancel id1's valve entry
	sq.lockedCancel(r1v)

	// id2's valve should be completely unaffected
	assert.Len(t, sq.valves["id2"], 1)
	assert.Same(t, r2v, sq.valves["id2"][0])
	assert.True(t, r2v.queued)
}

func TestValved_EmptyValve_MapCleanup(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	sq.lockedEnqueue("id1", 0)
	sq.lockedEnqueue("id1", 0)

	testValvedDequeue(sq) // removes first, promotes second
	testValvedDequeue(sq) // removes second

	_, exists := sq.valves["id1"]
	assert.False(t, exists, "empty valve should be removed from map")
}

// --- FIFO within contention ---

func TestValved_FIFO_WithinContention(t *testing.T) {
	clock := newTestClock()
	sq, _ := newValvedQueue(clock)

	r1 := sq.lockedEnqueue("id1", 0)
	r2 := sq.lockedEnqueue("id1", 0)
	r3 := sq.lockedEnqueue("id1", 0)

	d1 := testValvedDequeue(sq)
	d2 := testValvedDequeue(sq)
	d3 := testValvedDequeue(sq)

	assert.Same(t, r1, d1)
	assert.Same(t, r2, d2)
	assert.Same(t, r3, d3)
}
