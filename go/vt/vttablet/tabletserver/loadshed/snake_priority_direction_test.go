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

// TestSnake_PriorityDirection_MatchesVitess pins the direction of the proto
// priority mapping: in Vitess's ExecuteOptions convention a NUMERICALLY LOWER
// priority is MORE important and must be shed LAST, while a higher value is
// less important and shed first. Snake's internal CoDel key runs the opposite
// way (lowest key shed first), so Snake.priority inverts the proto value at a
// single boundary. This test drives two requests through that real boundary
// and asserts the less-important (proto-high) one is the one CoDel selects to
// drop, while the more-important (proto-low) one survives.
//
// If the inversion were ever dropped or reversed, the queue would shed the
// most important traffic first under load — this test fails loudly in that case.
func TestSnake_PriorityDirection_MatchesVitess(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	const (
		moreImportantProto = 0   // highest Vitess priority -> shed last
		lessImportantProto = 100 // lowest Vitess priority  -> shed first
	)

	moreImportantKey := s.priority(moreImportantProto)
	lessImportantKey := s.priority(lessImportantProto)

	// The boundary must map "more important" to a HIGHER internal key, because
	// CoDel sheds the lowest key first.
	assert.Greater(t, moreImportantKey, lessImportantKey,
		"more-important proto priority must yield a higher (shed-later) internal key")

	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	moreImportant := testEnqueue(q, moreImportantKey)
	lessImportant := testEnqueue(q, lessImportantKey)

	// Under contention, CoDel drops the lowest-priority droppable request.
	elem := q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, elem)
	assert.Same(t, lessImportant, elem.Value.(*Request),
		"the less-important request must be shed first")

	dropped := q.lockedPopElem(elem, &DroppedRequestError{})
	assert.Same(t, lessImportant, dropped)

	// The more-important request survives the drop.
	survivor := q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, survivor)
	assert.Same(t, moreImportant, survivor.Value.(*Request),
		"the more-important request must survive while less-important traffic is shed")
}

// TestSnake_PriorityDirection_OrdersAcrossRange confirms the mapping is
// monotonic across the proto range, not just at the extremes: a strictly lower
// (more important) proto priority is always shed after a higher one.
func TestSnake_PriorityDirection_OrdersAcrossRange(t *testing.T) {
	s := newTestSnake(defaultSnakeConfig())

	clock := newTestClock()
	q, _ := newTestQueue(defaultTestConfig(), clock)

	// Enqueue in arbitrary order; CoDel should still pick the least important.
	mid := testEnqueue(q, s.priority(50))
	least := testEnqueue(q, s.priority(90))
	most := testEnqueue(q, s.priority(10))

	// proto 90 is least important -> shed first.
	elem := q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, elem)
	assert.Same(t, least, elem.Value.(*Request))
	q.lockedPopElem(elem, &DroppedRequestError{})

	// proto 50 is next least important -> shed next.
	elem = q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, elem)
	assert.Same(t, mid, elem.Value.(*Request))
	q.lockedPopElem(elem, &DroppedRequestError{})

	// proto 10 is the most important of the three -> shed last.
	elem = q.lockedFindLowestPriorityDroppable()
	require.NotNil(t, elem)
	assert.Same(t, most, elem.Value.(*Request))
}
