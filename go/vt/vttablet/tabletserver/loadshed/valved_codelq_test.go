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

func newTestValvedCoDelQueue() *ValvedCoDelQueue[string] {
	return newValvedCoDelQueue[string](
		CoDelConfig{
			IntervalNs:     func() int64 { return (10 * time.Millisecond).Nanoseconds() },
			TargetNs:       func() int64 { return time.Millisecond.Nanoseconds() },
			Exponent:       func() float64 { return 1 },
			MinDropDelayNs: func() int64 { return 1 },
		},
		func() int64 { return 0 },
		func(int64) {},
		func() {},
		func() Mode { return ModeEnabled },
	)
}

func TestValvedCoDelQueueSerializesSameValve(t *testing.T) {
	q := newTestValvedCoDelQueue()

	first := q.lockedEnqueue("valve", 0)
	second := q.lockedEnqueue("valve", 0)
	other := q.lockedEnqueue("other", 0)

	require.NotNil(t, first.codelqElem)
	assert.Nil(t, second.codelqElem)
	require.NotNil(t, other.codelqElem)
	assert.Equal(t, 2, q.lockedLen())

	q.lockedDequeue(first)

	require.NotNil(t, second.codelqElem)
	assert.Equal(t, 2, q.lockedLen())
}

func TestValvedCoDelQueueEmptyValveIDBypassesValve(t *testing.T) {
	q := newTestValvedCoDelQueue()

	first := q.lockedEnqueue("", 0)
	second := q.lockedEnqueue("", 0)

	require.NotNil(t, first.codelqElem)
	require.NotNil(t, second.codelqElem)
	assert.Equal(t, 2, q.lockedLen())
}

func TestValvedCoDelQueueCancelPendingDoesNotBypassActive(t *testing.T) {
	q := newTestValvedCoDelQueue()

	active := q.lockedEnqueue("valve", 0)
	cancelled := q.lockedEnqueue("valve", 0)
	q.lockedCancel(cancelled)
	next := q.lockedEnqueue("valve", 0)

	assert.Nil(t, next.codelqElem)
	q.lockedDequeue(active)
	require.NotNil(t, next.codelqElem)
	assert.Equal(t, 1, q.lockedLen())
}

func TestValvedCoDelQueueCancelActivePromotesNext(t *testing.T) {
	q := newTestValvedCoDelQueue()

	active := q.lockedEnqueue("valve", 0)
	next := q.lockedEnqueue("valve", 0)

	q.lockedCancel(active)

	require.NotNil(t, next.codelqElem)
	assert.Equal(t, 1, q.lockedLen())
}

func TestValvedCoDelQueueDropPromotesNext(t *testing.T) {
	q := newTestValvedCoDelQueue()

	active := q.lockedEnqueue("valve", 0)
	next := q.lockedEnqueue("valve", 0)

	q.lockedDrop(active)

	require.NotNil(t, next.codelqElem)
	assert.Equal(t, 1, q.lockedLen())
	assert.Equal(t, []*Request[string]{active}, q.lockedTakePendingDrops())
}
