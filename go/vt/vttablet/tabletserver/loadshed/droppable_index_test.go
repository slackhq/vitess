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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDroppableIndexRequest(priority float64) *Request[struct{}] {
	return newRequest(struct{}{}, priority)
}

func TestDroppableIndexEmpty(t *testing.T) {
	var index droppableIndex[struct{}]
	index.init()

	assert.Nil(t, index.min())
}

func TestDroppableIndexReturnsLowestPriorityFIFO(t *testing.T) {
	var index droppableIndex[string]
	index.init()

	high := newRequest("high", 100)
	firstLow := newRequest("first-low", 0)
	secondLow := newRequest("second-low", 0)
	index.insert(high)
	index.insert(firstLow)
	index.insert(secondLow)

	require.Same(t, firstLow, index.min())
	index.remove(firstLow)
	require.Same(t, secondLow, index.min())
	index.remove(secondLow)
	assert.Same(t, high, index.min())
}

func TestDroppableIndexRemoveMiddle(t *testing.T) {
	var index droppableIndex[struct{}]
	index.init()
	low := newDroppableIndexRequest(1)
	middle := newDroppableIndexRequest(5)
	high := newDroppableIndexRequest(10)
	index.insert(low)
	index.insert(middle)
	index.insert(high)

	index.remove(middle)

	assert.Same(t, low, index.min())
	index.remove(low)
	assert.Same(t, high, index.min())
}

func TestDroppableIndexRemoveEmptiesBucket(t *testing.T) {
	var index droppableIndex[struct{}]
	index.init()
	low := newDroppableIndexRequest(1)
	high := newDroppableIndexRequest(50)
	index.insert(low)
	index.insert(high)

	index.remove(low)
	assert.Same(t, high, index.min())
	index.remove(high)
	assert.Nil(t, index.min())
}

func TestDroppableIndexDomainBoundaries(t *testing.T) {
	var index droppableIndex[struct{}]
	index.init()
	highest := newDroppableIndexRequest(100)
	lowest := newDroppableIndexRequest(0)
	index.insert(highest)
	index.insert(lowest)

	assert.Same(t, lowest, index.min())
	index.remove(lowest)
	assert.Same(t, highest, index.min())
}

func TestDroppableIndexOverflowAfterBuckets(t *testing.T) {
	var index droppableIndex[struct{}]
	index.init()
	overflow := newDroppableIndexRequest(math.Inf(1))
	inDomain := newDroppableIndexRequest(5)
	index.insert(overflow)
	index.insert(inDomain)

	assert.Same(t, inDomain, index.min())
	index.remove(inDomain)
	assert.Same(t, overflow, index.min())
}

func TestDroppableIndexOverflowFIFO(t *testing.T) {
	var index droppableIndex[struct{}]
	index.init()
	first := newDroppableIndexRequest(1.5)
	second := newDroppableIndexRequest(101)
	index.insert(first)
	index.insert(second)

	assert.Same(t, first, index.min())
	index.remove(first)
	assert.Same(t, second, index.min())
}

func TestDroppableIndexSecondWordBoundary(t *testing.T) {
	var index droppableIndex[struct{}]
	index.init()
	priority64 := newDroppableIndexRequest(64)
	priority63 := newDroppableIndexRequest(63)
	index.insert(priority64)

	assert.Same(t, priority64, index.min())
	index.insert(priority63)
	assert.Same(t, priority63, index.min())
}
