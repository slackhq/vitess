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
