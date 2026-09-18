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

func TestSnakeCancelMatching(t *testing.T) {
	snake := NewSnake[string](SnakeConfig{})
	snake.Enqueue("first", "", 0)
	snake.Enqueue("second", "", 0)

	removed, dropped := snake.CancelMatching(func(value string) bool {
		return value == "second"
	})

	require.True(t, removed)
	assert.Empty(t, dropped)
	assert.Equal(t, 1, snake.Len())
}

func TestSnakeCancelMatchingFindsValveWaiter(t *testing.T) {
	snake := NewSnake[string](SnakeConfig{})
	snake.Enqueue("active", "valve", 0)
	snake.Enqueue("pending", "valve", 0)

	removed, dropped := snake.CancelMatching(func(value string) bool {
		return value == "pending"
	})

	require.True(t, removed)
	assert.Empty(t, dropped)
	assert.Equal(t, 1, snake.Len())
	value, ok, dropped := snake.Dequeue()
	require.True(t, ok)
	assert.Equal(t, "active", value)
	assert.Empty(t, dropped)
}

func TestSnakeDrain(t *testing.T) {
	snake := NewSnake[string](SnakeConfig{})
	snake.EnqueueExisting("first", "", PriorityUndroppable)
	snake.EnqueueExisting("second", "", PriorityUndroppable)

	assert.Equal(t, []string{"first", "second"}, snake.Drain())
	assert.Zero(t, snake.Len())
}

func TestSnakeDrainPromotesValveWaiters(t *testing.T) {
	snake := NewSnake[string](SnakeConfig{})
	snake.EnqueueExisting("first", "valve", PriorityUndroppable)
	snake.EnqueueExisting("second", "valve", PriorityUndroppable)

	assert.Equal(t, []string{"first", "second"}, snake.Drain())
	assert.Zero(t, snake.Len())
}

func TestSnakeEnqueueExistingDoesNotCountAcquire(t *testing.T) {
	snake := NewSnake[string](SnakeConfig{})
	exporter := newFakeExporter()
	PublishStats(exporter, "SnakeTest", snake)

	snake.Enqueue("new", "", 100)
	snake.EnqueueExisting("existing", "", PriorityUndroppable)

	require.Contains(t, exporter.multiCounters, "SnakeTestAcquireByPriority")
	assert.Equal(t, map[string]int64{"0": 1}, exporter.multiCounters["SnakeTestAcquireByPriority"].Counts())
}

func TestSnakeShedByPriorityMetric(t *testing.T) {
	snake := NewSnake[string](SnakeConfig{})
	exporter := newFakeExporter()
	PublishStats(exporter, "SnakeTest", snake)

	snake.droppedValues([]*Request[string]{
		newRequest("highest", 100),
		newRequest("middle", 50),
		newRequest("lowest", 0),
	})

	require.Contains(t, exporter.multiCounters, "SnakeTestShedByPriority")
	assert.Equal(t, map[string]int64{
		"0":   1,
		"50":  1,
		"100": 1,
	}, exporter.multiCounters["SnakeTestShedByPriority"].Counts())
}
