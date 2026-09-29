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

func TestSnakeDefaultModeIsOff(t *testing.T) {
	cfg := defaultSnakeConfig()
	cfg.Mode = nil
	snake := NewSnake[string](cfg)

	_, dropped := snake.Enqueue("queued", "", 1, "")

	assert.Empty(t, dropped)
	assert.Equal(t, ModeOff, snake.mode())
	assert.Zero(t, snake.q.codelq.dropNextNs)
}

func TestSnakeCancelMatchingFindsValveWaiter(t *testing.T) {
	snake := NewSnake[string](defaultSnakeConfig())
	snake.Enqueue("active", "valve", 1, "")
	snake.Enqueue("pending", "valve", 1, "")

	removed := snake.CancelMatching(func(value string) bool {
		return value == "pending"
	})

	require.True(t, removed)
	assert.Equal(t, 1, snake.Len())
	value, ok, dropped := snake.Dequeue()
	require.True(t, ok)
	assert.Equal(t, "active", value)
	assert.Empty(t, dropped)
}

func TestSnakeLockedMaybeInheritPriority(t *testing.T) {
	snake := NewSnake[string](defaultSnakeConfig())
	low, _ := snake.Enqueue("low", "", 10, "low-key")
	mid, _ := snake.Enqueue("mid", "", 5, "mid-key")
	snake.Enqueue("unkeyed", "", 3, "")

	snake.LockedMaybeInheritPriority("low-key", 2)
	assert.Equal(t, 2, low.priority)
	assert.Equal(t, "mid", snake.q.codelq.droppable.max().value)

	snake.LockedMaybeInheritPriority("low-key", 20)
	assert.Equal(t, 2, low.priority)

	snake.LockedMaybeInheritPriority("missing-key", 50)
	assert.Equal(t, 2, low.priority)
	assert.Equal(t, 5, mid.priority)

	snake.LockedMaybeInheritPriority("mid-key", PriorityUndroppable)
	assert.Equal(t, 2, snake.q.lockedDroppableLen())
	assert.Equal(t, PriorityUndroppable, mid.priority)

	removed := snake.Cancel(low)
	require.True(t, removed)
	assert.NotContains(t, snake.q.priorityInheritors, "low-key")
	snake.LockedMaybeInheritPriority("low-key", 50)
	assert.Equal(t, 2, low.priority)

	_, ok, _ := snake.Dequeue()
	require.True(t, ok)
	assert.Empty(t, snake.q.priorityInheritors)
}

func TestSnakeLockedMaybeInheritPriorityPendingValve(t *testing.T) {
	snake := NewSnake[string](defaultSnakeConfig())
	snake.Enqueue("active", "valve", 5, "active-key")
	pending, _ := snake.Enqueue("pending", "valve", 10, "pending-key")

	require.Nil(t, pending.codelqElem)
	snake.LockedMaybeInheritPriority("pending-key", 2)
	assert.Equal(t, 2, pending.priority)

	_, ok, _ := snake.Dequeue()
	require.True(t, ok)
	require.NotNil(t, pending.codelqElem)
	assert.Equal(t, 2, pending.priority)
}

func TestSnakeDrain(t *testing.T) {
	snake := NewSnake[string](defaultSnakeConfig())
	snake.EnqueueExisting("first", "", PriorityUndroppable)
	snake.EnqueueExisting("second", "", PriorityUndroppable)

	assert.Equal(t, []string{"first", "second"}, snake.Drain())
	assert.Zero(t, snake.Len())
}

func TestSnakeEnqueueExistingDoesNotCountAcquire(t *testing.T) {
	snake := NewSnake[string](defaultSnakeConfig())
	exporter := newFakeExporter()
	PublishStats(exporter, "SnakeTest", snake)

	snake.Enqueue("new", "", 100, "")
	snake.EnqueueExisting("existing", "", PriorityUndroppable)

	require.Contains(t, exporter.multiCounters, "SnakeTestAcquireByPriority")
	assert.Equal(t, map[string]int64{"100": 1}, exporter.multiCounters["SnakeTestAcquireByPriority"].Counts())
}

func TestSnakeShedByPriorityMetric(t *testing.T) {
	snake := NewSnake[string](defaultSnakeConfig())
	exporter := newFakeExporter()
	PublishStats(exporter, "SnakeTest", snake)

	snake.droppedValues([]*Request[string]{
		newRequest("highest", 1),
		newRequest("middle", 50),
		newRequest("lowest", 100),
	})

	require.Contains(t, exporter.multiCounters, "SnakeTestShedByPriority")
	assert.Equal(t, map[string]int64{
		"1":   1,
		"50":  1,
		"100": 1,
	}, exporter.multiCounters["SnakeTestShedByPriority"].Counts())
}
