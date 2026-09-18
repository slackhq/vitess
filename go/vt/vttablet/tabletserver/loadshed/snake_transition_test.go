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

	_, dropped := snake.Enqueue("queued", "", 0)

	assert.Empty(t, dropped)
	assert.Equal(t, ModeOff, snake.mode())
	assert.Zero(t, snake.q.codelq.dropNextNs)
}

func TestSnakeCancelMatchingFindsValveWaiter(t *testing.T) {
	snake := NewSnake[string](defaultSnakeConfig())
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

func TestSnakeEnqueueExistingDoesNotCountAcquire(t *testing.T) {
	snake := NewSnake[string](defaultSnakeConfig())
	exporter := newFakeExporter()
	PublishStats(exporter, "SnakeTest", snake)

	snake.Enqueue("new", "", 100)
	snake.EnqueueExisting("existing", "", PriorityUndroppable)

	assert.Equal(t, map[string]int64{"0": 1}, exporter.multiCounters["SnakeTestAcquireByPriority"].Counts())
}

func TestSnakeShedByPriorityMetric(t *testing.T) {
	snake := NewSnake[string](defaultSnakeConfig())
	exporter := newFakeExporter()
	PublishStats(exporter, "SnakeTest", snake)

	snake.droppedValues([]*Request[string]{
		newRequest[string](100),
		newRequest[string](50),
		newRequest[string](0),
	})

	require.Contains(t, exporter.multiCounters, "SnakeTestShedByPriority")
	assert.Equal(t, map[string]int64{
		"0":   1,
		"50":  1,
		"100": 1,
	}, exporter.multiCounters["SnakeTestShedByPriority"].Counts())
}
