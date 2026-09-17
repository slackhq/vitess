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

type snakeTestClock struct {
	now int64
}

func (c *snakeTestClock) advance(d time.Duration) {
	c.now += d.Nanoseconds()
}

func newTestSnake[T any](mode Mode) (*Snake[T], *snakeTestClock) {
	clock := &snakeTestClock{}
	snake := NewSnake[T](SnakeConfig{
		Mode: func() Mode { return mode },
		CoDel: CoDelConfig{
			IntervalNs:        func() int64 { return (10 * time.Millisecond).Nanoseconds() },
			InitialIntervalNs: func() int64 { return (10 * time.Millisecond).Nanoseconds() },
			TargetNs:          func() int64 { return time.Millisecond.Nanoseconds() },
			InitialTargetNs:   func() int64 { return time.Millisecond.Nanoseconds() },
			Exponent:          func() float64 { return 1 },
			MinDropDelayNs:    func() int64 { return 1 },
		},
	})
	snake.clockFunc = func() int64 { return clock.now }
	snake.q.nowNs = snake.clockFunc
	return snake, clock
}

func TestSnakeQueue(t *testing.T) {
	snake, _ := newTestSnake[int](ModeEnabled)

	first, dropped := snake.Enqueue(1, 100)
	require.NotNil(t, first)
	assert.Empty(t, dropped)
	second, dropped := snake.Enqueue(2, 50)
	require.NotNil(t, second)
	assert.Empty(t, dropped)
	_, dropped = snake.Enqueue(3, 0)
	assert.Empty(t, dropped)
	assert.Equal(t, 3, snake.Len())

	value, ok, dropped := snake.DequeueMatching(func(value int) bool {
		return value == 2
	})
	require.True(t, ok)
	assert.Equal(t, 2, value)
	assert.Empty(t, dropped)

	removed, dropped := snake.Cancel(first)
	require.True(t, removed)
	assert.Empty(t, dropped)

	value, ok, dropped = snake.Dequeue()
	require.True(t, ok)
	assert.Equal(t, 3, value)
	assert.Empty(t, dropped)
	assert.Zero(t, snake.Len())

	removed, dropped = snake.Cancel(second)
	assert.False(t, removed)
	assert.Empty(t, dropped)
}

func TestSnakeCountMatching(t *testing.T) {
	snake := NewSnake[string](SnakeConfig{})
	_, dropped := snake.Enqueue("one", 0)
	assert.Empty(t, dropped)
	_, dropped = snake.Enqueue("two", 0)
	assert.Empty(t, dropped)
	_, dropped = snake.Enqueue("three", 0)
	assert.Empty(t, dropped)

	assert.Equal(t, 2, snake.CountMatching(func(value string) bool {
		return len(value) == 3
	}))
}

func TestSnakeDropsLowestPriorityRequest(t *testing.T) {
	snake, clock := newTestSnake[string](ModeEnabled)

	for _, item := range []struct {
		value    string
		priority float64
	}{
		{"highest", 100},
		{"lowest", 0},
		{"middle-1", 50},
		{"middle-2", 50},
		{"middle-3", 50},
		{"middle-4", 50},
	} {
		_, dropped := snake.Enqueue(item.value, item.priority)
		require.Empty(t, dropped)
	}

	clock.advance(10 * time.Millisecond)
	dropped := snake.LockedDropTimerFired()

	assert.Equal(t, []string{"lowest"}, dropped)
	assert.Equal(t, 5, snake.Len())
	assert.Equal(t, int64(1), snake.ShedCount())
}

func TestSnakeOffAndShadowDoNotDrop(t *testing.T) {
	for _, mode := range []Mode{ModeOff, ModeShadow} {
		t.Run(string(mode), func(t *testing.T) {
			snake, clock := newTestSnake[int](mode)
			for i := range 10 {
				_, dropped := snake.Enqueue(i, 0)
				require.Empty(t, dropped)
			}

			clock.advance(time.Second)
			assert.Empty(t, snake.LockedDropTimerFired())
			assert.Equal(t, 10, snake.Len())
			assert.Zero(t, snake.ShedCount())
		})
	}
}

func TestSnakeShadowRecordsInitialTarget(t *testing.T) {
	snake, clock := newTestSnake[int](ModeShadow)

	_, dropped := snake.Enqueue(1, 0)
	require.Empty(t, dropped)
	require.True(t, snake.initialTargetShadow.active)

	clock.advance(101 * time.Millisecond)
	_, ok, dropped := snake.Dequeue()
	require.True(t, ok)
	require.Empty(t, dropped)

	assert.Equal(t, int64(1), snake.shadowRequiredTarget.Count())
	assert.Equal(t, int64(1), snake.shadowRequiredTarget.Counts()["10"])
}

func TestSnakeNeverDropsUndroppableRequests(t *testing.T) {
	snake, clock := newTestSnake[string](ModeEnabled)

	_, dropped := snake.Enqueue("undroppable", PriorityUndroppable)
	require.Empty(t, dropped)
	for i := range 5 {
		_, dropped = snake.Enqueue(string(rune('a'+i)), 0)
		require.Empty(t, dropped)
	}

	clock.advance(10 * time.Millisecond)
	dropped = snake.LockedDropTimerFired()

	require.Len(t, dropped, 1)
	assert.NotEqual(t, "undroppable", dropped[0])
	assert.Equal(t, 5, snake.Len())
}
