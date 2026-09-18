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

func TestSnakeQueue(t *testing.T) {
	snake := NewSnake[int](SnakeConfig{
		Mode: func() Mode { return ModeEnabled },
	})

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
