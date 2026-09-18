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

	_, dropped := snake.Enqueue("queued", 0)

	assert.Empty(t, dropped)
	assert.Equal(t, ModeOff, snake.mode())
	assert.Zero(t, snake.q.dropNextNs)
}

func TestSnakeCancelMatching(t *testing.T) {
	snake := NewSnake[string](SnakeConfig{})
	snake.Enqueue("first", 0)
	snake.Enqueue("second", 0)

	removed, dropped := snake.CancelMatching(func(value string) bool {
		return value == "second"
	})

	require.True(t, removed)
	assert.Empty(t, dropped)
	assert.Equal(t, 1, snake.Len())
}

func TestSnakeDrain(t *testing.T) {
	snake := NewSnake[string](defaultSnakeConfig())
	snake.EnqueueExisting("first", PriorityUndroppable)
	snake.EnqueueExisting("second", PriorityUndroppable)

	assert.Equal(t, []string{"first", "second"}, snake.Drain())
	assert.Zero(t, snake.Len())
}
