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
)

func TestPriorityZeroIsUndroppable(t *testing.T) {
	assert.False(t, newRequest(struct{}{}, 0).isDroppable())
	assert.True(t, newRequest(struct{}{}, 1).isDroppable())
	assert.True(t, newRequest(struct{}{}, 100).isDroppable())
}

func TestRequestRejectsInvalidPriority(t *testing.T) {
	assert.Panics(t, func() { newRequest(struct{}{}, -1) })
	assert.Panics(t, func() { newRequest(struct{}{}, 101) })
}
