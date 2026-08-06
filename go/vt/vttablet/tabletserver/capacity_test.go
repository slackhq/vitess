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

package tabletserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLiveCapacity(t *testing.T) {
	assert.Equal(t, 5, liveCapacity(func() int { return 5 }, 10)(),
		"a live pool capacity should be used as-is")
	assert.Equal(t, 10, liveCapacity(func() int { return 0 }, 10)(),
		"capacity 0 (pool not open) should fall back to the configured size")
}
