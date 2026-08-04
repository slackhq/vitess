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

func TestParseDropMode(t *testing.T) {
	cases := []struct {
		in   string
		want CoDelDropMode
	}{
		{"slow", DropSlowStart},
		{"slow-start", DropSlowStart},
		{"jump", DropJumpStart},
		{"jump-start", DropJumpStart},
		{"both", DropBoth},
	}
	for _, c := range cases {
		got, err := ParseDropMode(c.in)
		require.NoErrorf(t, err, "ParseDropMode(%q)", c.in)
		assert.Equalf(t, c.want, got, "ParseDropMode(%q)", c.in)
	}
}

func TestParseDropMode_Invalid(t *testing.T) {
	_, err := ParseDropMode("bogus")
	assert.ErrorContains(t, err, "bogus")
}

func TestCoDelDropModeString(t *testing.T) {
	assert.Equal(t, "slow", DropSlowStart.String())
	assert.Equal(t, "jump", DropJumpStart.String())
	assert.Equal(t, "both", DropBoth.String())
}
