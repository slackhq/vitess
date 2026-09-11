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

func TestMatchesUndroppableSchema(t *testing.T) {
	allow := []string{"performance_schema", "information_schema", "sys", "mysql"}

	tcases := []struct {
		name       string
		qualifiers []string
		allowlist  []string
		want       bool
	}{
		{"unqualified query", nil, allow, false},
		{"empty allowlist", []string{"performance_schema"}, nil, false},
		{"match", []string{"performance_schema"}, allow, true},
		{"case-insensitive match", []string{"Performance_Schema"}, allow, true},
		{"no match", []string{"myapp"}, allow, false},
		{"one of several matches", []string{"myapp", "sys"}, allow, true},
		{"none of several match", []string{"myapp", "other"}, allow, false},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, matchesUndroppableSchema(tc.qualifiers, tc.allowlist))
		})
	}
}
