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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestExtractSchemaQualifiers(t *testing.T) {
	tcases := []struct {
		input string
		want  []string
	}{
		{"select * from t", nil},
		{"select * from performance_schema.events_statements_summary_by_digest", []string{"performance_schema"}},
		{"select 1 from information_schema.tables", []string{"information_schema"}},
		{"select * from performance_schema.a join performance_schema.b", []string{"performance_schema"}},
		{"select * from performance_schema.a join information_schema.b", []string{"performance_schema", "information_schema"}},
		{"select * from user_table join performance_schema.metrics", []string{"performance_schema"}},
		// Case is preserved in the stored value; dedup is case-insensitive.
		{"select * from Performance_Schema.a join PERFORMANCE_SCHEMA.b", []string{"Performance_Schema"}},
	}

	parser := sqlparser.NewTestParser()
	for _, tc := range tcases {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := parser.Parse(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.want, extractSchemaQualifiers(stmt))
		})
	}
}
