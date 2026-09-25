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
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

func TestUsesOnlyLocalTables(t *testing.T) {
	tables := map[string]*schema.Table{
		"dual": schema.NewTable("dual", schema.NoType),
		"t":    {Name: sqlparser.NewIdentifierCS("t")},
		"u":    {Name: sqlparser.NewIdentifierCS("u")},
	}
	tcases := []struct {
		name  string
		input string
		want  bool
	}{
		{"local table", "select * from t", true},
		{"local tables", "select * from t join u", true},
		{"local qualified table", "select * from vt_test.t", true},
		{"local qualified table case insensitive", "select * from VT_TEST.t", true},
		{"local table in subquery", "select * from t where exists (select 1 from u)", true},
		{"local table with synthetic dual", "select * from t where exists (select 1)", true},
		{"local table through CTE", "with cte as (select * from t) select * from cte", true},
		{"unknown table", "select * from unknown", false},
		{"system table", "select * from performance_schema.threads", false},
		{"mixed local and system tables", "select * from t join performance_schema.threads", false},
		{"foreign qualified table", "select * from other.t", false},
		{"tableless query", "select 1", false},
		{"explicit dual", "select 1 from dual", false},
	}

	parser := sqlparser.NewTestParser()
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.input)
			require.NoError(t, err)
			plan, err := Build(vtenv.NewTestEnv(), stmt, tables, "vt_test", false)
			require.NoError(t, err)
			assert.Equal(t, tc.want, plan.UsesOnlyLocalTables)
		})
	}
}
