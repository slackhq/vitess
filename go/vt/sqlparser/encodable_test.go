/*
Copyright 2019 The Vitess Authors.

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

package sqlparser

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
)

func TestEncodable(t *testing.T) {
	tcases := []struct {
		in  Encodable
		out string
	}{{
		in: InsertValues{{
			sqltypes.NewInt64(1),
			sqltypes.NewVarChar("foo('a')"),
		}, {
			sqltypes.NewInt64(2),
			sqltypes.NewVarChar("bar(`b`)"),
		}},
		out: "(1, 'foo(\\'a\\')'), (2, 'bar(`b`)')",
	}, {
		in: InsertValues{{
			sqltypes.NewInt64(1),
			sqltypes.NewVarBinary("foo('a')"),
		}, {
			sqltypes.NewInt64(2),
			sqltypes.NewVarBinary("bar(`b`)"),
		}},
		out: "(1, _binary'foo(\\'a\\')'), (2, _binary'bar(`b`)')",
	}, {
		// Single column.
		in: &TupleEqualityList{
			Columns: []IdentifierCI{NewIdentifierCI("pk")},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewInt64(1)},
				{sqltypes.NewVarChar("aa")},
			},
		},
		out: "pk in (1, 'aa')",
	}, {
		// Multiple columns.
		in: &TupleEqualityList{
			Columns: []IdentifierCI{NewIdentifierCI("pk1"), NewIdentifierCI("pk2")},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.NewInt64(1),
					sqltypes.NewVarChar("aa"),
				},
				{
					sqltypes.NewInt64(2),
					sqltypes.NewVarChar("bb"),
				},
			},
		},
		out: "(pk1 = 1 and pk2 = 'aa') or (pk1 = 2 and pk2 = 'bb')",
	}}
	for _, tcase := range tcases {
		buf := new(strings.Builder)
		tcase.in.EncodeSQL(buf)
		if out := buf.String(); out != tcase.out {
			t.Errorf("EncodeSQL(%v): %s, want %s", tcase.in, out, tcase.out)
		}
	}
}
