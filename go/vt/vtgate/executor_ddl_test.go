/*
Copyright 2021 The Vitess Authors.

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

package vtgate

import (
	"fmt"
	"testing"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"

	"github.com/stretchr/testify/require"
)

func TestDDLFlags(t *testing.T) {
	defer func() {
		enableOnlineDDL.Set(true)
		enableDirectDDL.Set(true)
	}()
	testcases := []struct {
		enableDirectDDL bool
		enableOnlineDDL bool
		sql             string
		wantErr         bool
		err             string
	}{
		{
			enableDirectDDL: false,
			sql:             "create table t (id int)",
			wantErr:         true,
			err:             "direct DDL is disabled",
		}, {
			enableDirectDDL: true,
			sql:             "create table t (id int)",
			wantErr:         false,
		}, {
			enableOnlineDDL: false,
			sql:             "revert vitess_migration 'abc'",
			wantErr:         true,
			err:             "online DDL is disabled",
		},
	}
	for _, testcase := range testcases {
		t.Run(fmt.Sprintf("%s-%v-%v", testcase.sql, testcase.enableDirectDDL, testcase.enableOnlineDDL), func(t *testing.T) {
			executor, _, _, _, ctx := createExecutorEnv(t)
			session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded})
			enableDirectDDL.Set(testcase.enableDirectDDL)
			enableOnlineDDL.Set(testcase.enableOnlineDDL)
			_, err := executor.Execute(ctx, nil, "TestDDLFlags", session, testcase.sql, nil, false)
			if testcase.wantErr {
				require.EqualError(t, err, testcase.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
