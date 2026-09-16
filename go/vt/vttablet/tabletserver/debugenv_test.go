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
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func newDebugEnvTabletServer(t *testing.T) *TabletServer {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	cfg := tabletenv.NewDefaultConfig()
	srvTopoCounts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	return NewTabletServer(ctx, vtenv.NewTestEnv(), "DebugEnvTest", cfg, memorytopo.NewServer(ctx, ""), &topodata.TabletAlias{}, srvTopoCounts)
}

func postVar(t *testing.T, tsv *TabletServer, name, value string) {
	t.Helper()
	form := url.Values{"varname": {name}, "value": {value}}
	r := httptest.NewRequest(http.MethodPost, "/debug/env", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	handlePost(tsv, w, r)
	require.Equalf(t, http.StatusOK, w.Code, "POST %s=%s: %s", name, value, w.Body.String())
}

func TestDebugEnvConsolidatorResponseMemoryLimit(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)

	postVar(t, tsv, "ConsolidatorQueryTotalSize", "1024")
	assert.Equal(t, int64(1024), tsv.Config().ConsolidatorQueryTotalSize)
	assert.Equal(t, int64(1024), tsv.qe.ConsolidatorResponseMemoryLimit())

	postVar(t, tsv, "ConsolidatorQueryTotalSize", "0")
	assert.Equal(t, int64(0), tsv.Config().ConsolidatorQueryTotalSize)
	assert.Equal(t, int64(0), tsv.qe.ConsolidatorResponseMemoryLimit())

	vars := getVars(tsv)
	names := make(map[string]struct{}, len(vars))
	for _, v := range vars {
		names[v.Name] = struct{}{}
	}
	_, ok := names["ConsolidatorQueryTotalSize"]
	assert.True(t, ok, "getVars should list ConsolidatorQueryTotalSize")
}

func TestDebugEnvConsolidatorResponseMemoryLimitRejectsNegative(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	form := url.Values{"varname": {"ConsolidatorQueryTotalSize"}, "value": {"-1"}}
	r := httptest.NewRequest(http.MethodPost, "/debug/env", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	handlePost(tsv, w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}
