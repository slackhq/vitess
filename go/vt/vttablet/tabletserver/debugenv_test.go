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
	"time"

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
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := tabletenv.NewDefaultConfig()
	srvTopoCounts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	return NewTabletServer(ctx, vtenv.NewTestEnv(), "DebugEnvTest", cfg, memorytopo.NewServer(ctx, ""), &topodata.TabletAlias{}, srvTopoCounts)
}

// postVar drives the /debug/env POST handler for a single variable and asserts
// the handler accepted it.
func postVar(t *testing.T, tsv *TabletServer, name, value string) {
	t.Helper()
	form := url.Values{"varname": {name}, "value": {value}}
	r := httptest.NewRequest(http.MethodPost, "/debug/env", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	handlePost(tsv, w, r)
	require.Equalf(t, http.StatusOK, w.Code, "POST %s=%s: %s", name, value, w.Body.String())
}

func TestDebugEnvLoadshedCoDelParams(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)

	postVar(t, tsv, "LoadshedOltpReadEnabled", "false")
	assert.False(t, tsv.Config().LoadshedOltpRead.IsEnabled())

	postVar(t, tsv, "LoadshedTxEnabled", "false")
	assert.False(t, tsv.Config().LoadshedTx.IsEnabled())

	postVar(t, tsv, "LoadshedOltpReadTarget", "7ms")
	assert.Equal(t, 7*time.Millisecond, tsv.Config().LoadshedOltpRead.TargetValue())

	postVar(t, tsv, "LoadshedOltpReadInitialTarget", "17ms")
	assert.Equal(t, 17*time.Millisecond, tsv.Config().LoadshedOltpRead.InitialTargetValue())

	postVar(t, tsv, "LoadshedTxInitialTarget", "23ms")
	assert.Equal(t, 23*time.Millisecond, tsv.Config().LoadshedTx.InitialTargetValue())

	postVar(t, tsv, "LoadshedTxIntervalRatio", "15")
	assert.Equal(t, 15.0, tsv.Config().LoadshedTx.IntervalRatioValue())
	assert.NotEqual(t, tsv.Config().LoadshedOltpRead.IntervalRatioValue(), tsv.Config().LoadshedTx.IntervalRatioValue())

	assert.Equal(t, (17 * time.Millisecond * 20).Nanoseconds(), tsv.qe.snake.Stats().CurrentInterval)
	assert.Equal(t, (23 * time.Millisecond * 15).Nanoseconds(), tsv.te.txPool.snake.Stats().CurrentInterval)
}

func TestDebugEnvLoadshedInitialTargetShowsConfiguredFallback(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	initialTarget := func() string {
		for _, variable := range getVars(tsv) {
			if variable.Name == "LoadshedOltpReadInitialTarget" {
				return variable.Value
			}
		}
		return ""
	}

	assert.Equal(t, "0s", initialTarget())

	postVar(t, tsv, "LoadshedOltpReadTarget", "7ms")
	assert.Equal(t, "0s", initialTarget())

	postVar(t, tsv, "LoadshedOltpReadInitialTarget", "17ms")
	assert.Equal(t, "17ms", initialTarget())

	postVar(t, tsv, "LoadshedOltpReadInitialTarget", "0s")
	assert.Equal(t, "0s", initialTarget())
}

func TestDebugEnvUnknownVariable(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	form := url.Values{"varname": {"LoadshedTarget"}, "value": {"7ms"}}
	r := httptest.NewRequest(http.MethodPost, "/debug/env", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	handlePost(tsv, w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// TestDebugEnvLoadshedParamsListed ensures every load-shed knob is surfaced in
// the GET listing so operators can see and edit it.
func TestDebugEnvLoadshedParamsListed(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	vars := getVars(tsv)
	names := make(map[string]struct{}, len(vars))
	for _, v := range vars {
		names[v.Name] = struct{}{}
	}
	for _, want := range []string{
		"LoadshedOltpReadEnabled", "LoadshedOltpReadTarget", "LoadshedOltpReadInitialTarget", "LoadshedOltpReadIntervalRatio",
		"LoadshedOltpReadUndroppableSchemas",
		"LoadshedTxEnabled", "LoadshedTxTarget", "LoadshedTxInitialTarget", "LoadshedTxIntervalRatio",
	} {
		_, ok := names[want]
		assert.Truef(t, ok, "getVars should list %s", want)
	}
	_, ok := names["LoadshedTxUndroppableSchemas"]
	assert.False(t, ok, "transaction pool must not expose undroppable schemas")
}

func TestDebugEnvEnablementWiredToOltpGate(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)

	postVar(t, tsv, "LoadshedOltpReadEnabled", "false")
	assert.False(t, tsv.Config().LoadshedOltpRead.IsEnabled())

	postVar(t, tsv, "LoadshedOltpReadEnabled", "true")
	assert.True(t, tsv.Config().LoadshedOltpRead.IsEnabled())
}
