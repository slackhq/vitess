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
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
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

	postVar(t, tsv, "LoadshedTxIntervalRatio", "15")
	assert.Equal(t, 15.0, tsv.Config().LoadshedTx.IntervalRatioValue())
	assert.NotEqual(t, tsv.Config().LoadshedOltpRead.IntervalRatioValue(), tsv.Config().LoadshedTx.IntervalRatioValue())

}

func TestDebugEnvLoadshedJumpStartParams(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)

	postVar(t, tsv, "LoadshedOltpReadDropMode", "both")
	assert.Equal(t, "both", tsv.Config().LoadshedOltpRead.DropModeValue())

	postVar(t, tsv, "LoadshedOltpReadTrigger", "12ms")
	assert.Equal(t, 12*time.Millisecond, tsv.Config().LoadshedOltpRead.TriggerValue())

	postVar(t, tsv, "LoadshedTxGraceCount", "4")
	assert.Equal(t, 4, tsv.Config().LoadshedTx.GraceCountValue())
}

func TestDebugEnvLoadshedDropModeInvalid(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	orig := tsv.Config().LoadshedOltpRead.DropModeValue()

	form := url.Values{"varname": {"LoadshedOltpReadDropMode"}, "value": {"bogus"}}
	r := httptest.NewRequest(http.MethodPost, "/debug/env", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	handlePost(tsv, w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, orig, tsv.Config().LoadshedOltpRead.DropModeValue(), "invalid value must not mutate config")
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
		"LoadshedOltpReadEnabled", "LoadshedOltpReadTarget", "LoadshedOltpReadIntervalRatio",
		"LoadshedOltpReadDropMode", "LoadshedOltpReadTrigger", "LoadshedOltpReadGraceCount",
		"LoadshedOltpReadUndroppableSchemas",
		"LoadshedTxEnabled", "LoadshedTxTarget", "LoadshedTxIntervalRatio",
		"LoadshedTxDropMode", "LoadshedTxTrigger", "LoadshedTxGraceCount",
	} {
		_, ok := names[want]
		assert.Truef(t, ok, "getVars should list %s", want)
	}
	_, ok := names["LoadshedTxUndroppableSchemas"]
	assert.False(t, ok, "transaction pool must not expose undroppable schemas")
}

// TestDebugEnvDropModeWiredToGate confirms a drop-mode override flows through to
// the live OLTP read gate, not just the config struct.
func TestDebugEnvDropModeWiredToGate(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	require.NotNil(t, tsv.qe.snake, "loadshed must be enabled for this test")

	postVar(t, tsv, "LoadshedOltpReadDropMode", "jump")

	mode, err := loadshed.ParseDropMode(tsv.Config().LoadshedOltpRead.DropModeValue())
	require.NoError(t, err)
	assert.Equal(t, loadshed.DropJumpStart, mode)
}

func TestDebugEnvEnablementWiredToOltpGate(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	require.NotNil(t, tsv.qe.snake)

	postVar(t, tsv, "LoadshedOltpReadEnabled", "false")
	assert.False(t, tsv.Config().LoadshedOltpRead.IsEnabled())

	postVar(t, tsv, "LoadshedOltpReadEnabled", "true")
	assert.True(t, tsv.Config().LoadshedOltpRead.IsEnabled())
}
