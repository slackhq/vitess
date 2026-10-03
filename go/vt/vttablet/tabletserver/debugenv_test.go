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
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	cfg := tabletenv.NewDefaultConfig()
	srvTopoCounts := stats.NewCountersWithSingleLabel("", "Resilient srvtopo server operations", "type")
	return NewTabletServer(ctx, vtenv.NewTestEnv(), "DebugEnvTest", cfg, memorytopo.NewServer(ctx, ""), &topodata.TabletAlias{}, srvTopoCounts)
}

func postVar(t *testing.T, tsv *TabletServer, name, value string) {
	t.Helper()
	w := postVarResponse(tsv, name, value)
	require.Equalf(t, http.StatusOK, w.Code, "POST %s=%s: %s", name, value, w.Body.String())
}

func postVarResponse(tsv *TabletServer, name, value string) *httptest.ResponseRecorder {
	form := url.Values{"varname": {name}, "value": {value}}
	r := httptest.NewRequest(http.MethodPost, "/debug/env", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	handlePost(tsv, w, r)
	return w
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

func TestDebugEnvLoadshedParams(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)

	assert.Equal(t, tabletenv.LoadshedModeOff, tsv.Config().LoadshedOltpRead.ModeValue())
	assert.Equal(t, tabletenv.LoadshedModeOff, tsv.Config().LoadshedOlapRead.ModeValue())
	assert.Equal(t, tabletenv.LoadshedModeOff, tsv.Config().LoadshedTx.ModeValue())

	postVar(t, tsv, "LoadshedOltpReadMode", "shadow")
	assert.Equal(t, tabletenv.LoadshedModeShadow, tsv.Config().LoadshedOltpRead.ModeValue())

	postVar(t, tsv, "LoadshedTxMode", "enabled")
	assert.Equal(t, tabletenv.LoadshedModeEnabled, tsv.Config().LoadshedTx.ModeValue())

	postVar(t, tsv, "LoadshedOlapReadTarget", "9ms")
	assert.Equal(t, 9*time.Millisecond, tsv.Config().LoadshedOlapRead.TargetValue())

	postVar(t, tsv, "LoadshedOltpReadTarget", "7ms")
	assert.Equal(t, 7*time.Millisecond, tsv.Config().LoadshedOltpRead.TargetValue())

	postVar(t, tsv, "LoadshedOltpReadInitialTarget", "17ms")
	assert.Equal(t, 17*time.Millisecond, tsv.Config().LoadshedOltpRead.InitialTargetValue())

	postVar(t, tsv, "LoadshedTxIntervalRatio", "15")
	assert.Equal(t, 15.0, tsv.Config().LoadshedTx.IntervalRatioValue())

	postVar(t, tsv, "LoadshedTxPriorityDequeueMaxSkips", "8")
	assert.Equal(t, 8, tsv.Config().LoadshedTx.PriorityDequeueMaxSkipsValue())

	postVar(t, tsv, "LoadshedOlapReadKeepDroppableFloor", "0")
	assert.Zero(t, tsv.Config().LoadshedOlapRead.KeepDroppableFloorValue())

	postVar(t, tsv, "LoadshedTxKeepDroppableFloorMaxHolds", "3")
	assert.Equal(t, 3, tsv.Config().LoadshedTx.KeepDroppableFloorMaxHoldsValue())
}

func TestDebugEnvLoadshedParamsRejectInvalidValues(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)

	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "LoadshedOltpReadTarget", value: "0s"},
		{name: "LoadshedOlapReadInitialTarget", value: "-1ns"},
		{name: "LoadshedTxIntervalRatio", value: "NaN"},
		{name: "LoadshedOltpReadPriorityDequeueMaxSkips", value: "-1"},
		{name: "LoadshedTxKeepDroppableFloor", value: "-1"},
		{name: "LoadshedTxKeepDroppableFloor", value: "x"},
		{name: "LoadshedOltpReadKeepDroppableFloorMaxHolds", value: "-1"},
	} {
		config := loadshedConfig(tsv, test.name)
		target := config.TargetValue()
		initialTarget := config.InitialTargetValue()
		intervalRatio := config.IntervalRatioValue()
		maxSkips := config.PriorityDequeueMaxSkipsValue()
		floor := config.KeepDroppableFloorValue()
		maxHolds := config.KeepDroppableFloorMaxHoldsValue()

		w := postVarResponse(tsv, test.name, test.value)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, target, config.TargetValue())
		assert.Equal(t, initialTarget, config.InitialTargetValue())
		assert.Equal(t, intervalRatio, config.IntervalRatioValue())
		assert.Equal(t, maxSkips, config.PriorityDequeueMaxSkipsValue())
		assert.Equal(t, floor, config.KeepDroppableFloorValue())
		assert.Equal(t, maxHolds, config.KeepDroppableFloorMaxHoldsValue())
	}
}

func TestDebugEnvLoadshedParamsListed(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	vars := getVars(tsv)
	names := make(map[string]struct{}, len(vars))
	for _, variable := range vars {
		names[variable.Name] = struct{}{}
	}

	for _, want := range []string{
		"LoadshedOltpReadMode",
		"LoadshedOltpReadTarget",
		"LoadshedOltpReadInitialTarget",
		"LoadshedOltpReadIntervalRatio",
		"LoadshedOltpReadPriorityDequeueMaxSkips",
		"LoadshedOltpReadKeepDroppableFloor",
		"LoadshedOltpReadKeepDroppableFloorMaxHolds",
		"LoadshedOlapReadMode",
		"LoadshedOlapReadTarget",
		"LoadshedOlapReadInitialTarget",
		"LoadshedOlapReadIntervalRatio",
		"LoadshedOlapReadPriorityDequeueMaxSkips",
		"LoadshedOlapReadKeepDroppableFloor",
		"LoadshedOlapReadKeepDroppableFloorMaxHolds",
		"LoadshedTxMode",
		"LoadshedTxTarget",
		"LoadshedTxInitialTarget",
		"LoadshedTxIntervalRatio",
		"LoadshedTxPriorityDequeueMaxSkips",
		"LoadshedTxKeepDroppableFloor",
		"LoadshedTxKeepDroppableFloorMaxHolds",
	} {
		_, ok := names[want]
		assert.Truef(t, ok, "getVars should list %s", want)
	}
}
