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

	postVar(t, tsv, "LoadshedOlapReadEasingLogBase", "2.5")
	assert.Equal(t, 2.5, tsv.Config().LoadshedOlapRead.EasingLogBaseValue())

	postVar(t, tsv, "LoadshedOlapReadEasingFractionalStrength", "0.1")
	assert.Equal(t, 0.1, tsv.Config().LoadshedOlapRead.EasingFractionalStrengthValue())

	postVar(t, tsv, "LoadshedOlapReadEasingFractionalCreditDecay", "0.8")
	assert.Equal(t, 0.8, tsv.Config().LoadshedOlapRead.EasingFractionalCreditDecayValue())

	postVar(t, tsv, "LoadshedOlapReadEasingReplayRetention", "0.2")
	assert.Equal(t, 0.2, tsv.Config().LoadshedOlapRead.EasingReplayRetentionValue())
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
		{name: "LoadshedOltpReadEasingLogBase", value: "1"},
		{name: "LoadshedOlapReadEasingFractionalStrength", value: "1.1"},
		{name: "LoadshedTxEasingFractionalCreditDecay", value: "NaN"},
		{name: "LoadshedTxEasingReplayRetention", value: "-0.1"},
	} {
		config := loadshedConfig(tsv, test.name)
		target := config.TargetValue()
		initialTarget := config.InitialTargetValue()
		intervalRatio := config.IntervalRatioValue()
		easingLogBase := config.EasingLogBaseValue()
		fractionalStrength := config.EasingFractionalStrengthValue()
		fractionalCreditDecay := config.EasingFractionalCreditDecayValue()
		replayRetention := config.EasingReplayRetentionValue()

		w := postVarResponse(tsv, test.name, test.value)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, target, config.TargetValue())
		assert.Equal(t, initialTarget, config.InitialTargetValue())
		assert.Equal(t, intervalRatio, config.IntervalRatioValue())
		assert.Equal(t, easingLogBase, config.EasingLogBaseValue())
		assert.Equal(t, fractionalStrength, config.EasingFractionalStrengthValue())
		assert.Equal(t, fractionalCreditDecay, config.EasingFractionalCreditDecayValue())
		assert.Equal(t, replayRetention, config.EasingReplayRetentionValue())
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
		"LoadshedOltpReadEasingLogBase",
		"LoadshedOltpReadEasingFractionalStrength",
		"LoadshedOltpReadEasingFractionalCreditDecay",
		"LoadshedOltpReadEasingReplayRetention",
		"LoadshedOlapReadMode",
		"LoadshedOlapReadTarget",
		"LoadshedOlapReadInitialTarget",
		"LoadshedOlapReadIntervalRatio",
		"LoadshedOlapReadEasingLogBase",
		"LoadshedOlapReadEasingFractionalStrength",
		"LoadshedOlapReadEasingFractionalCreditDecay",
		"LoadshedOlapReadEasingReplayRetention",
		"LoadshedTxMode",
		"LoadshedTxTarget",
		"LoadshedTxInitialTarget",
		"LoadshedTxIntervalRatio",
		"LoadshedTxEasingLogBase",
		"LoadshedTxEasingFractionalStrength",
		"LoadshedTxEasingFractionalCreditDecay",
		"LoadshedTxEasingReplayRetention",
	} {
		_, ok := names[want]
		assert.Truef(t, ok, "getVars should list %s", want)
	}
}
