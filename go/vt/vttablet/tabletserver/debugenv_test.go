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

	postVar(t, tsv, "LoadshedOltpReadMode", "shadow")
	assert.True(t, tsv.Config().LoadshedOltpRead.IsShadow())

	postVar(t, tsv, "LoadshedTxMode", "off")
	assert.Equal(t, tabletenv.LoadshedModeOff, tsv.Config().LoadshedTx.ModeValue())

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
		"LoadshedOltpReadMode", "LoadshedOltpReadTarget", "LoadshedOltpReadInitialTarget", "LoadshedOltpReadIntervalRatio",
		"LoadshedOltpReadUndroppableSchemas",
		"LoadshedTxMode", "LoadshedTxTarget", "LoadshedTxInitialTarget", "LoadshedTxIntervalRatio",
	} {
		_, ok := names[want]
		assert.Truef(t, ok, "getVars should list %s", want)
	}
	_, ok := names["LoadshedOltpReadEnabled"]
	assert.False(t, ok)
	_, ok = names["LoadshedTxEnabled"]
	assert.False(t, ok)
	_, ok = names["LoadshedTxUndroppableSchemas"]
	assert.False(t, ok, "transaction pool must not expose undroppable schemas")
}

func TestDebugEnvModeWiredToOltpGate(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	require.NotNil(t, tsv.qe.snake)

	postVar(t, tsv, "LoadshedOltpReadMode", "off")
	assert.False(t, tsv.Config().LoadshedOltpRead.IsEnabled())
	assert.False(t, tsv.Config().LoadshedOltpRead.IsShadow())

	postVar(t, tsv, "LoadshedOltpReadMode", "shadow")
	assert.False(t, tsv.Config().LoadshedOltpRead.IsEnabled())
	assert.True(t, tsv.Config().LoadshedOltpRead.IsShadow())

	postVar(t, tsv, "LoadshedOltpReadMode", "enabled")
	assert.True(t, tsv.Config().LoadshedOltpRead.IsEnabled())
	assert.False(t, tsv.Config().LoadshedOltpRead.IsShadow())
}

func TestDebugEnvEnablingRearmsExistingBacklog(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	postVar(t, tsv, "LoadshedOltpReadMode", "off")
	postVar(t, tsv, "LoadshedOltpReadTarget", "1ms")
	postVar(t, tsv, "LoadshedOltpReadIntervalRatio", "1")

	var holders []func()
	for range tsv.Config().OltpReadPool.Size {
		unlock, err := tsv.qe.snake.Acquire(t.Context(), "", 0)
		require.NoError(t, err)
		holders = append(holders, func() { require.NoError(t, unlock.Release()) })
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	const waiters = 6
	resultCh := make(chan error, waiters)
	for range waiters {
		go func() {
			unlock, err := tsv.qe.snake.Acquire(ctx, "", 0)
			if unlock != nil {
				_ = unlock.Release()
			}
			resultCh <- err
		}()
	}
	require.Eventually(t, func() bool {
		return tsv.qe.snake.Stats().DroppableLen == waiters
	}, time.Second, time.Millisecond)

	postVar(t, tsv, "LoadshedOltpReadMode", "enabled")

	require.Eventually(t, func() bool {
		return tsv.qe.snake.ShedCount() > 0
	}, 2*time.Second, time.Millisecond)

	cancel()
	for _, release := range holders {
		release()
	}
	for range waiters {
		select {
		case <-resultCh:
		case <-time.After(2 * time.Second):
			t.Fatal("queued acquire did not return")
		}
	}
}

func TestDebugEnvRejectsInvalidLoadshedMode(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	form := url.Values{"varname": {"LoadshedOltpReadMode"}, "value": {"dry-run"}}
	r := httptest.NewRequest(http.MethodPost, "/debug/env", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	handlePost(tsv, w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, tabletenv.LoadshedModeEnabled, tsv.Config().LoadshedOltpRead.ModeValue())
}
