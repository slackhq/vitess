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

	postVar(t, tsv, "LoadshedTarget", "7ms")
	assert.Equal(t, 7*time.Millisecond, tsv.Config().LoadshedTarget)

	postVar(t, tsv, "LoadshedIntervalRatio", "15")
	assert.Equal(t, 15.0, tsv.Config().LoadshedIntervalRatio)
}

func TestDebugEnvLoadshedJumpStartParams(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)

	postVar(t, tsv, "LoadshedDropMode", "both")
	assert.Equal(t, "both", tsv.Config().LoadshedDropMode)

	postVar(t, tsv, "LoadshedTrigger", "12ms")
	assert.Equal(t, 12*time.Millisecond, tsv.Config().LoadshedTrigger)

	postVar(t, tsv, "LoadshedGraceCount", "4")
	assert.Equal(t, 4, tsv.Config().LoadshedGraceCount)
}

func TestDebugEnvLoadshedDropModeInvalid(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	orig := tsv.Config().LoadshedDropMode

	form := url.Values{"varname": {"LoadshedDropMode"}, "value": {"bogus"}}
	r := httptest.NewRequest(http.MethodPost, "/debug/env", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	handlePost(tsv, w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, orig, tsv.Config().LoadshedDropMode, "invalid value must not mutate config")
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
		"LoadshedTarget", "LoadshedIntervalRatio",
		"LoadshedDropMode", "LoadshedTrigger", "LoadshedGraceCount",
	} {
		_, ok := names[want]
		assert.Truef(t, ok, "getVars should list %s", want)
	}
}

// TestDebugEnvDropModeUpdatesConfig confirms a /debug/env drop-mode override
// updates LoadshedDropMode; the gate reads this config lazily on each drop
// check, so the override takes effect without rebuilding the gate.
func TestDebugEnvDropModeUpdatesConfig(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)

	postVar(t, tsv, "LoadshedDropMode", "jump")

	mode, err := loadshed.ParseDropMode(tsv.Config().LoadshedDropMode)
	require.NoError(t, err)
	assert.Equal(t, loadshed.DropJumpStart, mode)
}
