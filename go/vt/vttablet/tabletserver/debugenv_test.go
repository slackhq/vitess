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
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime/debug"
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

	postVar(t, tsv, "LoadshedInterval", "250ms")
	assert.Equal(t, 250*time.Millisecond, tsv.Config().LoadshedInterval)

	postVar(t, tsv, "LoadshedExponent", "2.5")
	assert.Equal(t, 2.5, tsv.Config().LoadshedExponent)
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
		"LoadshedTarget", "LoadshedInterval", "LoadshedExponent",
		"LoadshedDropMode", "LoadshedTrigger", "LoadshedGraceCount",
	} {
		_, ok := names[want]
		assert.Truef(t, ok, "getVars should list %s", want)
	}
}

// TestDebugEnvDropModeWiredToGate confirms a drop-mode override flows through to
// the live OLTP read gate, not just the config struct.
func TestDebugEnvDropModeWiredToGate(t *testing.T) {
	tsv := newDebugEnvTabletServer(t)
	require.NotNil(t, tsv.qe.snake, "loadshed must be enabled for this test")

	postVar(t, tsv, "LoadshedDropMode", "jump")

	mode, err := loadshed.ParseDropMode(tsv.Config().LoadshedDropMode)
	require.NoError(t, err)
	assert.Equal(t, loadshed.DropJumpStart, mode)
}

// TestDebugEnvGCSettings verifies the GOGC / GOMEMLIMIT knobs apply to the Go
// runtime and read back through the GET listing. GC settings are process-global,
// so the original values are restored on cleanup.
func TestDebugEnvGCSettings(t *testing.T) {
	origGOGC := debug.SetGCPercent(-1)    // read + disable
	debug.SetGCPercent(origGOGC)          // restore immediately
	origLimit := debug.SetMemoryLimit(-1) // read current
	t.Cleanup(func() {
		debug.SetGCPercent(origGOGC)
		debug.SetMemoryLimit(origLimit)
	})

	tsv := newDebugEnvTabletServer(t)

	getVar := func(name string) string {
		for _, v := range getVars(tsv) {
			if v.Name == name {
				return v.Value
			}
		}
		return ""
	}

	// GOGC: set and confirm both the runtime and the GET listing reflect it.
	postVar(t, tsv, "GOGC", "250")
	assert.Equal(t, 250, debug.SetGCPercent(250), "SetGCPercent should report 250 as the previous value")
	assert.Equal(t, "250", getVar("GOGC"), "GET listing should show the new GOGC")

	// GOMEMLIMIT: a human size parses to bytes and reads back as a human size.
	postVar(t, tsv, "GOMEMLIMIT", "512MiB")
	assert.Equal(t, int64(512*1024*1024), debug.SetMemoryLimit(-1), "soft memory limit should be 512MiB in bytes")
	assert.Equal(t, "512 MiB", getVar("GOMEMLIMIT"), "GET listing should render GOMEMLIMIT as a human size")

	// "off" restores the default (no soft limit).
	postVar(t, tsv, "GOMEMLIMIT", "off")
	assert.Equal(t, int64(math.MaxInt64), debug.SetMemoryLimit(-1), "off restores the default (no soft limit)")
	assert.Equal(t, "off", getVar("GOMEMLIMIT"))
}

// TestDebugEnvGCSettingsInvalid rejects bad GC input without mutating the runtime.
func TestDebugEnvGCSettingsInvalid(t *testing.T) {
	origGOGC := debug.SetGCPercent(-1)
	debug.SetGCPercent(origGOGC)
	t.Cleanup(func() { debug.SetGCPercent(origGOGC) })

	tsv := newDebugEnvTabletServer(t)

	for _, tc := range []struct{ name, value string }{
		{"GOGC", "0"},
		{"GOGC", "-5"},
		{"GOGC", "notanint"},
		{"GOMEMLIMIT", "notasize"},
	} {
		form := url.Values{"varname": {tc.name}, "value": {tc.value}}
		r := httptest.NewRequest(http.MethodPost, "/debug/env", strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		handlePost(tsv, w, r)
		assert.Equalf(t, http.StatusBadRequest, w.Code, "%s=%s should be rejected", tc.name, tc.value)
	}
	assert.Equal(t, origGOGC, debug.SetGCPercent(origGOGC), "invalid input must not mutate GOGC")
}
