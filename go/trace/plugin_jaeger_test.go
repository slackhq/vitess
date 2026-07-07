/*
Copyright 2024 The Vitess Authors.

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

package trace

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/viperutil/vipertest"
)

func TestNewJaegerTracerFromEnv(t *testing.T) {
	tracingSvc, closer, err := newJagerTracerFromEnv("noop")
	require.NoError(t, err)
	require.NotEmpty(t, tracingSvc)
	require.NotEmpty(t, closer)

	tracingSvc, closer, err = newJagerTracerFromEnv("")
	require.ErrorContains(t, err, "no service name provided")
	require.Empty(t, tracingSvc)
	require.Empty(t, closer)
}

func TestSamplesNothing(t *testing.T) {
	cases := []struct {
		samplerType string
		param       float64
		want        bool
	}{
		{"const", 0, true},
		{"const", 1, false},
		{"probabilistic", 0, true},
		{"probabilistic", 0.001, false},
		{"Const", 0, true},         // case-insensitive
		{"PROBABILISTIC", 0, true}, // case-insensitive
		{"ratelimiting", 0, false}, // zero param != disabled for these
		{"remote", 0, false},       // remote can start sampling later
		{"unknown", 0, false},      // unrecognized types are left enabled
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, samplesNothing(c.samplerType, c.param),
			"samplesNothing(%q, %v)", c.samplerType, c.param)
	}
}

// TestJaegerTracerDisabledWhenSamplingNothing verifies that a const/0 sampler
// yields the noop tracer, so the gRPC interceptor (and its per-RPC span
// allocation) is never installed.
func TestJaegerTracerDisabledWhenSamplingNothing(t *testing.T) {
	v := viper.New()
	t.Cleanup(vipertest.Stub(t, v, samplingType))
	t.Cleanup(vipertest.Stub(t, v, samplingRate))
	v.Set(samplingType.Key(), "const")
	v.Set(samplingRate.Key(), 0.0)

	tracingSvc, closer, err := newJagerTracerFromEnv("vttablet")
	require.NoError(t, err)
	require.NotNil(t, closer)
	_, isNoop := tracingSvc.(noopTracingServer)
	assert.True(t, isNoop, "const/0 sampler should yield the noop tracer, got %T", tracingSvc)
}
