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

package querythrottler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// predicateOnlyStrategy implements ThrottlingStrategyHandler but NOT
// AdmissionController, so AcquireAdmission must treat it as no-gating.
type predicateOnlyStrategy struct{}

func (predicateOnlyStrategy) Evaluate(context.Context, topodatapb.TabletType, *sqlparser.ParsedQuery, int64, registry.QueryAttributes) registry.ThrottleDecision {
	return registry.ThrottleDecision{Throttle: false}
}
func (predicateOnlyStrategy) Start()                  {}
func (predicateOnlyStrategy) Stop()                   {}
func (predicateOnlyStrategy) GetStrategyName() string { return "predicate-only" }

// admissionStrategy also implements AdmissionController and records the pool it
// was asked to admit, returning a configurable outcome.
type admissionStrategy struct {
	predicateOnlyStrategy
	admitErr    error
	released    bool
	releasedErr error
	gotPool     registry.Pool
	called      bool
}

func (a *admissionStrategy) Admit(_ context.Context, _ registry.QueryAttributes, pool registry.Pool) (func(err error), error) {
	a.called = true
	a.gotPool = pool
	if a.admitErr != nil {
		return nil, a.admitErr
	}
	return func(err error) { a.released = true; a.releasedErr = err }, nil
}

func TestAcquireAdmission_NonAdmissionStrategyIsNoOp(t *testing.T) {
	qt := &QueryThrottler{strategyHandlerInstance: predicateOnlyStrategy{}}

	release, err := qt.AcquireAdmission(context.Background(), registry.QueryAttributes{}, registry.PoolOltpRead)
	require.NoError(t, err)
	require.NotNil(t, release, "release must be non-nil so callers can defer it unconditionally")
	release(nil) // must not panic
}

func TestAcquireAdmission_RoutesToAdmissionController(t *testing.T) {
	strategy := &admissionStrategy{}
	qt := &QueryThrottler{strategyHandlerInstance: strategy}

	release, err := qt.AcquireAdmission(context.Background(), registry.QueryAttributes{}, registry.PoolTx)
	require.NoError(t, err)
	require.NotNil(t, release)
	assert.True(t, strategy.called, "Admit should have been invoked")
	assert.Equal(t, registry.PoolTx, strategy.gotPool, "pool must be forwarded to the strategy")

	release(errors.New("done"))
	assert.True(t, strategy.released, "release must reach the strategy's release func")
	assert.EqualError(t, strategy.releasedErr, "done", "release error must be forwarded")
}

func TestAcquireAdmission_PropagatesRejection(t *testing.T) {
	strategy := &admissionStrategy{admitErr: errors.New("shed")}
	qt := &QueryThrottler{strategyHandlerInstance: strategy}

	release, err := qt.AcquireAdmission(context.Background(), registry.QueryAttributes{}, registry.PoolOltpRead)
	assert.Nil(t, release, "no release on rejection")
	assert.EqualError(t, err, "shed")
}

func TestInstallStrategy_PinnedAgainstConfigUpdate(t *testing.T) {
	qt := &QueryThrottler{
		ctx:                     t.Context(),
		cfg:                     &querythrottlerpb.Config{Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN},
		strategyHandlerInstance: &registry.NoOpStrategy{},
		tabletConfig:            &tabletenv.TabletConfig{},
	}

	pinned := &admissionStrategy{}
	qt.InstallStrategy(pinned)
	require.True(t, qt.pinnedStrategy)

	// A config update selecting a different strategy type must NOT replace the
	// pinned strategy, because the pinned one holds resources the topo factory
	// cannot rebuild.
	srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)
	require.True(t, qt.HandleConfigUpdate(srvks, nil))

	_, ok := qt.strategyHandlerInstance.(*admissionStrategy)
	assert.True(t, ok, "pinned strategy must survive a config update that selects another strategy")
}
