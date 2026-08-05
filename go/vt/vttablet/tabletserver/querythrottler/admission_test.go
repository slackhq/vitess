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
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

type predicateOnlyStrategy struct{}

func (predicateOnlyStrategy) Evaluate(context.Context, topodatapb.TabletType, *sqlparser.ParsedQuery, int64, registry.QueryAttributes) registry.ThrottleDecision {
	return registry.ThrottleDecision{Throttle: false}
}
func (predicateOnlyStrategy) Start()                  {}
func (predicateOnlyStrategy) Stop()                   {}
func (predicateOnlyStrategy) GetStrategyName() string { return "predicate-only" }

type admissionStrategy struct {
	predicateOnlyStrategy
	admitErr    error
	released    bool
	releasedErr error
	gotPool     tabletenv.PoolType
	called      bool
}

func (a *admissionStrategy) Admit(_ context.Context, _ registry.QueryAttributes, pool tabletenv.PoolType) (func(err error), error) {
	a.called = true
	a.gotPool = pool
	if a.admitErr != nil {
		return nil, a.admitErr
	}
	return func(err error) { a.released = true; a.releasedErr = err }, nil
}

func TestAcquireAdmission_NonAdmissionStrategyIsNoOp(t *testing.T) {
	qt := &QueryThrottler{strategyHandlerInstance: predicateOnlyStrategy{}}

	release, err := qt.AcquireAdmission(context.Background(), registry.QueryAttributes{}, tabletenv.PoolTypeOltpRead)
	require.NoError(t, err)
	require.NotNil(t, release, "release must be non-nil so callers can defer it unconditionally")
	release(nil)
}

func TestAcquireAdmission_RoutesToAdmissionController(t *testing.T) {
	strategy := &admissionStrategy{}
	qt := &QueryThrottler{strategyHandlerInstance: strategy}

	release, err := qt.AcquireAdmission(context.Background(), registry.QueryAttributes{}, tabletenv.PoolTypeTx)
	require.NoError(t, err)
	require.NotNil(t, release)
	assert.True(t, strategy.called, "Admit should have been invoked")
	assert.Equal(t, tabletenv.PoolTypeTx, strategy.gotPool, "pool must be forwarded to the strategy")

	release(errors.New("done"))
	assert.True(t, strategy.released, "release must reach the strategy's release func")
	assert.EqualError(t, strategy.releasedErr, "done", "release error must be forwarded")
}

func TestAcquireAdmission_PropagatesRejection(t *testing.T) {
	strategy := &admissionStrategy{admitErr: errors.New("shed")}
	qt := &QueryThrottler{strategyHandlerInstance: strategy}

	release, err := qt.AcquireAdmission(context.Background(), registry.QueryAttributes{}, tabletenv.PoolTypeOltpRead)
	assert.Nil(t, release, "no release on rejection")
	assert.EqualError(t, err, "shed")
}

// admissionFactory registers admissionStrategy for a strategy name and records
// the Deps it was built with, so the test can assert the pool accessors reach
// the factory.
type admissionFactory struct {
	built *admissionStrategy
	deps  registry.Deps
}

func (f *admissionFactory) New(deps registry.Deps, _ registry.StrategyConfig) (registry.ThrottlingStrategyHandler, error) {
	f.deps = deps
	f.built = &admissionStrategy{}
	return f.built, nil
}

func TestSelectThrottlingStrategy_BuildsAdmissionStrategyWithPoolSnakes(t *testing.T) {
	fac := &admissionFactory{}
	registry.Register(querythrottlerpb.ThrottlingStrategy_LOADSHED, fac)
	t.Cleanup(func() { registry.Unregister(querythrottlerpb.ThrottlingStrategy_LOADSHED) })

	sentinel := func() *loadshed.Snake { return nil }
	qt := &QueryThrottler{
		tabletConfig: &tabletenv.TabletConfig{},
		poolSnakes:   map[tabletenv.PoolType]func() *loadshed.Snake{tabletenv.PoolTypeOltpRead: sentinel},
	}

	strategy := qt.selectThrottlingStrategy(&querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_LOADSHED})

	require.Same(t, fac.built, strategy, "config selecting LOADSHED must build via the registered factory")
	_, ok := fac.deps.PoolSnakes[tabletenv.PoolTypeOltpRead]
	assert.True(t, ok, "pool-snake accessors must be threaded into Deps")
}
