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

package grpcqueryservice

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// limiterFakeQueryService is a QueryService that returns a fixed result and
// records consolidated-response memory acquisitions/releases so we can assert
// the gRPC handler wiring.
type limiterFakeQueryService struct {
	queryservice.QueryService
	result   *sqltypes.Result
	acquired atomic.Int64 // bytes requested via AcquireConsolidatedResponseMemory
	released atomic.Bool  // release func was invoked
	lastSize atomic.Int64
}

func (f *limiterFakeQueryService) Execute(ctx context.Context, session queryservice.Session, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return f.result, nil
}

func (f *limiterFakeQueryService) HandlePanic(err *error) {}

func (f *limiterFakeQueryService) AcquireConsolidatedResponseMemory(ctx context.Context, size int64) func() {
	f.acquired.Add(1)
	f.lastSize.Store(size)
	return func() { f.released.Store(true) }
}

var _ consolidatedResponseLimiter = (*limiterFakeQueryService)(nil)

func newConsolidatedResult() *sqltypes.Result {
	r := &sqltypes.Result{
		Fields: []*querypb.Field{{Name: "col", Type: querypb.Type_VARCHAR}},
		Rows:   [][]sqltypes.Value{{sqltypes.NewVarChar("value")}},
	}
	r.SetFromConsolidator()
	return r
}

func TestExecuteConsolidatedResponseAcquiresAndReleasesOnCtxDone(t *testing.T) {
	result := newConsolidatedResult()
	fake := &limiterFakeQueryService{result: result}
	q := &query{server: fake}

	ctx, cancel := context.WithCancel(context.Background())
	resp, err := q.Execute(ctx, &querypb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: "select 1"}})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Proto3 rows are not cached, so each waiter allocates both the proto copy
	// and the wire buffer: the reserved weight is 2x the row-byte estimate.
	assert.Equal(t, int64(1), fake.acquired.Load())
	assert.Equal(t, 2*result.ResponseBytesEstimate(), fake.lastSize.Load())

	// Budget is still held until the RPC context is done.
	assert.False(t, fake.released.Load(), "budget released before ctx done")

	// Cancelling the RPC context (gRPC does this right after the response is
	// sent) fires the AfterFunc release.
	cancel()
	assert.Eventually(t, func() bool {
		return fake.released.Load()
	}, 30*time.Second, time.Millisecond)
}

func TestExecuteConsolidatedResponseCachedProto3RowsWeight(t *testing.T) {
	// When proto3 rows are cached, the per-waiter copy is eliminated, so the
	// reserved weight is 1x the row-byte estimate (only the wire buffer).
	result := newConsolidatedResult()
	result.CacheProto3Rows()
	fake := &limiterFakeQueryService{result: result}
	q := &query{server: fake}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp, err := q.Execute(ctx, &querypb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: "select 1"}})
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, int64(1), fake.acquired.Load())
	assert.Equal(t, result.ResponseBytesEstimate(), fake.lastSize.Load())
}

func TestExecuteNonConsolidatedResponseSkipsBudget(t *testing.T) {
	// A result not flagged by the consolidator must never touch the budget.
	result := &sqltypes.Result{
		Fields: []*querypb.Field{{Name: "col", Type: querypb.Type_VARCHAR}},
		Rows:   [][]sqltypes.Value{{sqltypes.NewVarChar("value")}},
	}
	fake := &limiterFakeQueryService{result: result}
	q := &query{server: fake}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp, err := q.Execute(ctx, &querypb.ExecuteRequest{Query: &querypb.BoundQuery{Sql: "select 1"}})
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, int64(0), fake.acquired.Load(), "non-consolidated response must not acquire budget")
}
