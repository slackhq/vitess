/*
Copyright 2023 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/semisyncmonitor"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func newTestReplicationTM(tablet *topodatapb.Tablet, mysqlDaemon *mysqlctl.FakeMysqlDaemon, ts *topo.Server) *TabletManager {
	waitForGrantsComplete := make(chan struct{})
	close(waitForGrantsComplete)

	return &TabletManager{
		actionSema:             semaphore.NewWeighted(1),
		TopoServer:             ts,
		MysqlDaemon:            mysqlDaemon,
		tabletAlias:            tablet.Alias,
		_waitForGrantsComplete: waitForGrantsComplete,
		tmState: &tmState{
			displayState: displayState{
				tablet: tablet,
			},
		},
	}
}

// TestWaitForGrantsToHaveApplied tests that waitForGrantsToHaveApplied only succeeds after waitForDBAGrants has been called.
func TestWaitForGrantsToHaveApplied(t *testing.T) {
	tm := &TabletManager{
		_waitForGrantsComplete: make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := tm.waitForGrantsToHaveApplied(ctx)
	require.ErrorContains(t, err, "deadline exceeded")

	err = tm.waitForDBAGrants(nil, 0)
	require.NoError(t, err)

	secondContext, secondCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer secondCancel()
	err = tm.waitForGrantsToHaveApplied(secondContext)
	require.NoError(t, err)
}

type demotePrimaryStallQS struct {
	tabletserver.Controller
	qsWaitChan     chan any
	primaryStalled atomic.Bool
}

func (d *demotePrimaryStallQS) SetDemotePrimaryStalled(val bool) {
	d.primaryStalled.Store(val)
}

func (d *demotePrimaryStallQS) IsServing() bool {
	<-d.qsWaitChan
	return false
}

func TestPrimaryStatusIncludesServerVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)

	err := tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, false)
	require.NoError(t, err)

	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeMysqlDaemon.Version = "Ver 8.0.35"

	status, err := tm.PrimaryStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, "Ver 8.0.35", status.ServerVersion)
}

func TestReplicationStatusIncludesServerVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)

	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeMysqlDaemon.Version = "Ver 8.0.35"

	status, err := tm.ReplicationStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, "Ver 8.0.35", status.ServerVersion)
}

func TestDemotePrimaryIncludesServerVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)

	err := tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, false)
	require.NoError(t, err)

	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeMysqlDaemon.Version = "Ver 8.0.35"
	fakeMysqlDaemon.DB().SetNeverFail(true)

	tm.SemiSyncMonitor.Open()

	status, err := tm.DemotePrimary(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, "Ver 8.0.35", status.ServerVersion)
}

// TestDemotePrimaryStalled checks that if demote primary takes too long, then we mark it as stalled.
func TestDemotePrimaryStalled(t *testing.T) {
	// Set remote operation timeout to a very low value.
	origVal := topo.RemoteOperationTimeout
	topo.RemoteOperationTimeout = 100 * time.Millisecond
	defer func() {
		topo.RemoteOperationTimeout = origVal
	}()

	// Create a fake query service control to intercept calls from DemotePrimary function.
	qsc := &demotePrimaryStallQS{
		qsWaitChan: make(chan any),
	}
	// Create a tablet manager with a replica type tablet.
	fakeDb := newTestMysqlDaemon(t, 1)
	tm := &TabletManager{
		actionSema:  semaphore.NewWeighted(1),
		MysqlDaemon: fakeDb,
		tmState: &tmState{
			displayState: displayState{
				tablet: newTestTablet(t, 100, "ks", "-", map[string]string{}),
			},
		},
		QueryServiceControl: qsc,
		SemiSyncMonitor:     semisyncmonitor.CreateTestSemiSyncMonitor(fakeDb.DB(), exporter),
	}

	go func() {
		tm.demotePrimary(t.Context(), false /* revertPartialFailure */, false /* force */)
	}()
	// We make IsServing stall by making it wait on a channel.
	// This should cause the demote primary operation to be stalled.
	require.Eventually(t, func() bool {
		return qsc.primaryStalled.Load()
	}, 5*time.Second, 100*time.Millisecond)

	// Unblock the DemotePrimary call by closing the channel.
	close(qsc.qsWaitChan)

	// Eventually demote primary will succeed, and we want the stalled field to be cleared.
	require.Eventually(t, func() bool {
		return !qsc.primaryStalled.Load()
	}, 5*time.Second, 100*time.Millisecond)
}

// TestDemotePrimaryWaitingForSemiSyncUnblock tests that demote primary unblocks if the primary is blocked on semi-sync ACKs
// and doesn't issue the set super read-only query until all writes waiting on semi-sync ACKs have gone through.
func TestDemotePrimaryWaitingForSemiSyncUnblock(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	// Make the tablet a primary.
	err := tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, false)
	require.NoError(t, err)
	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeDb := fakeMysqlDaemon.DB()
	fakeDb.SetNeverFail(true)

	tm.SemiSyncMonitor.Open()
	// Add a universal insert query pattern that would block until we make it unblock.
	// ExecuteFetchMulti will execute each statement separately, so we need to add SET query.
	fakeDb.AddQueryPattern("SET SESSION lock_wait_timeout=.*", &sqltypes.Result{})
	ch := make(chan int)
	fakeDb.AddQueryPatternWithCallback("^INSERT INTO.*", sqltypes.MakeTestResult(nil), func(s string) {
		<-ch
	})
	// Add a fake query that makes the semi-sync monitor believe that the tablet is blocked on semi-sync ACKs.
	fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(500) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|1",
		"Rpl_semi_sync_source_yes_tx|5"))

	// Verify that in the beginning the tablet is serving.
	require.True(t, tm.QueryServiceControl.IsServing())

	// Start the demote primary operation in a go routine.
	var demotePrimaryFinished atomic.Bool
	go func() {
		_, err := tm.demotePrimary(ctx, false /* revertPartialFailure */, false /* force */)
		if !assert.NoError(t, err) {
			return
		}
		demotePrimaryFinished.Store(true)
	}()

	// Wait for the demote primary operation to have changed the serving state.
	// After that point, we can assume that the demote primary gets blocked on writes waiting for semi-sync ACKs.
	require.Eventually(t, func() bool {
		return !tm.QueryServiceControl.IsServing()
	}, 5*time.Second, 100*time.Millisecond)

	// DemotePrimary shouldn't have finished yet.
	require.False(t, demotePrimaryFinished.Load())
	// We shouldn't have seen the super-read only query either.
	require.False(t, fakeMysqlDaemon.SuperReadOnly.Load())

	// Now we unblock the semi-sync monitor.
	fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|0",
		"Rpl_semi_sync_source_yes_tx|5"))
	close(ch)

	// This should unblock the demote primary operation eventually.
	require.Eventually(t, func() bool {
		return demotePrimaryFinished.Load()
	}, 5*time.Second, 100*time.Millisecond)
	// We should have also seen the super-read only query.
	require.True(t, fakeMysqlDaemon.SuperReadOnly.Load())
}

// TestDemotePrimaryWithSemiSyncProgressDetection tests that demote primary proceeds
// without blocking when transactions are making progress (ackedTrxs increasing between checks).
func TestDemotePrimaryWithSemiSyncProgressDetection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	// Make the tablet a primary.
	err := tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, false)
	require.NoError(t, err)
	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeDb := fakeMysqlDaemon.DB()
	fakeDb.SetNeverFail(true)

	tm.SemiSyncMonitor.Open()

	// Set up the query to show waiting sessions, but with progress (ackedTrxs increasing).
	// The monitor makes TWO calls to getSemiSyncStats with a sleep between them.
	// We add the query result multiple times. The fakesqldb will return them in order (FIFO).
	// First few calls: waiting sessions present, ackedTrxs=5.
	for range 3 {
		fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|1",
			"Rpl_semi_sync_source_yes_tx|5"))
	}
	// Next calls: waiting sessions present, but ackedTrxs=6 (progress!).
	for range 10 {
		fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|1",
			"Rpl_semi_sync_source_yes_tx|6"))
	}

	// Verify that in the beginning the tablet is serving.
	require.True(t, tm.QueryServiceControl.IsServing())

	// Start the demote primary operation in a go routine.
	var demotePrimaryFinished atomic.Bool
	go func() {
		_, err := tm.demotePrimary(ctx, false /* revertPartialFailure */, false /* force */)
		if !assert.NoError(t, err) {
			return
		}
		demotePrimaryFinished.Store(true)
	}()

	// Wait for the demote primary operation to have changed the serving state.
	require.Eventually(t, func() bool {
		return !tm.QueryServiceControl.IsServing()
	}, 5*time.Second, 100*time.Millisecond)

	// DemotePrimary should finish quickly because progress is being made.
	// It should NOT wait for semi-sync to unblock since ackedTrxs is increasing.
	require.Eventually(t, func() bool {
		return demotePrimaryFinished.Load()
	}, 5*time.Second, 100*time.Millisecond)

	// We should have seen the super-read only query.
	require.True(t, fakeMysqlDaemon.SuperReadOnly.Load())
}

// TestDemotePrimaryWhenSemiSyncBecomesUnblockedBetweenChecks tests that demote primary
// proceeds immediately when waiting sessions drops to 0 between the two checks.
func TestDemotePrimaryWhenSemiSyncBecomesUnblockedBetweenChecks(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	// Make the tablet a primary.
	err := tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, false)
	require.NoError(t, err)
	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeDb := fakeMysqlDaemon.DB()
	fakeDb.SetNeverFail(true)

	tm.SemiSyncMonitor.Open()

	// Set up the query to show waiting sessions on first call, but 0 on second call.
	// This simulates the semi-sync becoming unblocked between the two checks.
	// The fakesqldb returns results in FIFO order.
	// First call: waiting sessions present.
	fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|2",
		"Rpl_semi_sync_source_yes_tx|5"))
	// Second and subsequent calls: no waiting sessions (unblocked!).
	for range 10 {
		fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|0",
			"Rpl_semi_sync_source_yes_tx|5"))
	}

	// Verify that in the beginning the tablet is serving.
	require.True(t, tm.QueryServiceControl.IsServing())

	// Start the demote primary operation in a go routine.
	var demotePrimaryFinished atomic.Bool
	go func() {
		_, err := tm.demotePrimary(ctx, false /* revertPartialFailure */, false /* force */)
		if !assert.NoError(t, err) {
			return
		}
		demotePrimaryFinished.Store(true)
	}()

	// Wait for the demote primary operation to have changed the serving state.
	require.Eventually(t, func() bool {
		return !tm.QueryServiceControl.IsServing()
	}, 5*time.Second, 100*time.Millisecond)

	// DemotePrimary should finish quickly because semi-sync became unblocked.
	require.Eventually(t, func() bool {
		return demotePrimaryFinished.Load()
	}, 5*time.Second, 100*time.Millisecond)

	// We should have seen the super-read only query.
	require.True(t, fakeMysqlDaemon.SuperReadOnly.Load())
}

// TestUndoDemotePrimaryStateChange tests that UndoDemotePrimary
// if able to change the state of the tablet to Primary if there
// is a mismatch with the tablet record.
func TestUndoDemotePrimaryStateChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	ti, err := ts.UpdateTabletFields(ctx, tm.Tablet().Alias, func(tablet *topodatapb.Tablet) error {
		tablet.Type = topodatapb.TabletType_PRIMARY
		tablet.PrimaryTermStartTime = protoutil.TimeToProto(time.Now())
		return nil
	})
	require.NoError(t, err)

	// Check that the tablet is initially a replica.
	require.EqualValues(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	// Verify that the tablet record says the tablet should be a primary.
	require.EqualValues(t, topodatapb.TabletType_PRIMARY, ti.Type)

	err = tm.UndoDemotePrimary(ctx, false)
	require.NoError(t, err)
	require.EqualValues(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	require.EqualValues(t, ti.PrimaryTermStartTime, tm.Tablet().PrimaryTermStartTime)
	require.True(t, tm.QueryServiceControl.IsServing())
	isReadOnly, err := tm.MysqlDaemon.IsReadOnly(ctx)
	require.NoError(t, err)
	require.False(t, isReadOnly)
}

func TestStopReplicationAndGetStatus_ServerVersion(t *testing.T) {
	tests := []struct {
		name            string
		mode            replicationdatapb.StopReplicationMode
		replicating     bool
		ioRunning       bool
		expectedQueries []string
		stopIOErr       error
		stopReplErr     error
		afterStatusErr  bool
		expectErr       string
	}{
		{
			name:            "IOTHREADONLY success",
			mode:            replicationdatapb.StopReplicationMode_IOTHREADONLY,
			replicating:     true,
			ioRunning:       true,
			expectedQueries: []string{"STOP REPLICA IO_THREAD"},
		},
		{
			name:        "IOTHREADONLY with IO thread already stopped",
			mode:        replicationdatapb.StopReplicationMode_IOTHREADONLY,
			replicating: false,
			ioRunning:   false,
		},
		{
			name:        "IOTHREADONLY with stopIOThread failure",
			mode:        replicationdatapb.StopReplicationMode_IOTHREADONLY,
			replicating: true,
			ioRunning:   true,
			stopIOErr:   errors.New("injected IO stop error"),
			expectErr:   "stop io thread failed",
		},
		{
			name:            "IOTHREADONLY with after-status failure",
			mode:            replicationdatapb.StopReplicationMode_IOTHREADONLY,
			replicating:     true,
			ioRunning:       true,
			expectedQueries: []string{"STOP REPLICA IO_THREAD"},
			afterStatusErr:  true,
			expectErr:       "acquiring replication status failed",
		},
		{
			name:            "IOANDSQLTHREAD success",
			mode:            replicationdatapb.StopReplicationMode_IOANDSQLTHREAD,
			replicating:     true,
			ioRunning:       true,
			expectedQueries: []string{"STOP REPLICA"},
		},
		{
			name:        "IOANDSQLTHREAD with replication not healthy",
			mode:        replicationdatapb.StopReplicationMode_IOANDSQLTHREAD,
			replicating: false,
			ioRunning:   false,
		},
		{
			name:            "IOANDSQLTHREAD with after-status failure",
			mode:            replicationdatapb.StopReplicationMode_IOANDSQLTHREAD,
			replicating:     true,
			ioRunning:       true,
			expectedQueries: []string{"STOP REPLICA"},
			afterStatusErr:  true,
			expectErr:       "acquiring replication status failed",
		},
		{
			name:        "IOANDSQLTHREAD with stopReplication failure",
			mode:        replicationdatapb.StopReplicationMode_IOANDSQLTHREAD,
			replicating: true,
			ioRunning:   true,
			stopReplErr: errors.New("injected stop error"),
			expectErr:   "stop replication failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
			fakeMysqlDaemon.Replicating = tc.replicating
			fakeMysqlDaemon.IOThreadRunning = tc.ioRunning
			fakeMysqlDaemon.Version = "Ver 8.0.35"

			if tc.expectedQueries != nil {
				fakeMysqlDaemon.ExpectedExecuteSuperQueryList = tc.expectedQueries
			}
			if tc.stopIOErr != nil {
				fakeMysqlDaemon.ExecuteSuperQueryErrorMap = map[string]error{
					"STOP REPLICA IO_THREAD": tc.stopIOErr,
				}
			}
			if tc.stopReplErr != nil {
				fakeMysqlDaemon.StopReplicationError = tc.stopReplErr
			}
			if tc.afterStatusErr {
				// The callback fires during the stop query execution, which happens
				// before the second ReplicationStatus call that fetches the "after" state.
				fakeMysqlDaemon.ExecuteSuperQueryListCallback = func() {
					fakeMysqlDaemon.ReplicationStatusError = errors.New("injected after-status error")
				}
			}

			tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)

			resp, err := tm.StopReplicationAndGetStatus(t.Context(), tc.mode)
			if tc.expectErr != "" {
				require.ErrorContains(t, err, tc.expectErr)
			} else {
				require.NoError(t, err)
			}

			require.NotNil(t, resp.Status)

			// ServerVersion is only populated on the success paths. On error returns
			// the RPC layer discards the status (grpctmserver copies it only when
			// err == nil), so the tablet deliberately skips the version fetch there to
			// avoid an unobservable MySQL query under the TabletManager lock.
			if tc.expectErr != "" {
				require.Empty(t, resp.Status.Before.ServerVersion)
				return
			}

			require.Equal(t, "Ver 8.0.35", resp.Status.Before.ServerVersion)
			if resp.Status.After != nil {
				require.Equal(t, "Ver 8.0.35", resp.Status.After.ServerVersion)
			}
		})
	}
}

// TestStopReplicationAndGetStatus_SlowVersionLookup verifies the wiring: when the
// post-mutation version lookup is slow (cold cache), StopReplicationAndGetStatus
// still returns the stopped-replication status (with an empty ServerVersion)
// rather than failing with a deadline error — the mutation already happened, so
// the response must be delivered.
func TestStopReplicationAndGetStatus_SlowVersionLookup(t *testing.T) {
	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	fakeMysqlDaemon.Replicating = true
	fakeMysqlDaemon.IOThreadRunning = true
	fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA IO_THREAD"}

	tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)
	// Swap in a version daemon whose lookup never completes on its own, so the
	// bounded post-mutation helper must cap it and fall back to "". The wrapper
	// embeds the same fakeMysqlDaemon and overrides only GetVersionString, so all
	// other calls (ReplicationStatus, the STOP REPLICA IO_THREAD query, etc.) still
	// route to the fake and behave as configured above.
	tm.MysqlDaemon = &countingVersionDaemon{
		FakeMysqlDaemon: fakeMysqlDaemon,
		version:         "Ver 8.0.35",
		delay:           time.Hour,
	}

	const deadline = 30 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), deadline)
	defer cancel()

	start := time.Now()
	resp, err := tm.StopReplicationAndGetStatus(ctx, replicationdatapb.StopReplicationMode_IOTHREADONLY)
	elapsed := time.Since(start)

	require.NoError(t, err, "a slow version lookup must not fail the RPC after replication was stopped")
	require.NotNil(t, resp.Status)
	require.NotNil(t, resp.Status.After, "the post-stop status must still be returned")
	require.Empty(t, resp.Status.Before.ServerVersion, "version degrades to empty when the lookup is bounded out")
	// The bounded helper caps the lookup near maxVersionLookupBudget (2s), so
	// the RPC must return promptly rather than run to the full deadline. Without the
	// bound it would block for the entire 30s. A generous 15s upper bound keeps this
	// CI-safe while still proving the lookup was bounded, not run to the deadline.
	require.Less(t, elapsed, 15*time.Second, "the bounded lookup must not run to the caller's full deadline")
}

// TestStopReplicationAndGetStatus_SlowVersionLookupNoOp verifies the no-op early
// returns (IO thread already stopped, or replication not running) also bound the
// version lookup. No stop is performed on these paths, but the status was already
// read successfully; a slow cold-cache lookup under the caller's full context must
// not burn the deadline and fail the RPC with DEADLINE_EXCEEDED, which in ERS would
// drop a reachable tablet purely over optional version metadata.
func TestStopReplicationAndGetStatus_SlowVersionLookupNoOp(t *testing.T) {
	tests := []struct {
		name string
		mode replicationdatapb.StopReplicationMode
	}{
		{
			name: "IO thread only, IO already stopped",
			mode: replicationdatapb.StopReplicationMode_IOTHREADONLY,
		},
		{
			name: "full stop, replication not running",
			mode: replicationdatapb.StopReplicationMode_IOANDSQLTHREAD,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
			// Replication is not running, so both StopReplicationMode branches take
			// their no-op early return without issuing any STOP REPLICA query.
			fakeMysqlDaemon.Replicating = false
			fakeMysqlDaemon.IOThreadRunning = false

			tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)
			// A version lookup that never completes on its own, so the bounded helper
			// must cap it and fall back to "".
			tm.MysqlDaemon = &countingVersionDaemon{
				FakeMysqlDaemon: fakeMysqlDaemon,
				version:         "Ver 8.0.35",
				delay:           time.Hour,
			}

			const deadline = 30 * time.Second
			ctx, cancel := context.WithTimeout(t.Context(), deadline)
			defer cancel()

			start := time.Now()
			resp, err := tm.StopReplicationAndGetStatus(ctx, tc.mode)
			elapsed := time.Since(start)

			require.NoError(t, err, "a slow version lookup must not fail the no-op RPC")
			require.NotNil(t, resp.Status)
			require.NotNil(t, resp.Status.After, "the no-op path returns before as after")
			require.Empty(t, resp.Status.Before.ServerVersion, "version degrades to empty when the lookup is bounded out")
			// Without the bound the lookup would run to the full 30s deadline; the
			// bounded helper caps it near maxVersionLookupBudget (2s). A generous
			// 15s upper bound keeps this CI-safe while still proving the bound applies.
			require.Less(t, elapsed, 15*time.Second, "the bounded lookup must not run to the caller's full deadline")
		})
	}
}

// countingVersionDaemon wraps a FakeMysqlDaemon to count GetVersionString calls
// and optionally return an error or block, so we can assert the version cache and
// deadline-bounding behavior.
type countingVersionDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	calls   atomic.Int64
	version string
	err     error
	// delay, if set, makes GetVersionString block for up to delay, returning early
	// with ctx.Err() if the context is cancelled first. It simulates a slow
	// cold-cache lookup.
	delay time.Duration
}

func (d *countingVersionDaemon) GetVersionString(ctx context.Context) (string, error) {
	d.calls.Add(1)
	if d.delay > 0 {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(d.delay):
		}
	}
	if d.err != nil {
		return "", d.err
	}
	return d.version, nil
}

func TestGetMySQLVersionStringCache(t *testing.T) {
	t.Run("caches within TTL", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		for range 5 {
			require.Equal(t, "Ver 8.0.35", tm.getMySQLVersionString(t.Context()))
		}
		require.EqualValues(t, 1, daemon.calls.Load(), "should query mysqld only once within the TTL")
	})

	t.Run("refetches after TTL", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		require.Equal(t, "Ver 8.0.35", tm.getMySQLVersionString(t.Context()))
		// Expire the cache by backdating the fetch time beyond the TTL.
		tm.mysqlVersion.mu.Lock()
		tm.mysqlVersion.fetchedAt = time.Now().Add(-2 * mysqlVersionCacheTTL)
		tm.mysqlVersion.mu.Unlock()

		require.Equal(t, "Ver 8.0.35", tm.getMySQLVersionString(t.Context()))
		require.EqualValues(t, 2, daemon.calls.Load(), "should re-query mysqld after the TTL expires")
	})

	t.Run("error is not cached", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			err:             errors.New("mysqld down"),
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		require.Empty(t, tm.getMySQLVersionString(t.Context()))
		require.Empty(t, tm.getMySQLVersionString(t.Context()))
		require.EqualValues(t, 2, daemon.calls.Load(), "should retry after an error rather than cache the empty result")
	})

	// Exercised under -race to prove the lock-drop-across-fetch design is sound.
	// The lock is intentionally released during the fetch, so a cold-cache burst
	// may fetch more than once; every caller must still observe the same value.
	t.Run("concurrent callers are race-free and consistent", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		const goroutines = 20
		var wg sync.WaitGroup
		results := make([]string, goroutines)
		wg.Add(goroutines)
		for i := range goroutines {
			go func() {
				defer wg.Done()
				results[i] = tm.getMySQLVersionString(t.Context())
			}()
		}
		wg.Wait()

		for _, r := range results {
			require.Equal(t, "Ver 8.0.35", r)
		}
		// Cold-cache burst may fetch more than once, but far fewer than once per caller.
		require.LessOrEqual(t, daemon.calls.Load(), int64(goroutines))
		require.GreaterOrEqual(t, daemon.calls.Load(), int64(1))
	})
}

func TestGetMySQLVersionStringAfterMutation(t *testing.T) {
	t.Run("bounds a slow lookup to half the remaining deadline and returns empty", func(t *testing.T) {
		// A cold-cache lookup that would never finish on its own. With a generous
		// (CI-safe) deadline, the helper must cap it at half the remaining budget,
		// return "" (best-effort), and leave the other half for the caller to return
		// the already-applied mutation. Timings are seconds, not sub-second, to avoid
		// flakiness on starved runners.
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
			delay:           time.Hour, // effectively never completes on its own
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		const deadline = 8 * time.Second
		ctx, cancel := context.WithTimeout(t.Context(), deadline)
		defer cancel()

		start := time.Now()
		got := tm.getMySQLVersionStringBounded(ctx)
		elapsed := time.Since(start)

		require.Empty(t, got, "a lookup that outruns its budget degrades to empty version")
		// The lookup is capped at min(deadline/2, 2s) = 2s here, so it must return
		// comfortably before the caller's full deadline, leaving budget to respond.
		require.Less(t, elapsed, deadline, "lookup must not consume the whole deadline")
		require.NoError(t, ctx.Err(), "caller deadline must not be exhausted by the version lookup")
	})

	t.Run("caps the lookup at maxVersionLookupBudget on a large deadline", func(t *testing.T) {
		// With a large remaining deadline, half of it (e.g. 15s) far exceeds the 2s
		// absolute cap, so the min(..., maxVersionLookupBudget) arm must bound
		// the hung lookup near 2s rather than ~15s.
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
			delay:           time.Hour,
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		start := time.Now()
		got := tm.getMySQLVersionStringBounded(ctx)
		elapsed := time.Since(start)

		require.Empty(t, got)
		// Must be bounded by the 2s cap, not remaining/2 (~15s). Generous upper bound
		// (10s) keeps it CI-safe while still proving the cap — not remaining/2 — applied.
		require.Less(t, elapsed, 10*time.Second, "lookup must be capped near maxVersionLookupBudget, not remaining/2")
	})

	t.Run("returns the version on a fast lookup", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		require.Equal(t, "Ver 8.0.35", tm.getMySQLVersionStringBounded(ctx))
	})

	t.Run("no deadline still returns the version on a fast lookup", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		require.Equal(t, "Ver 8.0.35", tm.getMySQLVersionStringBounded(context.Background()))
	})

	t.Run("no deadline is still capped at maxVersionLookupBudget", func(t *testing.T) {
		// A deadline-less caller (e.g. an in-process DemotePrimary) must not let a hung
		// cold-cache lookup hold the action lock forever: the absolute cap applies even
		// without a deadline.
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
			delay:           time.Hour,
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		start := time.Now()
		got := tm.getMySQLVersionStringBounded(context.Background())
		elapsed := time.Since(start)

		require.Empty(t, got)
		// Bounded near the 2s cap; generous upper bound keeps it CI-safe while still
		// proving the lookup did not run unbounded.
		require.Less(t, elapsed, 10*time.Second, "deadline-less lookup must still be capped")
	})
}
