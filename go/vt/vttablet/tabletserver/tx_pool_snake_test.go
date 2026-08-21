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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func setupWithSnake(t *testing.T, capacity int) (*fakesqldb.DB, *TxPool, func()) {
	t.Helper()
	cfg := tabletenv.NewDefaultConfig()
	cfg.TxPool.Size = 300
	cfg.Oltp.TxTimeout = 30 * time.Second
	cfg.TxPool.Timeout = 40 * time.Second
	cfg.OltpReadPool.IdleTimeout = 30 * time.Second
	cfg.OlapReadPool.IdleTimeout = 30 * time.Second
	cfg.TxPool.IdleTimeout = 30 * time.Second
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TxPoolSnakeTest")

	limiter := &fakeLimiter{}
	txPool := NewTxPool(env, limiter)
	txPool.snake = loadshed.NewSnake(loadshed.SnakeConfig{
		Name: "dml-test",
		CoDel: loadshed.CoDelConfig{
			TargetNs:       func() int64 { return (5 * time.Millisecond).Nanoseconds() },
			IntervalNs:     func() int64 { return (100 * time.Millisecond).Nanoseconds() },
			Exponent:       func() float64 { return 0.5 },
			MinDropDelayNs: func() int64 { return int64(time.Millisecond) },
		},
		Capacity:            func() int { return capacity },
		LoadsheddingAllowed: func() bool { return true },
	})

	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	params := dbconfigs.New(db.ConnParams())
	txPool.Open(params, params, params)

	return db, txPool, func() {
		txPool.Close()
		db.Close()
	}
}

func TestTxPoolSnake_BeginAcquiresSlot(t *testing.T) {
	_, txPool, closer := setupWithSnake(t, 2)
	defer closer()

	ctx := context.Background()
	opts := &querypb.ExecuteOptions{LoadshedValveId: "req-1"}

	conn, _, _, err := txPool.Begin(ctx, opts, false, 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, conn.TxProperties().SnakeRelease)
	conn.Unlock()

	// Commit releases the snake slot via txComplete.
	conn2, err := txPool.GetAndLock(conn.ReservedID(), "")
	require.NoError(t, err)
	_, err = txPool.Commit(ctx, conn2)
	require.NoError(t, err)
	conn2.Release(tx.TxCommit)
}

func TestTxPoolSnake_CommitReleasesSlot(t *testing.T) {
	_, txPool, closer := setupWithSnake(t, 1)
	defer closer()

	ctx := context.Background()
	opts := &querypb.ExecuteOptions{LoadshedValveId: "req-1"}

	// Fill the single slot.
	conn, _, _, err := txPool.Begin(ctx, opts, false, 0, nil)
	require.NoError(t, err)
	conn.Unlock()

	// Second begin with a different ID would block/fail if slot is not released.
	// Commit the first to free the slot.
	conn2, err := txPool.GetAndLock(conn.ReservedID(), "")
	require.NoError(t, err)
	_, err = txPool.Commit(ctx, conn2)
	require.NoError(t, err)
	conn2.Release(tx.TxCommit)

	// Now a new begin should succeed.
	conn3, _, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{LoadshedValveId: "req-2"}, false, 0, nil)
	require.NoError(t, err)
	conn3.Unlock()
	conn4, err := txPool.GetAndLock(conn3.ReservedID(), "")
	require.NoError(t, err)
	_, _ = txPool.Commit(ctx, conn4)
	conn4.Release(tx.TxCommit)
}

func TestTxPoolSnake_RollbackReleasesSlot(t *testing.T) {
	_, txPool, closer := setupWithSnake(t, 1)
	defer closer()

	ctx := context.Background()
	opts := &querypb.ExecuteOptions{LoadshedValveId: "req-1"}

	conn, _, _, err := txPool.Begin(ctx, opts, false, 0, nil)
	require.NoError(t, err)
	conn.Unlock()

	// Rollback should release the slot.
	conn2, err := txPool.GetAndLock(conn.ReservedID(), "")
	require.NoError(t, err)
	txPool.RollbackAndRelease(ctx, conn2)

	// Slot is free — new begin should succeed.
	conn3, _, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{LoadshedValveId: "req-2"}, false, 0, nil)
	require.NoError(t, err)
	conn3.Unlock()
	conn4, err := txPool.GetAndLock(conn3.ReservedID(), "")
	require.NoError(t, err)
	_, _ = txPool.Commit(ctx, conn4)
	conn4.Release(tx.TxCommit)
}

func TestTxPoolSnake_EmptyValveIDAcquiresSlot(t *testing.T) {
	_, txPool, closer := setupWithSnake(t, 2)
	defer closer()

	ctx := context.Background()

	// A request with no valve ID still acquires a Snake slot — it enters the
	// CoDel queue directly, bypassing only the per-valve fairness layer.
	conn, _, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, conn.TxProperties().SnakeRelease, "empty valve ID should still acquire a Snake slot")
	conn.Unlock()

	// Cleanup.
	c, _ := txPool.GetAndLock(conn.ReservedID(), "")
	_, _ = txPool.Commit(ctx, c)
	c.Release(tx.TxCommit)
}

func TestTxPoolSnake_ReservedConnSkipsSnake(t *testing.T) {
	_, txPool, closer := setupWithSnake(t, 1)
	defer closer()

	ctx := context.Background()
	opts := &querypb.ExecuteOptions{LoadshedValveId: "req-1"}

	// Fill the single slot.
	conn, _, _, err := txPool.Begin(ctx, opts, false, 0, nil)
	require.NoError(t, err)
	reservedID := conn.ReservedID()
	conn.Unlock()

	// Begin on a reserved conn (reservedID != 0) should skip the snake.
	conn2, err := txPool.GetAndLock(reservedID, "")
	require.NoError(t, err)
	_, _ = txPool.Commit(ctx, conn2)
	conn2.Release(tx.TxCommit)

	// Begin with reservedID on the same conn: since the first tx committed and released,
	// we need a new conn to test reservedID path. Create one via Begin first.
	conn3, _, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{LoadshedValveId: "req-2"}, false, 0, nil)
	require.NoError(t, err)
	rid := conn3.ReservedID()
	conn3.Unlock()

	// Now do a Begin with the reservedID — this takes the reservedID != 0 path.
	conn4, _, _, err := txPool.Begin(ctx, opts, false, rid, nil)
	require.NoError(t, err)
	// SnakeRelease is nil because the reservedID path doesn't acquire from snake.
	assert.Nil(t, conn4.TxProperties().SnakeRelease)
	conn4.Unlock()
	conn5, _ := txPool.GetAndLock(rid, "")
	_, _ = txPool.Commit(ctx, conn5)
	conn5.Release(tx.TxCommit)
}

func TestTxPoolSnake_CapacityExhaustedShedsLoad(t *testing.T) {
	_, txPool, closer := setupWithSnake(t, 1)
	defer closer()

	ctx := context.Background()

	// Fill the single slot.
	conn, _, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{LoadshedValveId: "req-1"}, false, 0, nil)
	require.NoError(t, err)
	conn.Unlock()

	// Second request with a different contention ID should be shed once CoDel kicks in.
	// Use a short-lived context to avoid waiting forever.
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	_, _, _, err = txPool.Begin(shortCtx, &querypb.ExecuteOptions{LoadshedValveId: "req-2"}, false, 0, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dml load shed")

	// Cleanup.
	c, _ := txPool.GetAndLock(conn.ReservedID(), "")
	_, _ = txPool.Commit(ctx, c)
	c.Release(tx.TxCommit)
}

// TestTxPoolSnake_DirectReleaseFreesSlot covers the leak path: a connection that
// acquired a Snake slot in Begin but is torn down via conn.Release() directly —
// as the kill/shutdown/taint/renew-fail paths do — instead of through
// Commit/Rollback (txComplete). The Snake slot must still be freed, or the gate
// leaks holders until capacity is exhausted and all writes stall.
func TestTxPoolSnake_DirectReleaseFreesSlot(t *testing.T) {
	_, txPool, closer := setupWithSnake(t, 1)
	defer closer()

	ctx := context.Background()

	// Fill the single slot.
	conn, _, _, err := txPool.Begin(ctx, &querypb.ExecuteOptions{LoadshedValveId: "req-1"}, false, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, conn.TxProperties().SnakeRelease)

	// Tear the connection down the "bypass" way: a direct Release, NOT via
	// Commit/Rollback. This is what transactionKiller, Shutdown, and tainted-conn
	// cleanup do. It must still free the Snake slot.
	conn.Release(tx.ConnRelease)

	// If the slot leaked, this Begin never gets granted and times out (shed).
	shortCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn2, _, _, err := txPool.Begin(shortCtx, &querypb.ExecuteOptions{LoadshedValveId: "req-2"}, false, 0, nil)
	require.NoError(t, err, "slot must be freed by a direct Release; a leak would starve this Begin")
	conn2.Unlock()
	c, _ := txPool.GetAndLock(conn2.ReservedID(), "")
	_, _ = txPool.Commit(ctx, c)
	c.Release(tx.TxCommit)
}

func TestTxPoolSnake_NilSnakePassesThrough(t *testing.T) {
	// Standard setup without snake — verify no panic.
	_, txPool, _, closer := setup(t)
	defer closer()

	ctx := context.Background()
	opts := &querypb.ExecuteOptions{LoadshedValveId: "req-1"}

	conn, _, _, err := txPool.Begin(ctx, opts, false, 0, nil)
	require.NoError(t, err)
	assert.Nil(t, conn.TxProperties().SnakeRelease)
	conn.Unlock()
	c, _ := txPool.GetAndLock(conn.ReservedID(), "")
	_, _ = txPool.Commit(ctx, c)
	c.Release(tx.TxCommit)
}

func TestTxPoolSnake_RuntimeEnablement(t *testing.T) {
	_, txPool, closer := setupWithSnake(t, 2)
	defer closer()

	txPool.env.Config().LoadshedTx.SetEnabled(false)
	conn, _, _, err := txPool.Begin(t.Context(), &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, conn.TxProperties().SnakeRelease)
	conn.Release(tx.ConnRelease)

	txPool.env.Config().LoadshedTx.SetEnabled(true)
	conn, _, _, err = txPool.Begin(t.Context(), &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, conn.TxProperties().SnakeRelease)
	conn.Release(tx.ConnRelease)
}

func TestTxPoolSnake_RespectsPoolTimeout(t *testing.T) {
	_, txPool, closer := setupWithSnake(t, 1)
	defer closer()
	txPool.env.Config().TxPool.Timeout = 20 * time.Millisecond

	conn, _, _, err := txPool.Begin(t.Context(), &querypb.ExecuteOptions{}, false, 0, nil)
	require.NoError(t, err)
	defer conn.Release(tx.ConnRelease)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, _, _, err = txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	require.ErrorContains(t, err, "transaction pool connection limit exceeded")
}
