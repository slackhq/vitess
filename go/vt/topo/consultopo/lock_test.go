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

package consultopo

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
)

type lockTestKV struct {
	keys    []string
	pair    *api.KVPair
	getErr  error
	getKeys []string
}

func (m *lockTestKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	m.getKeys = append(m.getKeys, key)
	return m.pair, nil, m.getErr
}

func (m *lockTestKV) List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	return nil, nil, nil
}

func (m *lockTestKV) Keys(prefix string, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error) {
	return m.keys, nil, nil
}

func (m *lockTestKV) Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error) {
	return false, nil, nil, nil
}

const testLockPath = "global/keyspaces/ks/shards/0/Lock"

// lockValue returns vitess lock contents that were locked at the given time.
func lockValue(lockedAt time.Time) []byte {
	return []byte(fmt.Sprintf(`{"Action":"VTOrc Recovery","HostName":"vtorc-1","UserName":"vitess","Time":%q,"Status":"Running"}`,
		lockedAt.Format(time.RFC3339)))
}

func TestCanAcquireLockFile(t *testing.T) {
	old := lockValue(time.Now().Add(-time.Hour))
	young := lockValue(time.Now().Add(-time.Minute))

	tests := []struct {
		name    string
		minAge  time.Duration
		pair    *api.KVPair
		getErr  error
		want    bool
		wantErr string
	}{
		{
			name:   "lock owned by a session",
			minAge: 10 * time.Minute,
			pair:   &api.KVPair{Key: testLockPath, Flags: api.LockFlagValue, Session: "session-1", Value: old},
			want:   false,
		},
		{
			name:   "stale orphaned lock",
			minAge: 10 * time.Minute,
			pair:   &api.KVPair{Key: testLockPath, Flags: api.LockFlagValue, Value: old},
			want:   true,
		},
		{
			name:   "young orphaned lock may still be in use by a holder that lost its session",
			minAge: 10 * time.Minute,
			pair:   &api.KVPair{Key: testLockPath, Flags: api.LockFlagValue, Value: young},
			want:   false,
		},
		{
			name:   "young orphaned lock with min age disabled",
			minAge: 0,
			pair:   &api.KVPair{Key: testLockPath, Flags: api.LockFlagValue, Value: young},
			want:   true,
		},
		{
			name:   "orphaned lock with unparsable contents",
			minAge: 10 * time.Minute,
			pair:   &api.KVPair{Key: testLockPath, Flags: api.LockFlagValue, Value: []byte("not json")},
			want:   false,
		},
		{
			name:   "orphaned lock without a lock time",
			minAge: 10 * time.Minute,
			pair:   &api.KVPair{Key: testLockPath, Flags: api.LockFlagValue, Value: []byte(`{"Action":"VTOrc Recovery"}`)},
			want:   false,
		},
		{
			name:   "lock file deleted after listing",
			minAge: 10 * time.Minute,
			pair:   nil,
			want:   true,
		},
		{
			name:    "consul error",
			minAge:  10 * time.Minute,
			getErr:  errors.New("Unexpected response code: 500 (No cluster leader)"),
			wantErr: "No cluster leader",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv := &lockTestKV{pair: tt.pair, getErr: tt.getErr}
			s := &Server{root: "global", kv: kv, orphanLockMinAge: tt.minAge}

			ok, err := s.canAcquireLockFile(context.Background(), "keyspaces/ks/shards/0")
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, ok)
			assert.Equal(t, []string{testLockPath}, kv.getKeys)
		})
	}
}

func TestTryLock_YoungOrphanedLockReturnsNodeExists(t *testing.T) {
	kv := &lockTestKV{
		keys: []string{"global/keyspaces/ks/shards/0/Lock", "global/keyspaces/ks/shards/0/Shard"},
		pair: &api.KVPair{Key: testLockPath, Flags: api.LockFlagValue, Value: lockValue(time.Now())},
	}
	s := &Server{root: "global", kv: kv, orphanLockMinAge: 10 * time.Minute}

	_, err := s.TryLock(context.Background(), "keyspaces/ks/shards/0", "contents")
	assert.True(t, topo.IsErrType(err, topo.NodeExists), "expected NodeExists, got %v", err)
}

func TestTryLock_HeldLockReturnsNodeExists(t *testing.T) {
	kv := &lockTestKV{
		keys: []string{"global/keyspaces/ks/shards/0/Lock", "global/keyspaces/ks/shards/0/Shard"},
		pair: &api.KVPair{Key: testLockPath, Flags: api.LockFlagValue, Session: "session-1"},
	}
	s := &Server{root: "global", kv: kv, orphanLockMinAge: 10 * time.Minute}

	_, err := s.TryLock(context.Background(), "keyspaces/ks/shards/0", "contents")
	assert.True(t, topo.IsErrType(err, topo.NodeExists), "expected NodeExists, got %v", err)
	assert.ErrorContains(t, err, "lock already exists at path keyspaces/ks/shards/0")
}

func TestTryLock_SessionCheckErrorIsReturned(t *testing.T) {
	kv := &lockTestKV{
		keys:   []string{"global/keyspaces/ks/shards/0/Lock", "global/keyspaces/ks/shards/0/Shard"},
		getErr: errors.New("Unexpected response code: 500 (No cluster leader)"),
	}
	s := &Server{root: "global", kv: kv}

	_, err := s.TryLock(context.Background(), "keyspaces/ks/shards/0", "contents")
	assert.ErrorContains(t, err, "No cluster leader")
	assert.False(t, topo.IsErrType(err, topo.NodeExists))
}

type destroyTestKV struct {
	getFunc  func(call int) (*api.KVPair, error)
	txnFunc  func(call int) (bool, error)
	getCalls int
	txnOps   []api.KVTxnOps
	ctxErrs  []error
}

func (m *destroyTestKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	m.getCalls++
	m.ctxErrs = append(m.ctxErrs, q.Context().Err())
	pair, err := m.getFunc(m.getCalls)
	return pair, nil, err
}

func (m *destroyTestKV) List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	return nil, nil, nil
}

func (m *destroyTestKV) Keys(prefix string, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error) {
	return nil, nil, nil
}

func (m *destroyTestKV) Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error) {
	m.txnOps = append(m.txnOps, txn)
	m.ctxErrs = append(m.ctxErrs, q.Context().Err())
	ok, err := m.txnFunc(len(m.txnOps))
	return ok, nil, nil, err
}

var errNoClusterLeader = errors.New("Unexpected response code: 500 (No cluster leader)")

func orphanedLockPair() *api.KVPair {
	return &api.KVPair{Key: testLockPath, Flags: api.LockFlagValue, ModifyIndex: 42, Value: lockValue(time.Now())}
}

func deleteCASOps() api.KVTxnOps {
	return api.KVTxnOps{&api.KVTxnOp{Verb: api.KVDeleteCAS, Key: testLockPath, Index: 42}}
}

func TestDestroyLockFile(t *testing.T) {
	tests := []struct {
		name    string
		pair    *api.KVPair
		getErr  error
		txnOK   bool
		txnErr  error
		wantOps []api.KVTxnOps
		wantErr string
	}{
		{
			name: "lock file already deleted",
			pair: nil,
		},
		{
			name: "lock file re-acquired by another session",
			pair: &api.KVPair{Key: testLockPath, Flags: api.LockFlagValue, Session: "session-2", ModifyIndex: 42},
		},
		{
			name:    "key is not a lock file",
			pair:    &api.KVPair{Key: testLockPath, ModifyIndex: 42},
			wantErr: api.ErrLockConflict.Error(),
		},
		{
			name:    "orphaned lock file is deleted",
			pair:    orphanedLockPair(),
			txnOK:   true,
			wantOps: []api.KVTxnOps{deleteCASOps()},
		},
		{
			name:    "lock file modified before delete",
			pair:    orphanedLockPair(),
			txnOK:   false,
			wantOps: []api.KVTxnOps{deleteCASOps()},
		},
		{
			name:    "get error",
			getErr:  errors.New("connection refused"),
			wantErr: "failed to read lock",
		},
		{
			name:    "delete error",
			pair:    orphanedLockPair(),
			txnErr:  errors.New("connection refused"),
			wantOps: []api.KVTxnOps{deleteCASOps()},
			wantErr: "failed to remove lock",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv := &destroyTestKV{
				getFunc: func(int) (*api.KVPair, error) { return tt.pair, tt.getErr },
				txnFunc: func(int) (bool, error) { return tt.txnOK, tt.txnErr },
			}
			s := &Server{kv: kv}

			err := s.destroyLockFile(context.Background(), testLockPath)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.wantOps, kv.txnOps)
		})
	}
}

func TestDestroyLockFile_RetriesTransientErrors(t *testing.T) {
	kv := &destroyTestKV{
		getFunc: func(call int) (*api.KVPair, error) {
			if call == 1 {
				return nil, errNoClusterLeader
			}
			return orphanedLockPair(), nil
		},
		txnFunc: func(call int) (bool, error) {
			if call == 1 {
				return false, errNoClusterLeader
			}
			return true, nil
		},
	}
	s := &Server{kv: newRetryKV(kv, 3, time.Millisecond, time.Millisecond, true, nil)}

	err := s.destroyLockFile(context.Background(), testLockPath)
	require.NoError(t, err)
	assert.Equal(t, 2, kv.getCalls)
	assert.Equal(t, []api.KVTxnOps{deleteCASOps(), deleteCASOps()}, kv.txnOps)
}

func TestUnlock_DestroysLockFileWithFreshContext(t *testing.T) {
	client, err := api.NewClient(api.DefaultConfig())
	require.NoError(t, err)
	l, err := client.LockKey(testLockPath)
	require.NoError(t, err)

	kv := &destroyTestKV{
		getFunc: func(int) (*api.KVPair, error) { return orphanedLockPair(), nil },
		txnFunc: func(int) (bool, error) { return true, nil },
	}
	li := &lockInstance{lock: l, done: make(chan struct{})}
	s := &Server{kv: kv, locks: map[string]*lockInstance{testLockPath: li}}

	// The caller's context has often expired by the time it unlocks.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = s.unlock(ctx, testLockPath)

	// l was never acquired, so releasing it fails, but cleanup still runs.
	assert.ErrorIs(t, err, api.ErrLockNotHeld)
	assert.NotContains(t, s.locks, testLockPath)
	assert.Equal(t, []api.KVTxnOps{deleteCASOps()}, kv.txnOps)
	assert.Equal(t, []error{nil, nil}, kv.ctxErrs)
}

func TestUnlock_RetriesDestroyOnTransientErrors(t *testing.T) {
	client, err := api.NewClient(api.DefaultConfig())
	require.NoError(t, err)
	l, err := client.LockKey(testLockPath)
	require.NoError(t, err)

	kv := &destroyTestKV{
		getFunc: func(call int) (*api.KVPair, error) {
			if call == 1 {
				return nil, errNoClusterLeader
			}
			return orphanedLockPair(), nil
		},
		txnFunc: func(call int) (bool, error) {
			if call == 1 {
				return false, errNoClusterLeader
			}
			return true, nil
		},
	}
	li := &lockInstance{lock: l, done: make(chan struct{})}
	s := &Server{
		kv:    newRetryKV(kv, 3, time.Millisecond, time.Millisecond, true, nil),
		locks: map[string]*lockInstance{testLockPath: li},
	}

	// The caller's context has often expired by the time it unlocks, which
	// would stop retryKV from retrying.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = s.unlock(ctx, testLockPath)

	assert.ErrorIs(t, err, api.ErrLockNotHeld)
	assert.Equal(t, 2, kv.getCalls)
	assert.Equal(t, []api.KVTxnOps{deleteCASOps(), deleteCASOps()}, kv.txnOps)
}
