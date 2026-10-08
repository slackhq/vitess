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
