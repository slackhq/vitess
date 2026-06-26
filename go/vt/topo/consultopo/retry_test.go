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

package consultopo

import (
	"context"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
)

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{
			name:      "nil",
			err:       nil,
			retryable: false,
		},
		{
			name:      "context canceled",
			err:       context.Canceled,
			retryable: false,
		},
		{
			name:      "bare deadline exceeded",
			err:       context.DeadlineExceeded,
			retryable: false,
		},
		{
			name: "url error wrapping deadline exceeded",
			err: &url.Error{
				Op:  "Get",
				URL: "http://consul:8500",
				Err: context.DeadlineExceeded,
			},
			retryable: true,
		},
		{
			name: "url error wrapping net error",
			err: &url.Error{
				Op:  "Get",
				URL: "http://consul:8500",
				Err: &net.OpError{
					Op:  "dial",
					Net: "tcp",
					Err: errors.New("connection refused"),
				},
			},
			retryable: true,
		},
		{
			name:      "topo NodeExists",
			err:       topo.NewError(topo.NodeExists, "/some/path"),
			retryable: false,
		},
		{
			name:      "topo BadVersion",
			err:       topo.NewError(topo.BadVersion, "/some/path"),
			retryable: false,
		},
		{
			name:      "topo NoNode",
			err:       topo.NewError(topo.NoNode, "/some/path"),
			retryable: false,
		},
		{
			name:      "ErrBadResponse",
			err:       ErrBadResponse,
			retryable: false,
		},
		{
			name:      "consul 500",
			err:       errors.New("Unexpected response code: 500"),
			retryable: true,
		},
		{
			name:      "consul 503",
			err:       errors.New("Unexpected response code: 503"),
			retryable: true,
		},
		{
			name:      "consul 403",
			err:       errors.New("Unexpected response code: 403"),
			retryable: false,
		},
		{
			name:      "connection refused string",
			err:       errors.New("dial tcp 127.0.0.1:8500: connection refused"),
			retryable: true,
		},
		{
			name:      "connection reset string",
			err:       errors.New("read tcp: connection reset by peer"),
			retryable: true,
		},
		{
			name:      "unknown error",
			err:       errors.New("something unexpected"),
			retryable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.retryable, isRetryableError(tt.err))
		})
	}
}

type mockKV struct {
	getCalls int
	getFunc  func(int) (*api.KVPair, *api.QueryMeta, error)
}

func (m *mockKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	m.getCalls++
	return m.getFunc(m.getCalls)
}

func (m *mockKV) List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	return nil, nil, nil
}

func (m *mockKV) Keys(prefix string, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error) {
	return nil, nil, nil
}

func (m *mockKV) Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error) {
	return false, nil, nil, nil
}

func TestRetryKV_Get_SucceedsFirstAttempt(t *testing.T) {
	mock := &mockKV{
		getFunc: func(call int) (*api.KVPair, *api.QueryMeta, error) {
			return &api.KVPair{Key: "test", Value: []byte("value")}, nil, nil
		},
	}
	r := newRetryKV(mock, 3, 1*time.Millisecond, 10*time.Millisecond, true)

	pair, _, err := r.Get("test", nil)
	require.NoError(t, err)
	assert.Equal(t, "test", pair.Key)
	assert.Equal(t, 1, mock.getCalls)
}

func TestRetryKV_Get_SucceedsOnRetry(t *testing.T) {
	mock := &mockKV{
		getFunc: func(call int) (*api.KVPair, *api.QueryMeta, error) {
			if call < 3 {
				return nil, nil, errors.New("Unexpected response code: 500")
			}
			return &api.KVPair{Key: "test", Value: []byte("value")}, nil, nil
		},
	}
	r := newRetryKV(mock, 3, 1*time.Millisecond, 10*time.Millisecond, true)

	pair, _, err := r.Get("test", nil)
	require.NoError(t, err)
	assert.Equal(t, "test", pair.Key)
	assert.Equal(t, 3, mock.getCalls)
}

func TestRetryKV_Get_NonRetryableReturnsImmediately(t *testing.T) {
	mock := &mockKV{
		getFunc: func(call int) (*api.KVPair, *api.QueryMeta, error) {
			return nil, nil, context.Canceled
		},
	}
	r := newRetryKV(mock, 3, 1*time.Millisecond, 10*time.Millisecond, true)

	_, _, err := r.Get("test", nil)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, mock.getCalls)
}

func TestRetryKV_Get_AllAttemptsExhausted(t *testing.T) {
	mock := &mockKV{
		getFunc: func(call int) (*api.KVPair, *api.QueryMeta, error) {
			return nil, nil, errors.New("Unexpected response code: 503")
		},
	}
	r := newRetryKV(mock, 3, 1*time.Millisecond, 10*time.Millisecond, true)

	_, _, err := r.Get("test", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "503")
	assert.Equal(t, 3, mock.getCalls)
}

func TestRetryKV_Get_DisabledSkipsRetry(t *testing.T) {
	mock := &mockKV{
		getFunc: func(call int) (*api.KVPair, *api.QueryMeta, error) {
			return nil, nil, errors.New("Unexpected response code: 500")
		},
	}
	r := newRetryKV(mock, 3, 1*time.Millisecond, 10*time.Millisecond, false)

	_, _, err := r.Get("test", nil)
	assert.Error(t, err)
	assert.Equal(t, 1, mock.getCalls)
}

func TestRetryKV_Backoff(t *testing.T) {
	r := &retryKV{
		baseDelay: 100 * time.Millisecond,
		maxDelay:  1 * time.Second,
	}

	for i := 0; i < 100; i++ {
		d1 := r.backoff(1)
		assert.Greater(t, d1, 50*time.Millisecond)
		assert.Less(t, d1, 150*time.Millisecond)

		d2 := r.backoff(2)
		assert.Greater(t, d2, 100*time.Millisecond)
		assert.Less(t, d2, 300*time.Millisecond)
	}

	d10 := r.backoff(10)
	assert.LessOrEqual(t, d10, r.maxDelay+r.maxDelay/4)
}

func TestRetryKV_Get_ContextCanceledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mock := &mockKV{
		getFunc: func(call int) (*api.KVPair, *api.QueryMeta, error) {
			if call == 1 {
				cancel()
			}
			return nil, nil, errors.New("Unexpected response code: 500")
		},
	}
	r := newRetryKV(mock, 3, 500*time.Millisecond, 5*time.Second, true)

	opts := (&api.QueryOptions{}).WithContext(ctx)
	start := time.Now()
	_, _, err := r.Get("test", opts)
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, mock.getCalls)
	assert.Less(t, elapsed, 200*time.Millisecond)
}

func TestRetryKV_Backoff_ZeroBaseDelay(t *testing.T) {
	r := &retryKV{
		baseDelay: 0,
		maxDelay:  1 * time.Second,
	}

	assert.NotPanics(t, func() {
		d := r.backoff(1)
		assert.Equal(t, time.Duration(0), d)
	})
}
