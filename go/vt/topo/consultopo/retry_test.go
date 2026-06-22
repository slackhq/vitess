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
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "non-transient error",
			err:      errors.New("some random error"),
			expected: false,
		},
		{
			name:     "500 status error",
			err:      api.StatusError{Code: 500, Body: "No cluster leader"},
			expected: true,
		},
		{
			name:     "403 status error is not transient",
			err:      api.StatusError{Code: 403, Body: "Permission denied"},
			expected: false,
		},
		{
			name:     "404 status error is not transient",
			err:      api.StatusError{Code: 404, Body: "Not found"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isTransientError(tt.err))
		})
	}
}

func TestRetryOnTransientError_ImmediateSuccess(t *testing.T) {
	calls := 0
	err := retryOnTransientError(context.Background(), func() error {
		calls++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestRetryOnTransientError_NonTransientError(t *testing.T) {
	calls := 0
	expectedErr := errors.New("permanent error")
	err := retryOnTransientError(context.Background(), func() error {
		calls++
		return expectedErr
	})
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, 1, calls)
}

func TestRetryOnTransientError_TransientThenSuccess(t *testing.T) {
	origInterval := consulRetryInterval
	consulRetryInterval = 10 * time.Millisecond
	defer func() { consulRetryInterval = origInterval }()

	calls := 0
	err := retryOnTransientError(context.Background(), func() error {
		calls++
		if calls < 3 {
			return api.StatusError{Code: 500, Body: "No cluster leader"}
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestRetryOnTransientError_TransientUntilTimeout(t *testing.T) {
	origInterval := consulRetryInterval
	origTimeout := consulRetryTimeout
	consulRetryInterval = 10 * time.Millisecond
	consulRetryTimeout = 100 * time.Millisecond
	defer func() {
		consulRetryInterval = origInterval
		consulRetryTimeout = origTimeout
	}()

	transientErr := api.StatusError{Code: 500, Body: "No cluster leader"}
	err := retryOnTransientError(context.Background(), func() error {
		return transientErr
	})
	assert.Equal(t, transientErr, err)
}

func TestRetryOnTransientError_ContextCanceled(t *testing.T) {
	origInterval := consulRetryInterval
	consulRetryInterval = 10 * time.Millisecond
	defer func() { consulRetryInterval = origInterval }()

	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	err := retryOnTransientError(ctx, func() error {
		calls++
		if calls == 2 {
			cancel()
		}
		return api.StatusError{Code: 500, Body: "No cluster leader"}
	})
	assert.Equal(t, context.Canceled, err)
}
