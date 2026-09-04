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
	"math/rand/v2"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

var (
	consulRetryCount     = 5
	consulRetryBaseDelay = 250 * time.Millisecond
	consulRetryMaxDelay  = 5 * time.Second
	consulRetryEnabled   = true
)

// kvClient defines the consul KV operations used by this package.
type kvClient interface {
	Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error)
	Keys(prefix string, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error)
	Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error)
}

// retryKV wraps a kvClient with configurable retry logic for transient errors.
type retryKV struct {
	inner     kvClient
	count     int
	baseDelay time.Duration
	maxDelay  time.Duration
	enabled   bool
}

func newRetryKV(inner kvClient, count int, baseDelay, maxDelay time.Duration, enabled bool) *retryKV {
	return &retryKV{
		inner:     inner,
		count:     count,
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		enabled:   enabled,
	}
}

func (r *retryKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	var pair *api.KVPair
	var meta *api.QueryMeta
	ctx := contextFromQueryOptions(q)
	err := r.retry(ctx, func() error {
		var ierr error
		pair, meta, ierr = r.inner.Get(key, q)
		return ierr
	})
	return pair, meta, err
}

func (r *retryKV) List(prefix string, q *api.QueryOptions) (api.KVPairs, *api.QueryMeta, error) {
	var pairs api.KVPairs
	var meta *api.QueryMeta
	ctx := contextFromQueryOptions(q)
	err := r.retry(ctx, func() error {
		var ierr error
		pairs, meta, ierr = r.inner.List(prefix, q)
		return ierr
	})
	return pairs, meta, err
}

func (r *retryKV) Keys(prefix string, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error) {
	var keys []string
	var meta *api.QueryMeta
	ctx := contextFromQueryOptions(q)
	err := r.retry(ctx, func() error {
		var ierr error
		keys, meta, ierr = r.inner.Keys(prefix, separator, q)
		return ierr
	})
	return keys, meta, err
}

// Txn retries transient errors, which means a CAS transaction that commits on
// the server but whose response is lost (e.g. TCP RST) will be re-submitted.
// The second attempt will observe the already-written state and return a
// consul-level conflict (manifesting as NodeExists, BadVersion, or NoNode to
// callers). This is the same outcome as no retry layer at all — the lost
// response would surface as a network error, and the caller's own retry would
// hit the same conflict — so retrying here doesn't worsen the window.
func (r *retryKV) Txn(txn api.KVTxnOps, q *api.QueryOptions) (bool, *api.KVTxnResponse, *api.QueryMeta, error) {
	var ok bool
	var resp *api.KVTxnResponse
	var meta *api.QueryMeta
	ctx := contextFromQueryOptions(q)
	err := r.retry(ctx, func() error {
		var ierr error
		ok, resp, meta, ierr = r.inner.Txn(txn, q)
		return ierr
	})
	return ok, resp, meta, err
}

func contextFromQueryOptions(q *api.QueryOptions) context.Context {
	if q != nil && q.Context() != nil {
		return q.Context()
	}
	return context.Background()
}

func (r *retryKV) retry(ctx context.Context, action func() error) error {
	if !r.enabled {
		return action()
	}

	var err error
	start := time.Now()
	for attempt := 0; attempt < r.count; attempt++ {
		if attempt > 0 {
			delay := r.backoff(attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		err = action()
		if err == nil {
			return nil
		}
		if !isRetryableError(err) {
			return err
		}
		log.Infof("consultopo: retryable error (attempt %d/%d, elapsed %v): %v", attempt+1, r.count, time.Since(start).Round(time.Millisecond), err)
	}
	return fmt.Errorf("%w (retried %d times over %v)", err, r.count, time.Since(start).Round(time.Millisecond))
}

func (r *retryKV) backoff(attempt int) time.Duration {
	delay := min(r.baseDelay*time.Duration(1<<uint(attempt-1)), r.maxDelay)
	if delay <= 0 {
		return 0
	}
	jitter := time.Duration(rand.Int64N(int64(delay)/2)) - delay/4
	return delay + jitter
}

// isRetryableError returns true if the error is transient and the operation
// should be retried.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return false
	}

	// A bare context.DeadlineExceeded means the caller's context expired.
	if errors.Is(err, context.DeadlineExceeded) {
		var urlErr *url.Error
		if !errors.As(err, &urlErr) {
			return false
		}
	}

	// topo errors are semantic (NodeExists, BadVersion, etc.), not transient.
	var topoErr *topo.Error
	if errors.As(err, &topoErr) {
		return false
	}

	if errors.Is(err, ErrBadResponse) {
		return false
	}

	// Network errors wrapped by the Go HTTP client.
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		var netErr net.Error
		if errors.As(urlErr.Err, &netErr) {
			return true
		}
		if errors.Is(urlErr.Err, context.DeadlineExceeded) {
			return true
		}
	}

	// Consul formats HTTP 5xx as "Unexpected response code: 5XX".
	msg := err.Error()
	if strings.Contains(msg, "Unexpected response code: 5") {
		return true
	}
	if strings.Contains(msg, "connection refused") {
		return true
	}
	if strings.Contains(msg, "connection reset") {
		return true
	}

	return false
}
