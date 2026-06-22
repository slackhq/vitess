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
	"net"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	consulRetryTimeout  = 15 * time.Second
	consulRetryInterval = 250 * time.Millisecond
)

func init() {
	servenv.RegisterFlagsForTopoBinaries(registerRetryFlags)
}

func registerRetryFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&consulRetryTimeout, "topo_consul_retry_timeout", consulRetryTimeout, "Total timeout for retrying consul operations that fail with transient errors (e.g. during leader elections).")
	fs.DurationVar(&consulRetryInterval, "topo_consul_retry_interval", consulRetryInterval, "Initial interval between retries for transient consul errors. Doubles on each retry (exponential backoff).")
}

// isTransientError returns true if the error is a transient consul error
// that should be retried, such as 500 "No cluster leader" during leader elections.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(net.Error); ok {
		return true
	}
	if se, ok := err.(api.StatusError); ok {
		return se.Code == 500
	}
	return api.IsRetryableError(err)
}

// retryOnTransientError retries the given function with exponential backoff
// when it returns a transient error. It respects the provided context for
// cancellation.
func retryOnTransientError(ctx context.Context, fn func() error) error {
	err := fn()
	if err == nil || !isTransientError(err) {
		return err
	}

	deadline := time.Now().Add(consulRetryTimeout)
	interval := consulRetryInterval

	for {
		if time.Now().After(deadline) {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}

		log.Warningf("consultopo: retrying after transient error: %v", err)

		err = fn()
		if err == nil || !isTransientError(err) {
			return err
		}

		interval = interval * 2
		if interval > 2*time.Second {
			interval = 2 * time.Second
		}
	}
}
