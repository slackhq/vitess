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

package registry

import "context"

// Pool identifies which vttablet connection pool an admission request targets.
// Admission is per-pool because the pools have independent capacity and may
// warrant independent admission policy.
type Pool int

const (
	// PoolOltpRead is the OLTP read pool (non-transactional queries).
	PoolOltpRead Pool = iota
	// PoolTx is the transaction pool.
	PoolTx
)

// AdmissionController is an OPTIONAL capability a strategy may implement in
// addition to ThrottlingStrategyHandler.
//
// Where Evaluate makes a one-shot, stateless verdict at the top of query
// execution, Admit gates entry to a specific connection pool for the lifetime
// of the reservation: it blocks until the request is admitted or rejected, and
// on admission returns a release the caller must invoke when the connection is
// freed. This models occupancy-based admission control (e.g. a CoDel gate whose
// signal is pool backpressure), which a fire-and-forget predicate cannot
// express.
//
// The QueryThrottler discovers this capability by type-asserting its active
// strategy; strategies that do not implement it are unaffected and their pool
// entry is never gated.
type AdmissionController interface {
	// Admit blocks until the request is admitted to pool or rejected. On
	// admission it returns a non-nil release that the caller MUST invoke exactly
	// once when the reservation ends (the optional error records why the work
	// released, for observability). On rejection it returns a nil release and a
	// non-nil error, which the caller maps to a rejection response.
	Admit(ctx context.Context, attrs QueryAttributes, pool Pool) (release func(err error), err error)
}
