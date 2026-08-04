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

// Package loadshed adapts the CoDel-based Snake load-shedding gates to the
// querythrottler framework as an AdmissionController strategy. Unlike a
// metric-threshold strategy, it makes no top-of-execution verdict; its Evaluate
// always allows, and all work happens in Admit, which gates entry to a
// connection pool for the lifetime of the reservation.
package loadshed

import (
	"context"
	"strings"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
)

// strategyName is the GetStrategyName value, used as a stats label.
const strategyName = "loadshed"

// Gate is one Snake instance plus the pool it guards. The caller (tablet
// startup) builds these where the pools are in scope and hands them to New.
type Gate struct {
	Pool  registry.Pool
	Snake *loadshed.Snake
}

// Strategy is a querythrottler ThrottlingStrategyHandler that also implements
// registry.AdmissionController. It routes per-pool admission to the matching
// Snake gate. It is installed via QueryThrottler.InstallStrategy rather than
// built by the topo factory, because the Snakes it holds are wired to live
// connection-pool capacity that the factory cannot reach.
type Strategy struct {
	gates map[registry.Pool]*loadshed.Snake

	// undroppableSchemas lists schema qualifiers whose queries are never shed
	// (e.g. performance_schema health checks). Compared case-insensitively.
	undroppableSchemas []string
}

// New builds a Strategy from the given gates and undroppable-schema allowlist.
func New(undroppableSchemas []string, gates ...Gate) *Strategy {
	m := make(map[registry.Pool]*loadshed.Snake, len(gates))
	for _, g := range gates {
		m[g.Pool] = g.Snake
	}
	return &Strategy{
		gates:              m,
		undroppableSchemas: undroppableSchemas,
	}
}

// Evaluate always allows: this strategy does its work in Admit, not as a
// top-of-execution predicate. Implements registry.ThrottlingStrategyHandler.
func (s *Strategy) Evaluate(ctx context.Context, targetTabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, transactionID int64, attrs registry.QueryAttributes) registry.ThrottleDecision {
	return registry.ThrottleDecision{Throttle: false}
}

// Admit gates entry to the Snake gate for pool. It blocks until the request is
// granted or shed. Implements registry.AdmissionController.
func (s *Strategy) Admit(ctx context.Context, attrs registry.QueryAttributes, pool registry.Pool) (func(err error), error) {
	snake, ok := s.gates[pool]
	if !ok || snake == nil {
		// No gate configured for this pool: admit without shedding.
		return func(error) {}, nil
	}

	priority := s.snakePriority(attrs)
	unlock, err := snake.Acquire(ctx, attrs.FairnessKey, priority)
	if err != nil {
		return nil, err
	}
	return func(releaseErr error) { unlock.Release(releaseErr) }, nil
}

// snakePriority translates the proto priority (0 = most important, 100 = least)
// into Snake's convention (lower value shed first), and marks queries against a
// configured undroppable schema as never-shed.
func (s *Strategy) snakePriority(attrs registry.QueryAttributes) float64 {
	if matchesUndroppableSchema(attrs.SchemaQualifiers, s.undroppableSchemas) {
		return loadshed.PriorityUndroppable
	}
	return float64(sqlparser.MaxPriorityValue - attrs.Priority)
}

// Start implements registry.ThrottlingStrategyHandler. The Snakes are already
// running when handed to New, so there is nothing to start.
func (s *Strategy) Start() {}

// Stop implements registry.ThrottlingStrategyHandler.
func (s *Strategy) Stop() {}

// GetStrategyName implements registry.ThrottlingStrategyHandler.
func (s *Strategy) GetStrategyName() string { return strategyName }

// matchesUndroppableSchema reports whether any of the query's schema qualifiers
// is in the configured undroppable-schema allowlist (case-insensitive). The
// common case is queryQualifiers empty (unqualified tables), which returns
// immediately without scanning the allowlist.
func matchesUndroppableSchema(queryQualifiers, allowlist []string) bool {
	if len(queryQualifiers) == 0 || len(allowlist) == 0 {
		return false
	}
	for _, q := range queryQualifiers {
		for _, a := range allowlist {
			if strings.EqualFold(q, a) {
				return true
			}
		}
	}
	return false
}
