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

package loadshed

import (
	"context"
	"strings"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const strategyName = "loadshed"

type Gate struct {
	Pool  tabletenv.PoolType
	Snake *loadshed.Snake
}

type Strategy struct {
	gates              map[tabletenv.PoolType]*loadshed.Snake
	undroppableSchemas []string
}

func New(undroppableSchemas []string, gates ...Gate) *Strategy {
	m := make(map[tabletenv.PoolType]*loadshed.Snake, len(gates))
	for _, g := range gates {
		m[g.Pool] = g.Snake
	}
	return &Strategy{
		gates:              m,
		undroppableSchemas: undroppableSchemas,
	}
}

func (s *Strategy) Evaluate(ctx context.Context, targetTabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, transactionID int64, attrs registry.QueryAttributes) registry.ThrottleDecision {
	return registry.ThrottleDecision{Throttle: false}
}

func (s *Strategy) Admit(ctx context.Context, attrs registry.QueryAttributes, pool tabletenv.PoolType) (func(err error), error) {
	snake, ok := s.gates[pool]
	if !ok || snake == nil {
		return func(error) {}, nil
	}

	priority := s.snakePriority(attrs)
	unlock, err := snake.Acquire(ctx, attrs.FairnessKey, priority)
	if err != nil {
		return nil, err
	}
	return func(releaseErr error) { unlock.Release(releaseErr) }, nil
}

func (s *Strategy) snakePriority(attrs registry.QueryAttributes) float64 {
	if matchesUndroppableSchema(attrs.SchemaQualifiers, s.undroppableSchemas) {
		return loadshed.PriorityUndroppable
	}
	return float64(sqlparser.MaxPriorityValue - attrs.Priority)
}

func (s *Strategy) Start() {}

func (s *Strategy) Stop() {}

func (s *Strategy) GetStrategyName() string { return strategyName }

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
