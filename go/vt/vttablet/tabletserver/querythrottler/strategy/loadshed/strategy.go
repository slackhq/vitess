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
	"time"

	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const strategyName = "loadshed"

var poolStatsName = map[tabletenv.PoolType]string{
	tabletenv.PoolTypeOltpRead: "SnakeOltpRead",
	tabletenv.PoolTypeTx:       "SnakeDml",
}

func init() {
	registry.Register(querythrottlerpb.ThrottlingStrategy_LOADSHED, factory{})
}

type factory struct{}

func (factory) New(deps registry.Deps, cfg registry.StrategyConfig) (registry.ThrottlingStrategyHandler, error) {
	config := deps.TabletConfig
	gates := make(map[tabletenv.PoolType]*loadshed.Snake, len(deps.PoolCapacities))
	for pool, capacity := range deps.PoolCapacities {
		snake := loadshed.NewSnake(loadshed.SnakeConfig{
			Name: poolStatsName[pool],
			CoDel: loadshed.CoDelConfig{
				TargetNs: func() int64 { return config.LoadshedTarget.Nanoseconds() },
				IntervalNs: func() int64 {
					return int64(float64(config.LoadshedTarget.Nanoseconds()) * config.LoadshedIntervalRatio)
				},
				Exponent:       func() float64 { return 1 },
				MinDropDelayNs: func() int64 { return int64(100 * time.Millisecond) },
				TriggerNs:      func() int64 { return config.LoadshedTrigger.Nanoseconds() },
				DropMode:       func() loadshed.CoDelDropMode { mode, _ := loadshed.ParseDropMode(config.LoadshedDropMode); return mode },
				GraceCount:     func() int { return config.LoadshedGraceCount },
			},
			Capacity:            capacity,
			LoadsheddingAllowed: func() bool { return true },
		})
		loadshed.PublishStats(deps.Env.Exporter(), poolStatsName[pool], snake)
		gates[pool] = snake
	}
	return &Strategy{
		gates:              gates,
		undroppableSchemas: config.LoadshedUndroppableSchemas,
	}, nil
}

type Strategy struct {
	gates              map[tabletenv.PoolType]*loadshed.Snake
	undroppableSchemas []string
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
	unlock, err := snake.Acquire(ctx, attrs.AppExecutionContextID, priority)
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
