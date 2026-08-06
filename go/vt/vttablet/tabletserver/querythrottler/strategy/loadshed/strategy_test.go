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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func newTestSnake(capacity int) *loadshed.Snake {
	return loadshed.NewSnake(loadshed.SnakeConfig{
		Name: "test",
		CoDel: loadshed.CoDelConfig{
			TargetNs:       func() int64 { return (5 * time.Millisecond).Nanoseconds() },
			IntervalNs:     func() int64 { return (100 * time.Millisecond).Nanoseconds() },
			Exponent:       func() float64 { return 1 },
			MinDropDelayNs: func() int64 { return int64(time.Millisecond) },
		},
		Capacity:            func() int { return capacity },
		LoadsheddingAllowed: func() bool { return true },
	})
}

func TestSnakePriority(t *testing.T) {
	s := &Strategy{}

	tcases := []struct {
		name  string
		attrs registry.QueryAttributes
		want  float64
	}{
		{"most important inverts to highest", registry.QueryAttributes{Priority: 0}, 100},
		{"least important inverts to lowest", registry.QueryAttributes{Priority: 100}, 0},
		{"mid-range inverts", registry.QueryAttributes{Priority: 30}, 70},
	}
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, s.snakePriority(tc.attrs))
		})
	}
}

func TestSnakePriority_UndroppableSchema(t *testing.T) {
	s := &Strategy{undroppableSchemas: []string{"performance_schema", "mysql"}}

	got := s.snakePriority(registry.QueryAttributes{
		Priority:         50,
		SchemaQualifiers: []string{"Performance_Schema"},
	})
	assert.Equal(t, loadshed.PriorityUndroppable, got)

	got = s.snakePriority(registry.QueryAttributes{
		Priority:         50,
		SchemaQualifiers: []string{"myapp"},
	})
	assert.Equal(t, float64(50), got)
}

func TestAdmit_DispatchesByPool(t *testing.T) {
	oltp := newTestSnake(1)
	tx := newTestSnake(1)
	s := &Strategy{gates: map[tabletenv.PoolType]*loadshed.Snake{
		tabletenv.PoolTypeOltpRead: oltp,
		tabletenv.PoolTypeTx:       tx,
	}}

	release, err := s.Admit(context.Background(), registry.QueryAttributes{}, tabletenv.PoolTypeTx)
	require.NoError(t, err)
	require.NotNil(t, release)
	assert.Equal(t, 1, tx.Stats().HolderCount, "tx gate should hold the slot")
	assert.Equal(t, 0, oltp.Stats().HolderCount, "oltp gate should be untouched")

	release(nil)
	assert.Equal(t, 0, tx.Stats().HolderCount, "slot released")
}

func TestAdmit_UnconfiguredPoolAdmits(t *testing.T) {
	s := &Strategy{gates: map[tabletenv.PoolType]*loadshed.Snake{tabletenv.PoolTypeOltpRead: newTestSnake(1)}}

	release, err := s.Admit(context.Background(), registry.QueryAttributes{}, tabletenv.PoolTypeTx)
	require.NoError(t, err)
	require.NotNil(t, release)
	release(nil)
}

func TestMatchesUndroppableSchema(t *testing.T) {
	allow := []string{"performance_schema", "information_schema", "sys", "mysql"}

	tcases := []struct {
		name       string
		qualifiers []string
		allowlist  []string
		want       bool
	}{
		{"unqualified query", nil, allow, false},
		{"empty allowlist", []string{"performance_schema"}, nil, false},
		{"match", []string{"performance_schema"}, allow, true},
		{"case-insensitive match", []string{"Performance_Schema"}, allow, true},
		{"no match", []string{"myapp"}, allow, false},
		{"one of several matches", []string{"myapp", "sys"}, allow, true},
		{"none of several match", []string{"myapp", "other"}, allow, false},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, matchesUndroppableSchema(tc.qualifiers, tc.allowlist))
		})
	}
}
