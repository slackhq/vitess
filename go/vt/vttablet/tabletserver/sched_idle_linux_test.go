//go:build linux

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

package tabletserver

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSchedIdle_SetAndGetPolicy verifies that setThreadSchedIdle actually moves
// the calling OS thread to SCHED_IDLE, observable via getThreadSchedPolicy.
func TestSchedIdle_SetAndGetPolicy(t *testing.T) {
	var (
		wg     sync.WaitGroup
		before int
		after  int
		setErr error
		getErr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Lock to the thread so the policy change applies to a thread we then
		// inspect, and never leaks to other goroutines.
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		before, _ = getThreadSchedPolicy()
		setErr = setThreadSchedIdle()
		after, getErr = getThreadSchedPolicy()
	}()
	wg.Wait()

	require.NoError(t, setErr, "setThreadSchedIdle should succeed on Linux")
	require.NoError(t, getErr)
	assert.Equal(t, schedPolicyNormal, before, "threads start at the normal policy")
	assert.Equal(t, schedPolicyIdle, after, "thread should be SCHED_IDLE after setThreadSchedIdle")
}

// TestSchedIdle_GranterThreadIsIdleNotCaller proves the central runtime
// guarantee: the granter's own thread runs at SCHED_IDLE, while the goroutine
// that performs the grant's downstream work (here, the caller of TryGrantIdle)
// runs at normal priority — SCHED_IDLE never applies to non-granter work.
func TestSchedIdle_GranterThreadIsIdleNotCaller(t *testing.T) {
	policyCh := make(chan int, 1)

	// A snake whose TryGrantIdle records the policy of the thread it runs on,
	// then reports no more work so the granter parks.
	snake := &policyRecordingSnake{policyCh: policyCh}

	g := newIdleGranter()
	g.workers = 1
	g.snake = snake
	g.yield = func() {}

	g.start()
	t.Cleanup(g.stop)

	g.kick()

	var granterPolicy int
	select {
	case granterPolicy = <-policyCh:
	case <-time.After(30 * time.Second):
		t.Fatal("granter never invoked TryGrantIdle")
	}

	assert.Equal(t, schedPolicyIdle, granterPolicy,
		"the granter thread must run at SCHED_IDLE")

	// The test goroutine (a normal goroutine, standing in for the query-running
	// caller) must NOT be SCHED_IDLE.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	callerPolicy, err := getThreadSchedPolicy()
	require.NoError(t, err)
	assert.Equal(t, schedPolicyNormal, callerPolicy,
		"non-granter goroutines must run at normal priority")
}

// policyRecordingSnake records the scheduling policy of the thread on which
// TryGrantIdle is invoked, exactly once, then always returns false.
type policyRecordingSnake struct {
	once     sync.Once
	policyCh chan int
}

func (s *policyRecordingSnake) TryGrantIdle() bool {
	s.once.Do(func() {
		p, _ := getThreadSchedPolicy()
		s.policyCh <- p
	})
	return false
}
