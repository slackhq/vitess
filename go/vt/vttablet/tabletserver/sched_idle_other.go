//go:build !linux

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
)

// schedPolicyIdle is a sentinel on non-Linux platforms, where SCHED_IDLE does
// not exist. The granter still runs as a plain worker pool, but gating is not
// real, so this value is only meaningful in Linux-gated tests. schedPolicyNormal
// mirrors the Linux normal policy value for cross-platform references.
const (
	schedPolicyIdle   = -1
	schedPolicyNormal = 0
)

// setThreadSchedIdle is a no-op on non-Linux platforms.
func setThreadSchedIdle() error {
	return nil
}

// getThreadSchedPolicy reports the sentinel policy on non-Linux platforms.
func getThreadSchedPolicy() (int, error) {
	return schedPolicyIdle, nil
}

// pinToCore is a no-op on non-Linux platforms.
func pinToCore(cpu int) error {
	return nil
}

// schedYield yields the goroutine on non-Linux platforms.
func schedYield() {
	runtime.Gosched()
}
