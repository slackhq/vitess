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
	"golang.org/x/sys/unix"
)

// These helpers operate on the current OS thread (pid 0). Callers must hold the
// goroutine to its thread via runtime.LockOSThread before invoking them.

// schedPolicyIdle is the scheduling policy reported when a thread runs at
// SCHED_IDLE, the lowest priority on Linux. schedPolicyNormal is the default
// time-sharing policy (SCHED_OTHER/SCHED_NORMAL, value 0).
const (
	schedPolicyIdle   = int(unix.SCHED_IDLE)
	schedPolicyNormal = int(unix.SCHED_NORMAL)
)

// setThreadSchedIdle sets the calling OS thread to the SCHED_IDLE policy. The
// kernel only schedules SCHED_IDLE threads when no other thread on the CPU
// wants to run, so being scheduled is itself proof that the CPU is idle.
func setThreadSchedIdle() error {
	attr := unix.SchedAttr{
		Size:   unix.SizeofSchedAttr,
		Policy: unix.SCHED_IDLE,
	}
	return unix.SchedSetAttr(0, &attr, 0)
}

// getThreadSchedPolicy returns the scheduling policy of the calling OS thread.
func getThreadSchedPolicy() (int, error) {
	attr, err := unix.SchedGetAttr(0, 0)
	if err != nil {
		return 0, err
	}
	return int(attr.Policy), nil
}

// pinToCore binds the calling OS thread to a single CPU core, giving per-core
// idle sampling.
func pinToCore(cpu int) error {
	var set unix.CPUSet
	set.Zero()
	set.Set(cpu)
	return unix.SchedSetaffinity(0, &set)
}

// schedYield moves the calling thread to the back of its run queue. It is used
// only to pace successive grants while a backlog exists; it does not sleep, so
// it must never be the mechanism that parks an otherwise-idle granter.
func schedYield() {
	_, _, _ = unix.Syscall(unix.SYS_SCHED_YIELD, 0, 0, 0)
}
