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

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
)

type (
	// idleGrantingSnake is the slice of Snake the granter needs: a single
	// attempt to grant one waiting request. It is defined here, where it is
	// consumed, so the granter does not depend on the full Snake type.
	idleGrantingSnake interface {
		// TryGrantIdle grants at most one waiting request if capacity allows,
		// reporting whether a grant was made.
		TryGrantIdle() bool
	}

	// idleGrantCounter decorates an idleGrantingSnake to count successful idle
	// grants for stats.
	idleGrantCounter struct {
		snake  idleGrantingSnake
		grants *stats.Counter
	}

	// idleGranter runs one goroutine per CPU, each locked to an OS thread set to
	// the SCHED_IDLE scheduling policy. Because the kernel only runs a SCHED_IDLE
	// thread when a core has no other work, a granter being scheduled to grant a
	// request is itself proof that at least one core is idle.
	//
	// The threads are deliberately NOT pinned to cores. A kick wakes a single
	// granter (the shared cap-1 wake channel has wake-one semantics); waking that
	// granter schedules work on whatever core it lands on, so a subsequent kick
	// must be served by another granter that the scheduler places on a different
	// idle core. Pinning would defeat this: a granter pinned to a now-busy core
	// could not run even while another core sits idle. Unpinned, N granters give
	// real cross-core parallelism, and each one that runs proves a distinct core
	// was idle.
	//
	// A granter never executes queries; it only calls TryGrantIdle. The granted
	// query runs on the caller's own (normal-priority) goroutine.
	idleGranter struct {
		snake   idleGrantingSnake
		workers int
		// yield paces successive grants while a backlog drains. It is a field
		// so tests can observe re-arm behavior; production uses schedYield.
		yield func()

		wake   chan struct{}
		stopCh chan struct{}
		wg     sync.WaitGroup
	}
)

// TryGrantIdle grants via the wrapped snake and counts successful grants.
func (c idleGrantCounter) TryGrantIdle() bool {
	if c.snake.TryGrantIdle() {
		if c.grants != nil {
			c.grants.Add(1)
		}
		return true
	}
	return false
}

// newIdleGranter creates a granter with one worker per CPU core. The snake may
// be set later via setSnake to break the construction cycle between the granter
// and the Snake it serves.
func newIdleGranter() *idleGranter {
	return &idleGranter{
		workers: runtime.NumCPU(),
		yield:   schedYield,
		wake:    make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}
}

// setSnake wires the Snake the granter grants on. Must be called before start.
func (g *idleGranter) setSnake(s idleGrantingSnake) {
	g.snake = s
}

// kick signals the granter that a request is waiting solely on the idle gate.
// It never blocks: the cap-1 channel coalesces redundant kicks.
func (g *idleGranter) kick() {
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

// start launches the granter goroutines. Each drops to SCHED_IDLE for its
// lifetime.
func (g *idleGranter) start() {
	for range g.workers {
		g.wg.Add(1)
		go g.run()
	}
}

// stop halts the granters and waits for them to exit.
func (g *idleGranter) stop() {
	close(g.stopCh)
	g.wg.Wait()
}

func (g *idleGranter) run() {
	defer g.wg.Done()

	// Lock to the OS thread for life and never unlock. We modify this thread's
	// scheduling policy (SCHED_IDLE); returning it to the Go runtime's thread
	// pool would let arbitrary goroutines (and GC work) run at idle priority. By
	// staying locked, when this goroutine returns on stop the runtime terminates
	// the thread, discarding the tainted scheduling state instead of recycling
	// it.
	//
	// The thread is deliberately NOT pinned to a core. A kick wakes a single
	// granter (the shared cap-1 wake channel has wake-one semantics), so pinning
	// would let a busy pinned core hide the fact that some other core is idle.
	// Unpinned, the woken granter can run on whichever core is free, so its
	// SCHED_IDLE thread being scheduled proves at least one core is idle.
	runtime.LockOSThread()
	// Best effort: a policy failure leaves the granter functional as a plain
	// worker (gating simply won't be CPU-accurate), so we don't abort. Log it so
	// operators can tell idle gating is not actually in effect.
	if err := setThreadSchedIdle(); err != nil {
		log.Warningf("idle granter: failed to set SCHED_IDLE, idle gating is not in effect: %v", err)
	}

	for {
		select {
		case <-g.stopCh:
			return
		case <-g.wake:
			// Grant one request per reschedule. After a successful grant we
			// yield so the kernel must reschedule this SCHED_IDLE thread before
			// the next grant — making each grant an independent idle proof, and
			// preventing a burst to capacity on a single idle slice. When
			// TryGrantIdle fails (empty queue or at capacity) we fall out and
			// block on <-wake, consuming no CPU until kicked again.
			for g.snake.TryGrantIdle() {
				g.yield()
			}
		}
	}
}
