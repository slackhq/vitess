/*
Copyright 2025 The Vitess Authors.

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

package smartconnpool

import (
	"math"
	"sync/atomic"
)

// CoDelConfig configures pool-level CoDel load shedding. A nil *CoDelConfig on
// Config disables shedding entirely.
type CoDelConfig struct {
	// TargetNs is the acceptable standing queue delay. A waiter whose sojourn
	// (time spent on the waitlist) stays below this is considered healthy.
	TargetNs int64
	// IntervalNs is the window over which delay must persist above target before
	// shedding begins. Conventionally a small multiple of the target.
	IntervalNs int64
	// Exponent is the control-law exponent: the inter-drop interval shrinks as
	// IntervalNs / count^Exponent. The OLTP wiring passes 1.
	Exponent float64
}

// codelState is a minimal, pool-level implementation of the CoDel (Controlled
// Delay) load-shedding algorithm. It drops the head-of-line waiter when the
// queue's realized sojourn has stayed above target for a full interval,
// following the classic CoDel control law.
//
// All methods assume the caller holds the waitlist mutex, except ShedCount
// which reads an atomic. There is no timer and no goroutine: the state machine
// is driven entirely by dequeues in tryReturnConnSlow (see the head-drop loop
// there), so shedding only advances when connections are returned.
type codelState struct {
	targetNs   int64
	intervalNs int64
	exponent   float64

	// firstAboveNs is the clock time at which the queue is allowed to enter the
	// dropping state: it is set to now+interval the first time sojourn crosses
	// target, and once now reaches it the queue starts dropping. Zero means the
	// queue is currently below target (healthy).
	firstAboveNs int64
	// dropNextNs is the clock time the next drop is due while dropping.
	dropNextNs int64
	// count is the drop intensity; it drives the inter-drop interval.
	count    uint32
	dropping bool

	shedCount atomic.Int64
}

func newCodelState(cfg CoDelConfig) *codelState {
	return &codelState{
		targetNs:   cfg.TargetNs,
		intervalNs: cfg.IntervalNs,
		exponent:   cfg.Exponent,
	}
}

// overTarget updates the CoDel signal with a fresh sojourn sample measured at
// dequeue and reports whether the queue is in the dropping state. A sub-target
// sample immediately returns the queue to healthy (the ease-out for this
// minimal version). An at/or/over-target sample arms the interval timer on the
// first crossing and, once the interval has elapsed while continuously above
// target, enters the dropping state.
func (c *codelState) overTarget(sojournNs, now int64) bool {
	if sojournNs < c.targetNs {
		c.firstAboveNs = 0
		c.dropping = false
		return false
	}
	if c.firstAboveNs == 0 {
		// First crossing: start the clock. Do not drop yet.
		c.firstAboveNs = now + c.intervalNs
		return false
	}
	if now >= c.firstAboveNs && !c.dropping {
		// Delay has persisted above target for a full interval: enter dropping
		// and pace the first drop one interval out rather than dropping a burst.
		c.dropping = true
		c.count = 1
		c.dropNextNs = now + c.intervalNs
	}
	return c.dropping
}

// dropDue reports whether a drop is due right now while dropping. When it
// returns true it advances the control law (count++, schedule the next drop)
// and records the shed. The caller must already be in the dropping state (see
// overTarget).
func (c *codelState) dropDue(now int64) bool {
	if !c.dropping || now < c.dropNextNs {
		return false
	}
	c.count++
	c.dropNextNs = c.controlLaw(c.dropNextNs)
	c.shedCount.Add(1)
	return true
}

// controlLaw returns the next drop time: t + interval/count^exponent, with a
// 1ns floor so the schedule always advances.
func (c *codelState) controlLaw(t int64) int64 {
	next := float64(c.intervalNs) / math.Pow(float64(c.count), c.exponent)
	step := max(int64(next), 1)
	return t + step
}

// ShedCount returns the cumulative number of waiters shed by CoDel.
func (c *codelState) ShedCount() int64 {
	return c.shedCount.Load()
}
