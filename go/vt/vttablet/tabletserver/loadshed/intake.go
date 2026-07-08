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
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	_ "unsafe" // for go:linkname
)

// procPin/procUnpin give a stable per-P index so an Acquire enqueues into the
// shard for the CPU it is currently running on. Because a P runs only one
// goroutine at a time and procPin prevents preemption/migration while pinned,
// concurrent enqueues almost never contend on the same shard's lock — that is
// the whole point of the intake: get arrivals off the single Snake mutex.
//
//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin()

type (
	// intakeShard is a per-CPU staging queue of arrived-but-not-yet-admitted
	// requests. Each request records its arrival time in codelqEnqueuedAtNs so
	// that when it is later merged into the CoDel queue its sojourn is measured
	// from arrival, not from merge time — preserving CoDel's time-to-grant
	// signal exactly.
	intakeShard struct {
		mu   sync.Mutex
		reqs []*Request
	}

	// intake is a set of per-CPU shards feeding a single CoDel queue. Arrivals
	// push to their P's shard lock-free of the Snake mutex; a merge (triggered
	// when the CoDel queue has no waiter left to serve) drains every shard,
	// sorts by arrival time, and admits them in FIFO order.
	intake struct {
		shards []intakeShard
		// pending is the number of requests sitting in shards not yet merged
		// into the CoDel queue. It is the fast-path gate: Acquire can consult it
		// atomically without taking the Snake mutex to know a backlog exists.
		pending atomic.Int64
	}
)

func newIntake() *intake {
	return &intake{shards: make([]intakeShard, runtime.NumCPU())}
}

// push stages req on the shard for the current P and bumps pending. Lock-free of
// the Snake mutex. The caller must have already stamped req.codelqEnqueuedAtNs
// with the arrival time.
func (in *intake) push(req *Request) {
	p := procPin()
	shard := &in.shards[p%len(in.shards)]
	procUnpin()

	shard.mu.Lock()
	shard.reqs = append(shard.reqs, req)
	shard.mu.Unlock()

	in.pending.Add(1)
}

// pendingLen reports the number of staged requests (fast-path gate).
func (in *intake) pendingLen() int64 {
	return in.pending.Load()
}

// drain removes and returns all staged requests across every shard, sorted by
// arrival time (codelqEnqueuedAtNs) so the caller can admit them in FIFO order.
// pending is decremented by the number drained. Returns nil when empty.
func (in *intake) drain() []*Request {
	if in.pending.Load() == 0 {
		return nil
	}
	var out []*Request
	for i := range in.shards {
		s := &in.shards[i]
		s.mu.Lock()
		if len(s.reqs) > 0 {
			out = append(out, s.reqs...)
			// Release the backing array so a burst doesn't pin memory.
			s.reqs = nil
		}
		s.mu.Unlock()
	}
	if len(out) == 0 {
		return nil
	}
	in.pending.Add(-int64(len(out)))
	sort.SliceStable(out, func(a, b int) bool {
		return out[a].codelqEnqueuedAtNs < out[b].codelqEnqueuedAtNs
	})
	return out
}
