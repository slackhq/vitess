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
	"math"

	"vitess.io/vitess/go/list"
)

type CoDelQueue[T any] struct {
	queue        list.List[*Request[T]]
	dropping     bool
	dropNextNs   int64
	count        int
	droppableLen int
	droppable    droppableIndex[T]

	cfg               CoDelConfig
	nowNs             func() int64
	scheduleDropTimer func(delayNs int64)
	stopDropTimer     func()
}

func newCoDelQueue[T any](cfg CoDelConfig, nowNs func() int64, scheduleDropTimer func(int64), stopDropTimer func()) *CoDelQueue[T] {
	q := &CoDelQueue[T]{
		count:             1,
		cfg:               cfg,
		nowNs:             nowNs,
		scheduleDropTimer: scheduleDropTimer,
		stopDropTimer:     stopDropTimer,
	}
	q.queue.Init()
	q.droppable.init()
	return q
}

func (q *CoDelQueue[T]) lockedLen() int {
	return q.queue.Len()
}

func (q *CoDelQueue[T]) lockedIsHealthy() bool {
	return !q.dropping
}

func (q *CoDelQueue[T]) lockedEnqueueIf(req *Request[T], enabled bool) {
	now := q.nowNs()
	req.codelqEnqueuedAtNs = now
	req.codelqElem = q.queue.PushBack(req)
	if !req.isDroppable() {
		return
	}

	q.droppableLen++
	q.droppable.insert(req)
	if !enabled {
		q.lockedDisable()
		return
	}
	if q.dropNextNs != 0 && q.droppableLen != 1 {
		return
	}
	if q.dropNextNs > 0 {
		q.lockedAdvance(now, func() bool { return false })
	}
	q.dropNextNs = q.lockedControlLaw(now)
	q.lockedArmDropTimer()
}

func (q *CoDelQueue[T]) lockedDisable() {
	q.dropping = false
	q.dropNextNs = 0
	q.count = 1
	q.stopDropTimer()
}

func (q *CoDelQueue[T]) lockedEnable() {
	if q.dropNextNs != 0 || q.droppableLen == 0 {
		return
	}
	now := q.nowNs()
	q.dropNextNs = q.lockedControlLaw(now)
	q.lockedArmDropTimer()
}

func (q *CoDelQueue[T]) lockedPeek() *Request[T] {
	if elem := q.queue.Front(); elem != nil {
		return elem.Value
	}
	return nil
}

func (q *CoDelQueue[T]) lockedFind(match func(T) bool) *Request[T] {
	for elem := q.queue.Front(); elem != nil; elem = elem.Next() {
		if match(elem.Value.value) {
			return elem.Value
		}
	}
	return nil
}

func (q *CoDelQueue[T]) lockedRemove(req *Request[T]) {
	if req.codelqElem == nil {
		return
	}
	q.queue.Remove(req.codelqElem)
	req.codelqElem = nil
	if !req.isDroppable() {
		return
	}

	q.droppableLen--
	q.droppable.remove(req)
	if q.droppableLen == 0 {
		q.dropping = false
	}
}

func (q *CoDelQueue[T]) lockedDequeue(req *Request[T]) {
	if q.nowNs()-req.codelqEnqueuedAtNs < q.lockedTargetNs() {
		q.dropping = false
	}
	q.lockedRemove(req)
}

func (q *CoDelQueue[T]) lockedFindLowestPriorityDroppable() *Request[T] {
	return q.droppable.min()
}

func (q *CoDelQueue[T]) lockedRunTimer(drop func() bool) {
	now := q.nowNs()
	if q.dropNextNs == 0 || now < q.dropNextNs {
		return
	}

	q.lockedAdvance(now, drop)
	if q.droppableLen > 0 || q.count > 1 {
		q.lockedArmDropTimer()
	} else {
		q.dropNextNs = 0
	}
}

func (q *CoDelQueue[T]) lockedAdvance(now int64, drop func() bool) {
	for now >= q.dropNextNs && (q.droppableLen > 0 || q.count > 1) {
		dropped := false
		if q.dropping {
			dropped = drop()
			if dropped {
				q.count++
				q.dropNextNs = q.lockedControlLaw(q.dropNextNs)
			}
		}
		if !dropped {
			q.count = q.lockedEaseCount()
			q.dropNextNs = q.lockedControlLaw(q.dropNextNs)
		}

		q.dropping = false
		if oldest := q.lockedPeek(); q.droppableLen > 0 && oldest != nil && oldest.codelqEnqueuedAtNs < q.dropNextNs {
			q.dropping = true
		}
	}
}

func (q *CoDelQueue[T]) lockedEaseCount() int {
	base := 3.0
	if q.cfg.EasingLogBase != nil {
		base = q.cfg.EasingLogBase()
	}
	if base <= 1 {
		base = 3
	}
	step := int(math.Log(float64(q.count)) / math.Log(base) / base)
	return max(q.count-max(step, 1), 1)
}

func (q *CoDelQueue[T]) lockedControlLaw(now int64) int64 {
	return now + q.lockedCurrentInterval()
}

func (q *CoDelQueue[T]) lockedTargetNs() int64 {
	if q.count == 1 && q.cfg.InitialTargetNs != nil {
		if target := q.cfg.InitialTargetNs(); target > 0 {
			return target
		}
	}
	return q.cfg.TargetNs()
}

func (q *CoDelQueue[T]) lockedCurrentInterval() int64 {
	interval := q.cfg.IntervalNs()
	if q.count <= 1 {
		if q.cfg.InitialIntervalNs != nil {
			if initialInterval := q.cfg.InitialIntervalNs(); initialInterval > 0 {
				return initialInterval
			}
		}
		return interval
	}
	return max(int64(float64(interval)/math.Pow(float64(q.count), q.cfg.Exponent())), 1)
}

func (q *CoDelQueue[T]) lockedArmDropTimer() {
	q.dropping = q.droppableLen > 0
	q.scheduleDropTimer(max(q.dropNextNs-q.nowNs(), q.cfg.MinDropDelayNs()))
}
