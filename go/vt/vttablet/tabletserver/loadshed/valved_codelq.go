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

type (
	DroppedRequestError struct{}

	ValvedCoDelQueue[T any] struct {
		codelq            *CoDelQueue[T]
		valves            map[string][]*Request[T]
		droppablePerValve map[string]*Request[T]
		pendingDrops      []*Request[T]
		mode              func() Mode
	}
)

func (e *DroppedRequestError) Error() string {
	return "request dropped by CoDel queue"
}

func newValvedCoDelQueue[T any](cfg CoDelConfig, nowNs func() int64, scheduleDropTimer func(int64), stopDropTimer func(), mode func() Mode) *ValvedCoDelQueue[T] {
	q := &ValvedCoDelQueue[T]{
		valves:            make(map[string][]*Request[T]),
		droppablePerValve: make(map[string]*Request[T]),
		mode:              mode,
	}
	q.codelq = newCoDelQueue[T](cfg, nowNs, scheduleDropTimer, stopDropTimer)
	return q
}

func (q *ValvedCoDelQueue[T]) lockedCurrentInterval() int64 {
	return q.codelq.lockedCurrentInterval()
}

func (q *ValvedCoDelQueue[T]) lockedDroppableLen() int {
	return q.codelq.droppableLen
}

func (q *ValvedCoDelQueue[T]) lockedCount() int {
	return q.codelq.count
}

func (q *ValvedCoDelQueue[T]) lockedLen() int {
	return q.codelq.lockedLen()
}

func (q *ValvedCoDelQueue[T]) lockedValveDepth(valveID string) int {
	return len(q.valves[valveID])
}

func (q *ValvedCoDelQueue[T]) lockedIsHealthy() bool {
	return q.codelq.lockedIsHealthy()
}

func (q *ValvedCoDelQueue[T]) lockedEnqueue(valveID string, priority float64) *Request[T] {
	var zero T
	req := newRequest(zero, priority)
	req.valveID = valveID
	if valveID != "" && q.droppablePerValve[valveID] != nil {
		q.valves[valveID] = append(q.valves[valveID], req)
		return req
	}
	q.lockedEnqueueToCoDel(req)
	return req
}

func (q *ValvedCoDelQueue[T]) lockedDrop(req *Request[T]) {
	q.codelq.lockedRemove(req)
	req.signal(&DroppedRequestError{})
	q.pendingDrops = append(q.pendingDrops, req)
	q.lockedPromoteOnEvict(req)
}

func (q *ValvedCoDelQueue[T]) lockedTakePendingDrops() []*Request[T] {
	dropped := q.pendingDrops
	q.pendingDrops = nil
	return dropped
}

func (q *ValvedCoDelQueue[T]) lockedCancel(req *Request[T]) {
	if req.codelqElem != nil {
		q.codelq.lockedRemove(req)
		req.signal(&DroppedRequestError{})
		q.lockedPromoteOnEvict(req)
		return
	}
	if req.signaledValue == nil {
		req.signal(&DroppedRequestError{})
	}
}

func (q *ValvedCoDelQueue[T]) lockedRunTimerIf(enabled bool) {
	if !enabled {
		q.codelq.lockedDisable()
		return
	}
	q.codelq.lockedEnable()
	q.codelq.lockedRunTimer(func() bool {
		if q.codelq.droppableLen <= keepDroppableFloor {
			return false
		}
		req := q.codelq.lockedFindLowestPriorityDroppable()
		if req == nil {
			return false
		}
		q.lockedDrop(req)
		return true
	})
}

func (q *ValvedCoDelQueue[T]) lockedDequeue(req *Request[T]) {
	q.codelq.lockedDequeue(req)
	if req.valveID == "" {
		return
	}
	delete(q.droppablePerValve, req.valveID)
	q.lockedPromote(req.valveID)
}

func (q *ValvedCoDelQueue[T]) lockedPeek() *Request[T] {
	return q.codelq.lockedPeek()
}

func (q *ValvedCoDelQueue[T]) lockedFind(match func(T) bool) *Request[T] {
	return q.codelq.lockedFind(match)
}

func (q *ValvedCoDelQueue[T]) lockedEnqueueToCoDel(req *Request[T]) {
	if req.valveID != "" {
		q.droppablePerValve[req.valveID] = req
	}
	q.codelq.lockedEnqueueIf(req, q.mode == nil || q.mode() == ModeEnabled)
}

func (q *ValvedCoDelQueue[T]) lockedPromoteOnEvict(req *Request[T]) {
	if req.valveID == "" {
		return
	}
	delete(q.droppablePerValve, req.valveID)
	q.lockedPromote(req.valveID)
}

func (q *ValvedCoDelQueue[T]) lockedPromote(valveID string) {
	q.lockedClearDone(valveID)
	pending := q.valves[valveID]
	if len(pending) == 0 {
		return
	}

	next := pending[0]
	pending[0] = nil
	pending = pending[1:]
	if len(pending) == 0 {
		delete(q.valves, valveID)
	} else {
		q.valves[valveID] = pending
	}
	q.lockedEnqueueToCoDel(next)
}

func (q *ValvedCoDelQueue[T]) lockedClearDone(valveID string) {
	pending := q.valves[valveID]
	for len(pending) > 0 && pending[0].signaledValue != nil {
		pending[0] = nil
		pending = pending[1:]
	}
	if len(pending) == 0 {
		delete(q.valves, valveID)
	} else {
		q.valves[valveID] = pending
	}
}
