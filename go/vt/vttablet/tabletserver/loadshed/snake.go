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

type (
	Mode string

	CoDelConfig struct {
		IntervalNs     func() int64
		TargetNs       func() int64
		Exponent       func() float64
		MinDropDelayNs func() int64
	}

	SnakeConfig struct {
		CoDel            CoDelConfig
		Mode             func() Mode
		DropTimerFired   func()
		ShadowTimerFired func()
	}

	Request[T any] struct {
		elem  *list.Element[*Request[T]]
		value T
	}

	Snake[T any] struct {
		queue list.List[*Request[T]]
	}
)

const (
	ModeOff     Mode = "off"
	ModeShadow  Mode = "shadow"
	ModeEnabled Mode = "enabled"
)

var PriorityUndroppable = math.Inf(-1)

func NewSnake[T any](_ SnakeConfig) *Snake[T] {
	s := &Snake[T]{}
	s.queue.Init()
	return s
}

func (s *Snake[T]) Enqueue(value T, priority float64) (*Request[T], []T) {
	return s.enqueue(value, priority)
}

func (s *Snake[T]) EnqueueExisting(value T, priority float64) (*Request[T], []T) {
	return s.enqueue(value, priority)
}

func (s *Snake[T]) enqueue(value T, _ float64) (*Request[T], []T) {
	req := &Request[T]{value: value}
	req.elem = s.queue.PushBack(req)
	return req, nil
}

func (s *Snake[T]) Dequeue() (T, bool, []T) {
	return s.dequeue(nil)
}

func (s *Snake[T]) DequeueMatching(match func(T) bool) (T, bool, []T) {
	return s.dequeue(match)
}

func (s *Snake[T]) dequeue(match func(T) bool) (T, bool, []T) {
	for elem := s.queue.Front(); elem != nil; elem = elem.Next() {
		req := elem.Value
		if match != nil && !match(req.value) {
			continue
		}

		s.queue.Remove(elem)
		req.elem = nil
		value := req.value
		var zero T
		req.value = zero
		return value, true, nil
	}

	var zero T
	return zero, false, nil
}

func (s *Snake[T]) Len() int {
	return s.queue.Len()
}

func (s *Snake[T]) CountMatching(match func(T) bool) int {
	count := 0
	for elem := s.queue.Front(); elem != nil; elem = elem.Next() {
		if match(elem.Value.value) {
			count++
		}
	}
	return count
}

func (s *Snake[T]) Drain() []T {
	values := make([]T, 0, s.Len())
	for {
		value, ok, _ := s.Dequeue()
		if !ok {
			return values
		}
		values = append(values, value)
	}
}

func (s *Snake[T]) Cancel(req *Request[T]) (bool, []T) {
	if req.elem == nil {
		return false, nil
	}

	s.queue.Remove(req.elem)
	req.elem = nil
	var zero T
	req.value = zero
	return true, nil
}

func (s *Snake[T]) CancelMatching(match func(T) bool) (bool, []T) {
	for elem := s.queue.Front(); elem != nil; elem = elem.Next() {
		req := elem.Value
		if match(req.value) {
			return s.Cancel(req)
		}
	}
	return false, nil
}

func (s *Snake[T]) LockedDropTimerFired() []T {
	return nil
}

func (s *Snake[T]) LockedShadowTimerFired() {
}
