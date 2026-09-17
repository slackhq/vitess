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

type Request[T any] struct {
	priority           float64
	codelqEnqueuedAtNs int64
	codelqElem         *list.Element[*Request[T]]
	value              T
	bucketElem         *list.Element[*Request[T]]
	bucketIdx          int
}

var PriorityUndroppable = math.Inf(-1)

func newRequest[T any](value T, priority float64) *Request[T] {
	return &Request[T]{
		priority: priority,
		value:    value,
	}
}

func (r *Request[T]) isDroppable() bool {
	return r.priority != PriorityUndroppable
}
