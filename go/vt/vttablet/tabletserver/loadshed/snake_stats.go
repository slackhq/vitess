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
	"vitess.io/vitess/go/stats"
)

// statsExporter is the subset of servenv.Exporter that PublishStats needs.
// Declaring it here (rather than importing the concrete Exporter) keeps the
// loadshed package free of a servenv dependency and lets tests pass a
// throwaway exporter.
type statsExporter interface {
	NewGaugeFunc(name, help string, f func() int64) *stats.GaugeFunc
	NewCounterFunc(name, help string, f func() int64) *stats.CounterFunc
}

// PublishStats registers one GaugeFunc per SnakeStats field, each name prefixed
// with prefix (e.g. "SnakeOltpRead" or "SnakeDml"). Call this once per Snake
// instance from engine init — never from NewSnake, which is also exercised by
// tests and the benchmark harness where duplicate registration would panic.
//
// Each Snake gets its own prefixed metric names rather than a shared "pool"
// label: both the oltp-read and dml snakes register through the same tablet
// Exporter, whose single label dimension is already the tablet name, so a
// shared labeled gauge would collide on that one key.
func PublishStats(exporter statsExporter, prefix string, s *Snake) {
	exporter.NewGaugeFunc(prefix+"QueueLen", "Snake CoDel queue length (waiters)", func() int64 {
		return int64(s.Stats().QueueLen)
	})
	exporter.NewGaugeFunc(prefix+"DroppableLen", "Snake CoDel droppable queue length", func() int64 {
		return int64(s.Stats().DroppableLen)
	})
	exporter.NewGaugeFunc(prefix+"HolderCount", "Snake current slot holders", func() int64 {
		return int64(s.Stats().HolderCount)
	})
	exporter.NewGaugeFunc(prefix+"Dropping", "Whether Snake CoDel is in the dropping state (1) or not (0)", func() int64 {
		if s.Stats().Dropping {
			return 1
		}
		return 0
	})
	exporter.NewGaugeFunc(prefix+"DropCount", "Snake CoDel drop count (control law state)", func() int64 {
		return int64(s.Stats().DropCount)
	})
	exporter.NewGaugeFunc(prefix+"CurrentIntervalNs", "Snake CoDel current control interval in nanoseconds", func() int64 {
		return s.Stats().CurrentInterval
	})
	exporter.NewCounterFunc(prefix+"ShedCount", "Cumulative requests shed by the Snake load shedder", func() int64 {
		return s.ShedCount()
	})
}
