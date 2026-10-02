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
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const priorityTestTargetNs = 100

// newPriorityTestSnake builds an enabled Snake whose drop floor is high enough
// that nothing is shed, isolating dequeue selection.
func newPriorityTestSnake(maxSkips int) (*Snake[string], *testClock) {
	clock := newTestClock()
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeEnabled }
	cfg.CoDel.TargetNs = func() int64 { return priorityTestTargetNs }
	cfg.CoDel.IntervalNs = func() int64 { return 1000 }
	cfg.CoDel.KeepDroppableFloor = func() int { return 1 << 30 }
	cfg.CoDel.PriorityDequeueMaxSkips = func() int { return maxSkips }
	snake := NewSnake[string](cfg)
	snake.clockFunc = clock.nowFunc
	snake.q.nowNs = clock.nowFunc
	return snake, clock
}

func dequeueValue(t *testing.T, snake *Snake[string]) string {
	t.Helper()
	value, ok, dropped := snake.Dequeue()
	require.True(t, ok)
	require.Empty(t, dropped)
	return value
}

func TestPriorityDequeue_PicksMoreImportantOverTarget(t *testing.T) {
	snake, clock := newPriorityTestSnake(8)
	snake.Enqueue("low", 50)
	clock.advance(10)
	snake.Enqueue("high", 1)
	clock.advance(200)

	assert.Equal(t, "high", dequeueValue(t, snake))
	assert.Equal(t, "low", dequeueValue(t, snake))
	assert.Equal(t, int64(1), snake.priorityDequeueReordered.Load())
}

func TestPriorityDequeue_IgnoresMoreImportantUnderTarget(t *testing.T) {
	snake, clock := newPriorityTestSnake(8)
	snake.Enqueue("low", 50)
	clock.advance(150)
	snake.Enqueue("high", 1)
	clock.advance(50)

	assert.Equal(t, "low", dequeueValue(t, snake))
}

func TestPriorityDequeue_HeadUnderTargetUnchanged(t *testing.T) {
	snake, clock := newPriorityTestSnake(8)
	snake.Enqueue("low", 50)
	clock.advance(10)
	snake.Enqueue("high", 1)
	snake.Enqueue("low-match", 50)
	clock.advance(10)

	value, ok, _ := snake.DequeueMatching(func(value string) bool {
		return strings.HasSuffix(value, "match")
	})
	require.True(t, ok)
	assert.Equal(t, "low-match", value, "settings match still picks anywhere when healthy")
	assert.Equal(t, "low", dequeueValue(t, snake))
	assert.Zero(t, snake.priorityDequeueReordered.Load())
}

func TestPriorityDequeue_UndroppableWins(t *testing.T) {
	snake, clock := newPriorityTestSnake(8)
	snake.Enqueue("low", 50)
	clock.advance(10)
	snake.EnqueueExisting("undroppable", PriorityUndroppable)
	clock.advance(10)
	snake.Enqueue("high", 1)
	clock.advance(200)

	assert.Equal(t, "undroppable", dequeueValue(t, snake))
}

func TestPriorityDequeue_MatchWithinWinningBucket(t *testing.T) {
	snake, clock := newPriorityTestSnake(8)
	snake.Enqueue("low-match", 50)
	clock.advance(10)
	snake.Enqueue("high", 1)
	clock.advance(10)
	snake.Enqueue("high-match", 1)
	clock.advance(200)

	var visited []string
	value, ok, _ := snake.DequeueMatching(func(value string) bool {
		visited = append(visited, value)
		return strings.HasSuffix(value, "match")
	})
	require.True(t, ok)
	assert.Equal(t, "high-match", value)
	assert.Equal(t, []string{"high", "high-match"}, visited)
}

func TestPriorityDequeue_ForcesHeadAfterMaxSkips(t *testing.T) {
	snake, clock := newPriorityTestSnake(2)
	snake.Enqueue("low", 50)
	for _, value := range []string{"h1", "h2", "h3"} {
		clock.advance(10)
		snake.Enqueue(value, 1)
	}
	clock.advance(200)

	var got []string
	for range 4 {
		got = append(got, dequeueValue(t, snake))
	}
	assert.Equal(t, []string{"h1", "h2", "low", "h3"}, got)
	assert.Equal(t, int64(2), snake.priorityDequeueReordered.Load())
	assert.Equal(t, int64(1), snake.priorityDequeueForcedHead.Load())
}

func TestPriorityDequeue_FIFOWhenOff(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxSkips int
		mode     Mode
	}{
		{name: "max skips zero", maxSkips: 0, mode: ModeEnabled},
		{name: "shadow mode", maxSkips: 8, mode: ModeShadow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snake, clock := newPriorityTestSnake(tc.maxSkips)
			snake.cfg.Mode = func() Mode { return tc.mode }
			snake.Enqueue("low", 50)
			clock.advance(10)
			snake.Enqueue("high", 1)
			clock.advance(200)

			assert.Equal(t, "low", dequeueValue(t, snake))
		})
	}
}

// TestPriorityDequeue_DroppingParityWithFIFO checks the core invariant: with no
// drops and only droppable requests, reordering within the over-target prefix
// yields the same CoDel health sequence as FIFO.
func TestPriorityDequeue_DroppingParityWithFIFO(t *testing.T) {
	for _, maxSkips := range []int{1, 8, 1 << 30} {
		var reordered int64
		for seed := range uint64(20) {
			fifo, fifoClock := newPriorityTestSnake(0)
			prio, prioClock := newPriorityTestSnake(maxSkips)
			rng := rand.New(rand.NewPCG(seed, uint64(maxSkips)))

			for step := range 2000 {
				switch op := rng.IntN(10); {
				case op < 4:
					priority := 1 + rng.IntN(100)
					fifo.Enqueue("", priority)
					prio.Enqueue("", priority)
				case op < 7:
					_, fifoOK, _ := fifo.Dequeue()
					_, prioOK, _ := prio.Dequeue()
					require.Equal(t, fifoOK, prioOK)
				default:
					ns := int64(rng.IntN(3 * priorityTestTargetNs))
					fifoClock.advance(ns)
					prioClock.advance(ns)
				}
				require.Equal(t, fifo.q.dropping, prio.q.dropping, "maxSkips=%d seed=%d step=%d", maxSkips, seed, step)
				require.Equal(t, fifo.q.count, prio.q.count, "maxSkips=%d seed=%d step=%d", maxSkips, seed, step)
			}
			reordered += prio.priorityDequeueReordered.Load()
		}
		assert.Positive(t, reordered, "maxSkips=%d never reordered", maxSkips)
	}
}

type overloadResult struct {
	acquired, shed map[int]int
}

func (r overloadResult) shedRate(priority int) float64 {
	return float64(r.shed[priority]) / float64(r.acquired[priority])
}

// runOverloadSim drives a Snake at 1.5x capacity in 10µs steps: C slots with a
// fixed service time, Bernoulli arrivals split between two priorities.
func runOverloadSim(maxSkips, floor int, highShare float64) overloadResult {
	const (
		stepNs     = 10_000
		steps      = 300_000
		slots      = 10
		serviceNs  = 1_000_000
		arrivalP   = 1.5 * slots * stepNs / serviceNs
		highPri    = 10
		lowPri     = 50
		targetNs   = 5_000_000
		intervalNs = 20 * targetNs
	)
	clock := newTestClock()
	cfg := defaultSnakeConfig()
	cfg.Mode = func() Mode { return ModeEnabled }
	cfg.CoDel.TargetNs = func() int64 { return targetNs }
	cfg.CoDel.IntervalNs = func() int64 { return intervalNs }
	cfg.CoDel.MinDropDelayNs = func() int64 { return stepNs }
	cfg.CoDel.KeepDroppableFloor = func() int { return floor }
	cfg.CoDel.PriorityDequeueMaxSkips = func() int { return maxSkips }
	snake := NewSnake[int](cfg)
	snake.clockFunc = clock.nowFunc
	snake.q.nowNs = clock.nowFunc

	res := overloadResult{acquired: map[int]int{}, shed: map[int]int{}}
	countShed := func(dropped []int) {
		for _, priority := range dropped {
			res.shed[priority]++
		}
	}
	rng := rand.New(rand.NewPCG(1, 2))
	var busyUntil [slots]int64
	for range steps {
		clock.advance(stepNs)
		if rng.Float64() < arrivalP {
			priority := lowPri
			if rng.Float64() < highShare {
				priority = highPri
			}
			res.acquired[priority]++
			_, dropped := snake.Enqueue(priority, priority)
			countShed(dropped)
		}
		if snake.dropTimerArmed && clock.now >= snake.dropTimerExpectedNs {
			countShed(snake.LockedDropTimerFired())
		}
		for i := range busyUntil {
			if busyUntil[i] > clock.now {
				continue
			}
			_, ok, dropped := snake.Dequeue()
			countShed(dropped)
			if ok {
				busyUntil[i] = clock.now + serviceNs
			}
		}
	}
	return res
}

func TestPriorityDequeue_OverloadShedsLessHighPriority(t *testing.T) {
	for _, highShare := range []float64{0.5, 0.6} {
		fifo := runOverloadSim(0, keepDroppableFloor, highShare)
		prio := runOverloadSim(8, keepDroppableFloor, highShare)
		prioNoFloor := runOverloadSim(8, 0, highShare)
		t.Logf("highShare=%.2f high shed: K=0 %.4f, K=8 %.4f, K=8/floor=0 %.4f; low shed: %.4f, %.4f, %.4f",
			highShare,
			fifo.shedRate(10), prio.shedRate(10), prioNoFloor.shedRate(10),
			fifo.shedRate(50), prio.shedRate(50), prioNoFloor.shedRate(50))
		assert.Less(t, prio.shedRate(10), fifo.shedRate(10), "highShare=%.2f", highShare)
	}
}

func BenchmarkSnakeDequeue(b *testing.B) {
	for _, state := range []struct {
		name   string
		stepNs int64
	}{
		{name: "healthy", stepNs: 0},
		{name: "over-target", stepNs: 2 * priorityTestTargetNs},
	} {
		for _, n := range []int{8, 32, 128, 1024} {
			for _, maxSkips := range []int{0, 8} {
				b.Run(fmt.Sprintf("%s/n=%d/K=%d", state.name, n, maxSkips), func(b *testing.B) {
					snake, clock := newPriorityTestSnake(maxSkips)
					rng := rand.New(rand.NewPCG(1, 2))
					priorities := make([]int, 1024)
					for i := range priorities {
						priorities[i] = 1 + rng.IntN(100)
					}
					for i := range n {
						snake.Enqueue("", priorities[i%len(priorities)])
						clock.advance(state.stepNs)
					}
					b.ResetTimer()
					for i := range b.N {
						snake.Enqueue("", priorities[i%len(priorities)])
						clock.advance(state.stepNs)
						snake.Dequeue()
					}
				})
			}
		}
	}
}
