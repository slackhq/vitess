/*
Copyright 2023 The Vitess Authors.

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
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

type StressConn struct {
	setting *Setting
	owner   atomic.Int32
	closed  atomic.Bool
}

func (b *StressConn) Expired(_ time.Duration) bool {
	return false
}

func (b *StressConn) IsSettingApplied() bool {
	return b.setting != nil
}

func (b *StressConn) IsSameSetting(setting string) bool {
	return b.setting != nil && b.setting.ApplyQuery() == setting
}

var _ Connection = (*StressConn)(nil)

func (b *StressConn) ApplySetting(ctx context.Context, setting *Setting) error {
	b.setting = setting
	return nil
}

func (b *StressConn) ResetSetting(ctx context.Context) error {
	b.setting = nil
	return nil
}

func (b *StressConn) Setting() *Setting {
	return b.setting
}

func (b *StressConn) IsClosed() bool {
	return b.closed.Load()
}

func (b *StressConn) Close() {
	b.closed.Store(true)
}

func TestStackRace(t *testing.T) {
	const Count = 64
	const Procs = 32

	var wg sync.WaitGroup
	var stack connStack[*StressConn]
	var done atomic.Bool

	for c := 0; c < Count; c++ {
		stack.Push(&Pooled[*StressConn]{Conn: &StressConn{}})
	}

	for i := 0; i < Procs; i++ {
		wg.Add(1)
		go func(tid int32) {
			defer wg.Done()
			for !done.Load() {
				if conn, ok := stack.Pop(); ok {
					previousOwner := conn.Conn.owner.Swap(tid)
					if previousOwner != 0 {
						panic(fmt.Errorf("owner race: %d with %d", tid, previousOwner))
					}
					runtime.Gosched()
					previousOwner = conn.Conn.owner.Swap(0)
					if previousOwner != tid {
						panic(fmt.Errorf("owner race: %d with %d", previousOwner, tid))
					}
					stack.Push(conn)
				}
			}
		}(int32(i + 1))
	}

	time.Sleep(5 * time.Second)
	done.Store(true)
	wg.Wait()

	for c := 0; c < Count; c++ {
		conn, ok := stack.Pop()
		require.NotNil(t, conn)
		require.True(t, ok)
	}
}

func TestStress(t *testing.T) {
	const parallelism = 8

	connect := func(ctx context.Context) (*StressConn, error) {
		return &StressConn{}, nil
	}

	tests := []struct {
		name         string
		capacity     int64
		opsPerWorker int64
		duration     time.Duration
		poolConfig   PoolConfig
		toggle       bool
	}{
		{name: "legacy", capacity: 2, opsPerWorker: 1000},
		{name: "snake", capacity: 2, opsPerWorker: 1000, poolConfig: newMutableTestPoolConfig(loadshed.ModeShadow)},
		{name: "stack", capacity: 64, duration: 5 * time.Second},
		{name: "toggling", capacity: 2, duration: 5 * time.Second, poolConfig: newMutableTestPoolConfig(loadshed.ModeOff), toggle: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := NewPool[*StressConn](&Config[*StressConn]{
				Capacity:   tt.capacity,
				PoolName:   "ConnPool",
				PoolConfig: tt.poolConfig,
			}).Open(connect, nil)
			t.Cleanup(pool.Close)

			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			wg, ctx := errgroup.WithContext(ctx)
			t.Cleanup(func() {
				cancel()
				_ = wg.Wait()
			})
			stopAt := time.Now().Add(tt.duration)
			completed := make([]atomic.Int64, parallelism)
			migrations := make(map[loadshed.Mode]int)
			if tt.toggle {
				config := tt.poolConfig.(*mutableTestPoolConfig)
				wg.Go(func() error {
					modes := []loadshed.Mode{loadshed.ModeOff, loadshed.ModeShadow, loadshed.ModeEnabled}
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					modeIndex := 0
					observed := false
					var before int64
					for {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-ticker.C:
							if !time.Now().Before(stopAt) {
								return nil
							}
							var progress int64
							for worker := range completed {
								progress += completed[worker].Load()
							}
							mode := modes[modeIndex%len(modes)]
							wl := &pool.wait
							wl.mu.Lock()
							queued := wl.list.Len() > 0 && wl.snake.Len() == 0
							if mode != loadshed.ModeOff {
								queued = wl.snake.Len() > 0 && wl.list.Len() == 0
							}
							if wl.activeMode == mode && queued {
								if !observed {
									before = progress
									observed = true
								} else if progress > before {
									if modeIndex > 0 {
										migrations[mode]++
									}
									modeIndex++
									config.setMode(modes[modeIndex%len(modes)])
									observed = false
								}
							}
							wl.mu.Unlock()
						}
					}
				})
			}
			for p := range parallelism {
				tid := int32(p + 1)
				wg.Go(func() error {
					for completed[p].Load() < tt.opsPerWorker || time.Now().Before(stopAt) {
						if err := ctx.Err(); err != nil {
							return err
						}
						conn, err := pool.get(ctx)
						if err != nil {
							if tt.toggle && errors.Is(err, ErrPoolLoadShed) {
								continue
							}
							return err
						}

						previousOwner := conn.Conn.owner.Swap(tid)
						if previousOwner != 0 {
							conn.Recycle()
							return fmt.Errorf("owner race: %d with %d", tid, previousOwner)
						}
						runtime.Gosched()
						previousOwner = conn.Conn.owner.Swap(0)
						if previousOwner != tid {
							conn.Recycle()
							return fmt.Errorf("owner race: %d with %d", previousOwner, tid)
						}
						conn.Recycle()
						completed[p].Add(1)
					}
					return nil
				})
			}

			require.NoError(t, wg.Wait())
			for i := range completed {
				if tt.duration == 0 {
					assert.Equal(t, tt.opsPerWorker, completed[i].Load(), "worker %d", i)
				} else {
					assert.Positive(t, completed[i].Load(), "worker %d", i)
				}
			}
			if tt.toggle {
				for _, mode := range []loadshed.Mode{loadshed.ModeOff, loadshed.ModeShadow, loadshed.ModeEnabled} {
					assert.Positive(t, migrations[mode], "queued migration and progress in mode %s", mode)
				}
			}
		})
	}
}
