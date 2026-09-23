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
	const (
		capacity     = 2
		parallelism  = 8
		opsPerWorker = 1000
	)

	connect := func(ctx context.Context) (*StressConn, error) {
		return &StressConn{}, nil
	}

	tests := []struct {
		name       string
		poolConfig PoolConfig
	}{
		{name: "legacy"},
		{name: "snake", poolConfig: newMutableTestPoolConfig(loadshed.ModeShadow)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := NewPool[*StressConn](&Config[*StressConn]{
				Capacity:   capacity,
				PoolName:   "ConnPool",
				PoolConfig: tt.poolConfig,
			}).Open(connect, nil)
			t.Cleanup(pool.Close)

			completed := make([]atomic.Int64, parallelism)
			var wg errgroup.Group
			for p := range parallelism {
				tid := int32(p + 1)
				wg.Go(func() error {
					for range opsPerWorker {
						conn, err := pool.get(t.Context(), "", loadshed.PriorityUndroppable)
						if err != nil {
							return err
						}

						previousOwner := conn.Conn.owner.Swap(tid)
						if previousOwner != 0 {
							return fmt.Errorf("owner race: %d with %d", tid, previousOwner)
						}
						runtime.Gosched()
						previousOwner = conn.Conn.owner.Swap(0)
						if previousOwner != tid {
							return fmt.Errorf("owner race: %d with %d", previousOwner, tid)
						}
						conn.Recycle()
						completed[p].Add(1)
					}
					return nil
				})
			}

			require.NoError(t, wg.Wait())
			for _, count := range completed {
				assert.Equal(t, int64(opsPerWorker), count.Load())
			}
		})
	}
}
