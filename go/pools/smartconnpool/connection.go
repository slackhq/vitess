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
	"sync/atomic"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/loadshed"
)

type Connection interface {
	ApplySetting(ctx context.Context, setting *Setting) error
	ResetSetting(ctx context.Context) error
	Setting() *Setting

	IsClosed() bool
	Close()
}

type Pooled[C Connection] struct {
	next        atomic.Pointer[Pooled[C]]
	timeCreated timestamp
	timeUsed    timestamp
	pool        *ConnPool[C]
	snakeUnlock *loadshed.SafeUnlock

	Conn C
}

func NewUnpooled[C Connection](conn C) *Pooled[C] {
	return &Pooled[C]{Conn: conn}
}

func (dbc *Pooled[C]) SetSnakeUnlock(unlock *loadshed.SafeUnlock) {
	dbc.snakeUnlock = unlock
}

func (dbc *Pooled[C]) releaseSnakeUnlock() {
	if dbc.snakeUnlock != nil {
		_ = dbc.snakeUnlock.Release()
		dbc.snakeUnlock = nil
	}
}

func (dbc *Pooled[C]) Close() {
	dbc.releaseSnakeUnlock()
	dbc.Conn.Close()
}

func (dbc *Pooled[C]) Recycle() {
	dbc.releaseSnakeUnlock()
	switch {
	case dbc.pool == nil:
		dbc.Conn.Close()
	case dbc.Conn.IsClosed():
		dbc.pool.put(nil)
	default:
		dbc.pool.put(dbc)
	}
}

func (dbc *Pooled[C]) Taint() {
	if dbc.pool == nil {
		return
	}
	dbc.releaseSnakeUnlock()
	dbc.pool.put(nil)
	dbc.pool = nil
}
