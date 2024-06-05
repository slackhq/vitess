package vtgateproxy

/*
Copyright 2024 The Vitess Authors.

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

// The firstready balancer implements the GRPC load balancer abstraction by
// routing all queries to the first available target in the list returned from
// discovery.
//
// Similar to the builtin "round_robin" balancer, the base functionality takes care
// of establishing subconns to all targets in the list and keeping them in the "ready"
// state, so all we have to do is pick the first available one from the set.
//
// This is in contrast to the `pick_first` balancer which only establishes the subconn
// to a single target at a time and is therefore subject to undesirable behaviors if,
// for example, the first host in the set is unreachable for some time, but not declared
// down.
// https://github.com/grpc/grpc-go/blob/master/pickfirst.go

import (
	"errors"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/vt/log"
)

// newBuilder creates a new first_ready balancer builder.
func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder("first_ready", &frPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newBuilder())
}

// frPickerBuilder implements both the Builder and the Picker interfaces.
//
// Once a conn is chosen and is in the ready state, it will remain as the
// active subconn even if other connections become available.
type frPickerBuilder struct {
}

type frConnWithAddr struct {
	subConn balancer.SubConn
	addr    resolver.Address
}

type frPicker struct {
	subConns []frConnWithAddr
	cur      uint32
}

func (f *frPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	log.V(100).Infof("first_ready: Build called with info: %v", info)

	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(errors.New("no available connections"))
	}

	subConns := make([]frConnWithAddr, len(info.ReadySCs))
	idx := 0
	for sc, k := range info.ReadySCs {
		subConns[idx] = frConnWithAddr{subConn: sc, addr: k.Address}
		idx++
	}

	return &frPicker{subConns: subConns, cur: 0}
}

// Pick simply returns the currently chosen conn
func (f *frPicker) Pick(pi balancer.PickInfo) (balancer.PickResult, error) {
	curIndex := atomic.LoadUint32(&f.cur)
	return balancer.PickResult{SubConn: f.subConns[curIndex].subConn, Done: func(info balancer.DoneInfo) {
		if info.Err != nil {
			// Only try to move the index at most 1 - if someone else raced and advanced it, do nothing.
			nextIndex := (curIndex + 1) % uint32(len(f.subConns))
			atomic.CompareAndSwapUint32(&f.cur, curIndex, nextIndex)
		}
	}}, nil
}
