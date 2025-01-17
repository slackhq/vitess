/*
 *
 * Copyright 2017 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Sticky random is a derivative based on the round_robin balancer which uses a Context
// variable to maintain client-side affinity to a given connection.

package vtgateproxy

import (
	"math/rand/v2"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
)

type ConnIdKey string

const CONN_ID_KEY = ConnIdKey("ConnId")

const Name = "sticky_random"

var logger = grpclog.Component("sticky_random")

// newBuilder creates a new roundrobin balancer builder.
func newStickyRandomBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Name, &stickyPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newStickyRandomBuilder())
}

type stickyPickerBuilder struct{}

func (*stickyPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	logger.Infof("stickyRandomPicker: Build called with info: %v", info)
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	scs := make([]balancer.SubConn, 0, len(info.ReadySCs))
	for sc := range info.ReadySCs {
		scs = append(scs, sc)
	}
	return &stickyPicker{
		subConns: scs,
	}
}

type stickyPicker struct {
	// subConns is the snapshot of the  balancer when this picker was
	// created. The slice is immutable.
	subConns []balancer.SubConn
}

func (p *stickyPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {

	subConnsLen := len(p.subConns)

	connId := info.Ctx.Value(CONN_ID_KEY).(int)
	if connId == 0 {
		connId = rand.IntN(subConnsLen) // shouldn't happen
	}

	sc := p.subConns[connId%subConnsLen]
	return balancer.PickResult{SubConn: sc}, nil
}
