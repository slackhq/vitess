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

	"vitess.io/vitess/go/vt/log"
)

type ConnIdKey string

const CONN_ID_KEY = ConnIdKey("ConnId")

// newBuilder creates a new roundrobin balancer builder.
func newStickyRandomBuilder() balancer.Builder {
	return base.NewBalancerBuilder("sticky_random", &stickyPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	log.V(1).Infof("registering sticky_random balancer")
	balancer.Register(newStickyRandomBuilder())
}

type stickyPickerBuilder struct{}

// Would be nice if this were easier in golang
func boolValue(val interface{}) bool {
	switch val := val.(type) {
	case bool:
		return val
	}
	return false
}

func (*stickyPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	//	log.V(100).Infof("stickyRandomPicker: Build called with info: %v", info)
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	scs := make([]balancer.SubConn, 0, len(info.ReadySCs))

	// Where possible filter to only ready conns in the local zone, using the remote
	// zone only if there are no local conns available.
	for sc, scInfo := range info.ReadySCs {
		local := boolValue(scInfo.Address.Attributes.Value(ZoneLocalAttr))
		if local {
			scs = append(scs, sc)
		}
	}

	// Otherwise use all the ready conns regardless of locality
	if len(scs) == 0 {
		for sc := range info.ReadySCs {
			scs = append(scs, sc)
		}
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

	var connId int
	connIdVal := info.Ctx.Value(CONN_ID_KEY)
	if connIdVal != nil {
		connId = connIdVal.(int)
		log.V(100).Infof("stickyRandomPicker: using connId %d", connId)
	} else {
		log.V(100).Infof("stickyRandomPicker: nonexistent connId -- using random")
		connId = rand.IntN(subConnsLen) // shouldn't happen
	}

	// XXX/demmer might want to hash the connId rather than just mod
	sc := p.subConns[connId%subConnsLen]

	return balancer.PickResult{SubConn: sc}, nil
}
