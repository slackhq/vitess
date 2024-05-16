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
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
)

// Name is the name of first_ready balancer.
const Name = "first_ready"

var logger = grpclog.Component("firstready")

// newBuilder creates a new roundrobin balancer builder.
func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Name, &frPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newBuilder())
}

type frPickerBuilder struct{}

func (*frPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	logger.Infof("firstreadyPicker: Build called with info: %v", info)
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	var subConn balancer.SubConn
	for sc := range info.ReadySCs {
		subConn = sc
		break
	}
	return &frPicker{
		subConn: subConn,
	}
}

type frPicker struct {
	// subConn is the first ready subconn when this picker was
	// created. The slice is immutable. Each Get() will do a round robin
	// selection from it and return the selected SubConn.
	subConn balancer.SubConn
}

func (p *frPicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	return balancer.PickResult{SubConn: p.subConn}, nil
}
