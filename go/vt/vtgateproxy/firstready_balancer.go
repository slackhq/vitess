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
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"

	"vitess.io/vitess/go/vt/log"
)

// newBuilder creates a new first_ready balancer builder.
func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder("first_ready", &frPickerBuilder{currentConns: map[string]balancer.SubConn{}}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newBuilder())
}

// frPickerBuilder implements both the Builder and the Picker interfaces.
//
// Once a conn is chosen and is in the ready state, it will remain as the
// active subconn even if other connections become available.
type frPickerBuilder struct {
	mu           sync.Mutex
	currentConns map[string]balancer.SubConn
}

func (f *frPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	log.V(100).Infof("first_ready: Build called with info: %v", info)

	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(errors.New("no available connections"))
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// First check if there is a previously chosen subconn in the ready list
	for sc, scInfo := range info.ReadySCs {
		// poolType is a required attribute to be set by discovery
		poolType := scInfo.Address.Attributes.Value(PoolTypeAttr).(string)
		currentConn := f.currentConns[poolType]
		if currentConn == sc {
			log.V(100).Infof("first_ready: conn for pool %s still active - keeping addr %s", poolType, scInfo.Address.Addr)
			return &frPicker{sc}
		}
	}

	// Otherwise either we don't have an active conn or the conn we were using is
	// no longer active, so pick an arbitrary new one out of the map.
	for sc, scInfo := range info.ReadySCs {
		poolType := scInfo.Address.Attributes.Value(PoolTypeAttr).(string)
		log.V(100).Infof("first_ready: conn for pool %s not active - picked %s", poolType, scInfo.Address.Addr)
		f.currentConns[poolType] = sc
		return &frPicker{sc}
	}

	return base.NewErrPicker(errors.New("error should not be reachable"))
}

// frPicker is a trivial picker that just wraps the one chosen conn
type frPicker struct {
	currentConn balancer.SubConn
}

// Pick simply returns the currently chosen conn
func (p *frPicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	return balancer.PickResult{SubConn: p.currentConn}, nil
}