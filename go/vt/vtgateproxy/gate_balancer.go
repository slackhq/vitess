package vtgateproxy

import (
	"errors"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

// Name is the name of az affinity balancer.
const Name = "slack_affinity_balancer"

func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Name, &pickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newBuilder())
}

type pickerBuilder struct{}

func (*pickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	allSubConns := []balancer.SubConn{}

	for sc := range info.ReadySCs {
		//subConnInfo := info.ReadySCs[sc]
		//matchesFilter := subConnInfo.Address.BalancerAttributes.Value(matchesFilter{}).(string)
		allSubConns = append(allSubConns, sc)
	}

	return &filteredAffinityPicker{
		subConns: allSubConns,
	}
}

type filteredAffinityPicker struct {
	// allSubConns is all subconns that were in the ready state when the picker was created
	subConns []balancer.SubConn
	next     uint32
}

// Pick the next in the list from the list of subconns (RR)
func (p *filteredAffinityPicker) pickFromSubconns(scList []balancer.SubConn, nextIndex uint32) (balancer.PickResult, error) {
	subConnsLen := uint32(len(scList))

	if subConnsLen == 0 {
		return balancer.PickResult{}, errors.New("no hosts in list")
	}

	sc := scList[nextIndex%subConnsLen]
	return balancer.PickResult{SubConn: sc}, nil
}

func (p *filteredAffinityPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	fmt.Printf("Picking: subcons counts: len(%v)\n", len(p.subConns))
	return p.pickFromSubconns(p.subConns, atomic.AddUint32(&p.next, 1))
}
