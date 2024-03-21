package vtgateproxy

import (
	"errors"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
)

// Name is the name of az affinity balancer.
const Name = "slack_affinity_balancer"
const MetadataHostAffinityCount = "grpc-slack-num-connections-metadata"
const MetadataDiscoveryFilterPrefix = "grpc_discovery_filter_"

var logger = grpclog.Component("slack_affinity_balancer")

func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Name, &slackAZAffinityBalancer{}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newBuilder())
}

type slackAZAffinityBalancer struct{}

func (*slackAZAffinityBalancer) Build(info base.PickerBuildInfo) balancer.Picker {
	logger.Infof("slackAZAffinityBalancer: Build called with info: %v", info)
	fmt.Printf("Rebuilding picker\n")

	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	allSubConns := []balancer.SubConn{}
	subConnsByFiltered := []balancer.SubConn{}

	for sc := range info.ReadySCs {
		subConnInfo := info.ReadySCs[sc]
		matchesFilter := subConnInfo.Address.BalancerAttributes.Value(matchesFilter{}).(string)

		allSubConns = append(allSubConns, sc)
		if matchesFilter == "match" {
			subConnsByFiltered = append(subConnsByFiltered, sc)
		}
	}

	fmt.Printf("Filtered subcons: %v\n", len(subConnsByFiltered))
	fmt.Printf("All subcons: %v\n", len(allSubConns))

	return &slackAZAffinityPicker{
		allSubConns:      allSubConns,
		filteredSubConns: subConnsByFiltered,
	}
}

type slackAZAffinityPicker struct {
	// allSubConns is all subconns that were in the ready state when the picker was created
	allSubConns      []balancer.SubConn
	filteredSubConns []balancer.SubConn
	next             uint32
}

// Pick the next in the list from the list of subconns (RR)
func (p *slackAZAffinityPicker) pickFromSubconns(scList []balancer.SubConn, nextIndex uint32) (balancer.PickResult, error) {
	subConnsLen := uint32(len(scList))

	if subConnsLen == 0 {
		return balancer.PickResult{}, errors.New("no hosts in list")
	}

	sc := scList[nextIndex%subConnsLen]
	fmt.Printf("Select offset: %v %v %v %v\n", nextIndex, nextIndex%subConnsLen, len(scList), sc)

	return balancer.PickResult{SubConn: sc}, nil
}

func (p *slackAZAffinityPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	filteredSubConns := p.filteredSubConns
	numConnections := *numConnectionsInt
	if len(filteredSubConns) == 0 {
		fmt.Printf("No subconns in the filtered list, pick from anywhere in pool\n")
		return p.pickFromSubconns(p.allSubConns, atomic.AddUint32(&p.next, 1))
	}

	if len(filteredSubConns) >= numConnections && numConnections > 0 {
		fmt.Printf("Limiting to first %v\n", numConnections)
		return p.pickFromSubconns(filteredSubConns[0:numConnections], atomic.AddUint32(&p.next, 1))
	} else {
		return p.pickFromSubconns(filteredSubConns, atomic.AddUint32(&p.next, 1))
	}
}
