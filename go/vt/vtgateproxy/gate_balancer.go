package vtgateproxy

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"
)

// Name is the name of az affinity balancer.
const Name = "slack_affinity_balancer"
const MetadataHostAffinityCount = "grpc-slack-num-connections-metadata"
const MetadataDiscoveryFilterPrefix = "grpc_discovery_filter_"

var logger = grpclog.Component("slack_affinity_balancer")

func WithSlackAZAffinityContext(ctx context.Context, numConnections string, filters metadata.MD) context.Context {
	metadata.NewOutgoingContext(ctx, filters)
	ctx = metadata.AppendToOutgoingContext(ctx, MetadataHostAffinityCount, numConnections)
	return ctx
}

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
		subConnInfo, _ := info.ReadySCs[sc]
		matchesFilter := subConnInfo.Address.BalancerAttributes.Value(matchesFilter{}).(bool)

		allSubConns = append(allSubConns, sc)
		if matchesFilter {
			subConnsByFiltered = append(subConnsByFiltered, sc)
		}

	}
	return &slackAZAffinityPicker{
		allSubConns:      allSubConns,
		filteredSubConns: subConnsByFiltered,
	}
}

type slackAZAffinityPicker struct {
	// allSubConns is all subconns that were in the ready state when the picker was created
	allSubConns      []balancer.SubConn
	filteredSubConns []balancer.SubConn
	nextByAZ         sync.Map
	next             uint32
}

// Pick the next in the list from the list of subconns (RR)
func (p *slackAZAffinityPicker) pickFromSubconns(scList []balancer.SubConn, nextIndex uint32) (balancer.PickResult, error) {
	subConnsLen := uint32(len(scList))

	if subConnsLen == 0 {
		return balancer.PickResult{}, errors.New("No hosts in list")
	}

	fmt.Printf("Select offset: %v %v %v\n", nextIndex, nextIndex%subConnsLen, len(scList))

	sc := scList[nextIndex%subConnsLen]
	return balancer.PickResult{SubConn: sc}, nil
}

func (p *slackAZAffinityPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	hdrs, _ := metadata.FromOutgoingContext(info.Ctx)
	numConnections := 0
	keys := hdrs.Get(MetadataAZKey)
	if len(keys) < 1 {
		return p.pickFromSubconns(p.allSubConns, atomic.AddUint32(&p.next, 1))
	}
	az := keys[0]

	filteredSubconns := p.allSubConns
	for k, v := range hdrs {
		if strings.HasPrefix(k, MetadataDiscoveryFilterPrefix) {
			filterName := strings.TrimPrefix(k, MetadataDiscoveryFilterPrefix)
			filterValue := v
		}
	}

	for _, s := range v {

	}

	if az == "" {
		return p.pickFromSubconns(p.allSubConns, atomic.AddUint32(&p.next, 1))
	}

	keys = hdrs.Get(MetadataHostAffinityCount)
	if len(keys) > 0 {
		if i, err := strconv.Atoi(keys[0]); err != nil {
			numConnections = i
		}
	}

	subConns := p.subConnsByAZ[az]
	if len(subConns) == 0 {
		fmt.Printf("No subconns in az and gate type, pick from anywhere\n")
		return p.pickFromSubconns(p.allSubConns, atomic.AddUint32(&p.next, 1))
	}
	val, _ := p.nextByAZ.LoadOrStore(az, new(uint32))
	ptr := val.(*uint32)
	atomic.AddUint32(ptr, 1)

	if len(subConns) >= numConnections && numConnections > 0 {
		fmt.Printf("Limiting to first %v\n", numConnections)
		return p.pickFromSubconns(subConns[0:numConnections], *ptr)
	} else {
		return p.pickFromSubconns(subConns, *ptr)
	}
}
