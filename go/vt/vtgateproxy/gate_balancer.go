package vtgateproxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"
)

// Name is the name of az affinity balancer.
const Name = "slack_affinity_balancer"
const MetadataAZKey = "grpc-slack-az-metadata"
const MetadataGateTypeKey = "grpc-slack-gate-type-metadata"

var logger = grpclog.Component("slack_affinity_balancer")

func WithSlackAZAffinityContext(ctx context.Context, azID string, gateType string) context.Context {
	ctx = metadata.AppendToOutgoingContext(ctx, MetadataAZKey, azID, MetadataGateTypeKey, gateType)
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
	subConnsByAZ := map[string][]balancer.SubConn{}

	for sc := range info.ReadySCs {
		subConnInfo, _ := info.ReadySCs[sc]
		az := subConnInfo.Address.BalancerAttributes.Value(discoverySlackAZ{}).(string)

		allSubConns = append(allSubConns, sc)
		subConnsByAZ[az] = append(subConnsByAZ[az], sc)
	}
	return &slackAZAffinityPicker{
		allSubConns:  allSubConns,
		subConnsByAZ: subConnsByAZ,
	}
}

type slackAZAffinityPicker struct {
	// allSubConns is all subconns that were in the ready state when the picker was created
	allSubConns  []balancer.SubConn
	subConnsByAZ map[string][]balancer.SubConn
	nextByAZ     sync.Map
	next         uint32
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
	fmt.Printf("Headers: %v %v\n", hdrs, info)
	keys := hdrs.Get(MetadataAZKey)
	if len(keys) < 1 {
		fmt.Printf("uh oh - missing keys: %v %v %v\n", keys, hdrs, info.Ctx)
		fmt.Printf("no header - pick from anywhere\n")
		return p.pickFromSubconns(p.allSubConns, atomic.AddUint32(&p.next, 1))
	}
	az := keys[0]

	if az == "" {
		fmt.Printf("Header unset, pick from anywhere\n")
		return p.pickFromSubconns(p.allSubConns, atomic.AddUint32(&p.next, 1))
	}

	fmt.Printf("Selecting from az: %v\n", az)
	subConns := p.subConnsByAZ[az]
	if len(subConns) == 0 {
		fmt.Printf("No subconns in az and gate type, pick from anywhere\n")
		return p.pickFromSubconns(p.allSubConns, atomic.AddUint32(&p.next, 1))
	}
	val, _ := p.nextByAZ.LoadOrStore(az, new(uint32))
	ptr := val.(*uint32)
	atomic.AddUint32(ptr, 1)

	if len(subConns) >= 2 {
		fmt.Printf("Limiting to first 2\n")
		return p.pickFromSubconns(subConns[0:2], *ptr)
	} else {
		return p.pickFromSubconns(subConns, *ptr)
	}
}
