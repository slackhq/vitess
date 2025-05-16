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
package vtgateproxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"

	svcdisco "slack-github.com/slack/svcdisco-client-go/service_discovery"
	"slack-github.com/slack/svcdisco-client-go/source"
)

// Discovery for vtgate grpc endpoints.
//
// We support two ways of getting endpoint discovery information
//  - reading from a JSON file, rendered by consul-template from Rotor
//  - directly from the local Rotor frontend

const PoolTypeAttr = "PoolType"
const ZoneLocalAttr = "ZoneLocal"

// Resolver(https://godoc.org/google.golang.org/grpc/resolver#Resolver).
type GateResolver struct {
	target     resolver.Target
	clientConn resolver.ClientConn
	poolType   string

	currentAddrs []resolver.Address
	mu           sync.Mutex
}

func (r *GateResolver) ResolveNow(o resolver.ResolveNowOptions) {}

func (r *GateResolver) Close() {
	log.Infof("Closing resolver for target %s", r.target.URL.String())
}

type GateResolverBuilder struct {
	scheme         string
	affinityValue  string
	poolTypeField  string
	numConnections int
	numBackupConns int

	mu        sync.RWMutex
	targets   map[string][]targetHost
	resolvers []*GateResolver

	sorter *shuffleSorter

	discovery GateDiscovery
}

type targetHost struct {
	Hostname string
	Addr     string
	PoolType string
	Affinity string
}

type GateDiscovery interface {

	// discover returns a set of candidate vtgates synchronously
	discover() (map[string][]targetHost, error)

	// discoverAsync passes candidate vtgate to the given callback whenever there is a change, and
	// continues to do so forever.
	// The callback should not be called multiple times concurrently by discoverAsync.
	discoverAsync(func(map[string][]targetHost)) error
}

// File based discovery for vtgate grpc endpoints
//
// This loads the list of hosts from json and watches for changes to the list of hosts. It will select N connection to maintain to backend vtgates.
// Connections will rebalance every 5 minutes
//
// # Example json config - based on the slack hosts format
//
// [
//
//	{
//	    "address": "10.4.56.194",
//	    "az_id": "use1-az1",
//	    "port": 15999,
//	    "type": "aux"
//	},
//
// URL scheme:
// vtgate://<type>?az_id=<string>
//
// num_connections: Option number of hosts to open connections to for round-robin selection
// az_id: Filter to just hosts in this az (optional)
// type: Only select from hosts of this type (required)
type JSONGateDiscovery struct {
	jsonPath      string
	addressField  string
	portField     string
	poolTypeField string
	affinityField string

	ticker   *time.Ticker
	checksum []byte
}

// RotorGateDiscovery watches service discovery information via Rotor
type RotorGateDiscovery struct {
	service string
}

var (
	parseCount  = stats.NewCountersWithSingleLabel("JsonDiscoveryParseCount", "Count of results of JSON host file parsing (changed, unchanged, error)", "result")
	targetCount = stats.NewGaugesWithSingleLabel("JsonDiscoveryTargetCount", "Count of hosts returned from discovery by pool type", "pool")

	discoveryCount = stats.NewGaugesWithMultiLabels("DiscoveryTargetCount", "Count of hosts returned from discovery by pool type and discovery scheme", []string{"pool", "scheme"})
	selectedCount  = stats.NewGaugesWithMultiLabels("SelectedCount", "Count of hosts passed to GRPC balancer by pool type, scheme and locality", []string{"pool", "scheme", "locality"})
)

func RegisterRotorGateResolver(scheme, service, poolTypeField, affinityValue string, numConnections int,
	numBackupConns int) (*GateResolverBuilder, error) {
	resolverBuilder := &GateResolverBuilder{
		scheme:         scheme,
		affinityValue:  affinityValue,
		numConnections: numConnections,
		numBackupConns: numBackupConns,
		sorter:         newShuffleSorter(),
		poolTypeField:  poolTypeField,
		discovery:      RotorGateDiscovery{service: service},
	}

	resolver.Register(resolverBuilder)

	log.Infof("Registered Rotor discovery scheme %v to watch: %v\n", resolverBuilder.Scheme(), service)
	if err := resolverBuilder.start(); err != nil {
		return nil, err
	}

	servenv.AddStatusPart("Rotor Discovery", targetsTemplate, resolverBuilder.debugTargets)

	return resolverBuilder, nil
}

func RegisterJSONGateResolver(
	scheme string,
	jsonPath string,
	addressField string,
	portField string,
	poolTypeField string,
	affinityField string,
	affinityValue string,
	numConnections int,
	numBackupConns int,
) (*GateResolverBuilder, error) {

	jsonDiscovery := &GateResolverBuilder{
		scheme:         scheme,
		targets:        map[string][]targetHost{},
		affinityValue:  affinityValue,
		numConnections: numConnections,
		numBackupConns: numBackupConns,
		sorter:         newShuffleSorter(),
		poolTypeField:  poolTypeField,
		discovery: JSONGateDiscovery{
			jsonPath:      jsonPath,
			addressField:  addressField,
			portField:     portField,
			poolTypeField: poolTypeField,
			affinityField: affinityField,
		},
	}

	resolver.Register(jsonDiscovery)
	log.Infof("Registered JSON discovery scheme %v to watch: %v\n", jsonDiscovery.Scheme(), jsonPath)

	err := jsonDiscovery.start()
	if err != nil {
		return nil, err
	}

	servenv.AddStatusPart("JSON Discovery", targetsTemplate, jsonDiscovery.debugTargets)

	return jsonDiscovery, nil
}

func (b *GateResolverBuilder) Scheme() string { return b.scheme }

func (b *GateResolverBuilder) targetsChanged(allTargets map[string][]targetHost) {
	b.selectTargets(allTargets)

	var wg sync.WaitGroup

	// notify all the resolvers that the targets changed in parallel, since each update might sleep for
	// the warmup time
	b.mu.RLock()
	for _, r := range b.resolvers {
		wg.Add(1)
		go func(r *GateResolver) {
			defer wg.Done()

			err := b.update(r)
			if err != nil {
				log.Errorf("Failed to update resolver: %v", err)
			}
		}(r)
	}
	b.mu.RUnlock()
	wg.Wait()
}

// Parse and validate the format of the file and start watching for changes
func (b *GateResolverBuilder) start() error {
	allTargets, err := b.discovery.discover()

	if err != nil {
		return err
	}

	b.selectTargets(allTargets)

	// Validate some stats
	if len(b.targets) == 0 {
		return fmt.Errorf("no valid targets")
	}

	// Log some stats on startup
	poolTypes := map[string]int{}
	affinityTypes := map[string]int{}

	for _, ts := range b.targets {
		for _, t := range ts {
			count := poolTypes[t.PoolType]
			poolTypes[t.PoolType] = count + 1

			count = affinityTypes[t.Affinity]
			affinityTypes[t.Affinity] = count + 1
		}
	}

	parseCount.Add("changed", 1)

	log.Infof("[%s] loaded targets, pool types %v, affinity %s, groups %v", b.Scheme(), poolTypes, *affinityValue, affinityTypes)

	return b.discovery.discoverAsync(func(allTargets map[string][]targetHost) {
		b.targetsChanged(allTargets)
	})
}

func (b *GateResolverBuilder) selectTargets(allTargets map[string][]targetHost) {

	// If a pool disappears, the metric will not record this unless all counts
	// are reset each time the file is parsed. If this ends up causing problems
	// with the metric briefly dropping to 0, it could be done by rlocking the
	// target lock and then comparing the previous targets with the current
	// targets and only resetting pools which disappear.
	targetCount.ResetAll()

	b.mu.RLock()
	for pool := range b.targets {
		discoveryCount.Reset([]string{pool, b.scheme})
		selectedCount.Reset([]string{pool, b.scheme, "local"})
		selectedCount.Reset([]string{pool, b.scheme, "remote"})
	}
	b.mu.RUnlock()

	var selected = map[string][]targetHost{}

	for poolType := range allTargets {
		discoveryCount.Set([]string{poolType, b.scheme}, int64(len(allTargets[poolType])))

		b.sorter.shuffleSort(allTargets[poolType], b.affinityValue)

		// try to pick numConnections from the front of the list (local zone) and numBackupConnections
		// from the tail (remote zone). if that's not possible, just take the whole set
		if len(allTargets[poolType]) >= b.numConnections+b.numBackupConns {
			remoteOffset := len(allTargets[poolType]) - b.numBackupConns
			selected[poolType] = append(allTargets[poolType][:b.numConnections], allTargets[poolType][remoteOffset:]...)
		} else {
			selected[poolType] = allTargets[poolType]
		}

		targetCount.Set(poolType, int64(len(selected[poolType])))

		var localCount, remoteCount int64
		for _, t := range selected[poolType] {
			if t.Affinity == b.affinityValue {
				localCount += 1
			} else {
				remoteCount += 1
			}
		}

		selectedCount.Set([]string{poolType, b.scheme, "local"}, localCount)
		selectedCount.Set([]string{poolType, b.scheme, "remote"}, remoteCount)
	}

	b.mu.Lock()
	b.targets = selected
	b.mu.Unlock()
}

func (b *GateResolverBuilder) GetPools() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var pools []string
	for pool := range b.targets {
		pools = append(pools, pool)
	}
	sort.Strings(pools)
	return pools
}

func (b *GateResolverBuilder) getTargets(poolType string) []targetHost {
	// Copy the target slice
	b.mu.RLock()
	targets := []targetHost{}
	targets = append(targets, b.targets[poolType]...)
	b.mu.RUnlock()

	b.sorter.shuffleSort(targets, b.affinityValue)

	return targets
}

type shuffleSorter struct {
	rand *rand.Rand
	mu   *sync.Mutex
}

func newShuffleSorter() *shuffleSorter {
	return &shuffleSorter{
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
		mu:   &sync.Mutex{},
	}
}

// shuffleSort shuffles a slice of targetHost to ensure every host has a
// different order to iterate through, putting the affinity matching (e.g. same
// az) hosts at the front and the non-matching ones at the end.
func (s *shuffleSorter) shuffleSort(targets []targetHost, targetAffinity string) {
	n := len(targets)
	head := 0
	// Only need to do n-1 swaps since the last host is always in the right place.
	tail := n - 1
	for i := 0; i < n-1; i++ {
		s.mu.Lock()
		j := head + s.rand.Intn(tail-head+1)
		s.mu.Unlock()

		if targets[j].Affinity == targetAffinity {
			targets[head], targets[j] = targets[j], targets[head]
			head++
		} else {
			targets[tail], targets[j] = targets[j], targets[tail]
			tail--
		}
	}
}

// Update the current list of hosts for the given resolver
func (b *GateResolverBuilder) update(r *GateResolver) error {
	log.V(100).Infof("resolving target %s to %d connections\n", r.target.URL.String(), *numConnections)

	targets := b.getTargets(r.poolType)

	// There should only ever be a single goroutine calling update on a given Resolver,
	// but add a lock just in case to ensure that the r.currentAddrs are in fact synchronized
	r.mu.Lock()
	defer r.mu.Unlock()

	var addrs []resolver.Address
	for _, target := range targets {
		attrs := attributes.New(PoolTypeAttr, r.poolType).WithValue(ZoneLocalAttr, target.Affinity == b.affinityValue)
		addrs = append(addrs, resolver.Address{Addr: target.Addr, Attributes: attrs})
	}

	// If we've already selected some targets, give the new addresses some time to warm up before removing
	// the old ones from the list
	if r.currentAddrs != nil && warmupTime.Seconds() > 0 {
		combined := append(r.currentAddrs, addrs...)
		log.V(100).Infof("updating targets for %s to warmup %v", r.target.URL.String(), targets)
		_ = r.clientConn.UpdateState(resolver.State{Addresses: combined})
		time.Sleep(*warmupTime)
	}

	log.V(100).Infof("updating targets for %s after warmup to %v", r.target.URL.String(), targets)
	r.currentAddrs = addrs
	return r.clientConn.UpdateState(resolver.State{Addresses: addrs})
}

// Build a new Resolver to route to the given target
func (b *GateResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	attrs := target.URL.Query()
	log.Infof("[%s] building resolver for %+v", b.Scheme(), target.URL)

	// If the config specifies a pool type attribute, then the caller must supply it in the connection
	// attributes, otherwise reject the request.
	poolType := ""
	if b.poolTypeField != "" {
		poolType = attrs.Get(b.poolTypeField)
		if poolType == "" {
			return nil, fmt.Errorf("pool type attribute %s not in target", b.poolTypeField)
		}
	}

	log.V(100).Infof("Start discovery for target %v poolType %s affinity %s\n", target.URL.String(), poolType, b.affinityValue)

	r := &GateResolver{
		target:     target,
		clientConn: cc,
		poolType:   poolType,
	}

	err := b.update(r)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.resolvers = append(b.resolvers, r)
	b.mu.Unlock()

	return r, nil
}

// debugTargets will return the builder's targets with a sorted slice of
// poolTypes for rendering debug output
func (b *GateResolverBuilder) debugTargets() any {
	pools := b.GetPools()
	targets := map[string][]targetHost{}
	for pool := range b.targets {
		targets[pool] = b.getTargets(pool)
	}
	return struct {
		Pools   []string
		Targets map[string][]targetHost
	}{
		Pools:   pools,
		Targets: targets,
	}
}

func (b RotorGateDiscovery) discover() (map[string][]targetHost, error) {
	client, err := svcdisco.NewSvcDiscovery()
	if err != nil {
		return nil, err
	}
	endpoints, err := client.FetchResources(context.Background(), []string{b.service}) //[]string{"vitess-vtgate@dev-us-east-1-vitess1"})
	if err != nil {
		return nil, err
	}

	return b.targetsFromEndpoints(endpoints), nil
}

func (b RotorGateDiscovery) targetsFromEndpoints(eps map[string][]*source.Endpoint) map[string][]targetHost {
	targets := map[string][]targetHost{}
	for _, eps := range eps {
		for _, ep := range eps {
			target := targetHost{}

			port := ""

			for tag := range ep.Tags {
				if strings.HasPrefix(tag, "type-") {
					target.PoolType, _ = strings.CutPrefix(tag, "type-")
				} else if strings.HasPrefix(tag, "az_id:") {
					target.Affinity, _ = strings.CutPrefix(tag, "az_id:")
				} else if strings.HasPrefix(tag, "pod:") {
					target.Hostname, _ = strings.CutPrefix(tag, "pod:")
				} else if strings.HasPrefix(tag, "grpc:") {
					port, _ = strings.CutPrefix(tag, "grpc:")
				}
			}

			target.Addr = fmt.Sprintf("%s:%s", ep.Address.Address, port)
			///			log.Infof("Rotor: Found address with: %s", target.Addr)

			if target.PoolType == "" || target.Affinity == "" || target.Hostname == "" || port == "" {
				log.Errorf("Malformed target from Rotor: %+v", ep)
				continue
			}

			if pool, exists := targets[target.PoolType]; exists {
				targets[target.PoolType] = append(pool, target)
			} else {
				targets[target.PoolType] = []targetHost{target}
			}
		}
	}

	return targets
}

func (b RotorGateDiscovery) discoverAsync(cb func(map[string][]targetHost)) error {
	client, err := svcdisco.NewSvcDiscovery()
	if err != nil {
		return err
	}

	go func() {
		err := client.WatchResourcesUntilCancel(context.Background(), b.service, func(eps map[string][]*source.Endpoint) {
			targets := b.targetsFromEndpoints(eps)
			cb(targets)
		})
		if err != nil {
			log.Errorf("Error watching resources in Rotor: %v", err)
			return
		}
	}()

	return nil
}

func (b JSONGateDiscovery) discover() (map[string][]targetHost, error) {
	allTargets, err := b.parseJSON()
	if err != nil {
		parseCount.Add("error", 1)
		if err == nil {
			log.Error(err)
		}
	}

	return allTargets, nil
}

// Checks every second if the JSON file has changed on disk, and reloads if so.
func (b JSONGateDiscovery) discoverAsync(cb func(map[string][]targetHost)) error {
	// Start a config watcher
	b.ticker = time.NewTicker(1 * time.Second)
	fileStat, err := os.Stat(b.jsonPath)
	if err != nil {
		return err
	}

	go func() {
		var parseErr error
		for range b.ticker.C {

			checkFileStat, err := os.Stat(b.jsonPath)
			if err != nil {
				log.Errorf("Error stat'ing config %v\n", err)
				parseCount.Add("error", 1)
				continue
			}
			isUnchanged := checkFileStat.Size() == fileStat.Size() && checkFileStat.ModTime() == fileStat.ModTime()
			if isUnchanged {
				// no change
				continue
			}

			fileStat = checkFileStat

			allTargets, err := b.parseJSON()
			if err != nil {
				parseCount.Add("error", 1)
				if parseErr == nil || err.Error() != parseErr.Error() {
					parseErr = err
					log.Error(err)
				}
				continue
			}

			parseErr = nil
			if allTargets == nil {
				parseCount.Add("unchanged", 1)
				continue
			}
			parseCount.Add("changed", 1)

			cb(allTargets)
		}
	}()

	return nil
}

// parseJSON the file and build the target host list, returning whether or not the list was
// updated since the last parseJSON, or if the checksum matched
func (b *JSONGateDiscovery) parseJSON() (map[string][]targetHost, error) {
	data, err := os.ReadFile(b.jsonPath)
	if err != nil {
		return nil, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	sum := h.Sum(nil)

	if bytes.Equal(sum, b.checksum) {
		log.V(100).Infof("file did not change (checksum %x), skipping re-parse", sum)
		return nil, nil
	}
	b.checksum = sum
	log.V(100).Infof("detected file change (checksum %x), parsing", sum)

	hosts := []map[string]interface{}{}
	err = json.Unmarshal(data, &hosts)
	if err != nil {
		return nil, fmt.Errorf("error parsing JSON discovery file %s: %v", b.jsonPath, err)
	}

	var allTargets = map[string][]targetHost{}
	for _, host := range hosts {
		hostname, hasHostname := host["host"]
		address, hasAddress := host[b.addressField]
		port, hasPort := host[b.portField]
		poolType, hasPoolType := host[b.poolTypeField]
		affinity, hasAffinity := host[b.affinityField]

		if !hasAddress {
			return nil, fmt.Errorf("error parsing JSON discovery file %s: address field %s not present", b.jsonPath, b.addressField)
		}

		if !hasPort {
			return nil, fmt.Errorf("error parsing JSON discovery file %s: port field %s not present", b.jsonPath, b.portField)
		}

		if !hasHostname {
			hostname = address
		}

		if b.poolTypeField != "" && !hasPoolType {
			return nil, fmt.Errorf("error parsing JSON discovery file %s: pool type field %s not present", b.jsonPath, b.poolTypeField)
		}

		if b.affinityField != "" && !hasAffinity {
			return nil, fmt.Errorf("error parsing JSON discovery file %s: affinity field %s not present", b.jsonPath, b.affinityField)
		}

		if b.poolTypeField == "" {
			poolType = ""
		}

		if b.affinityField == "" {
			affinity = ""
		}

		// Handle both int and string values for port
		switch port.(type) {
		case int:
			port = fmt.Sprintf("%d", port)
		case string:
			// nothing to do
		default:
			return nil, fmt.Errorf("error parsing JSON discovery file %s: port field %s has invalid value %v", b.jsonPath, b.portField, port)
		}
		//		log.Infof("JSON: Found host with address %s", fmt.Sprintf("%s:%s", address, port))
		target := targetHost{hostname.(string), fmt.Sprintf("%s:%s", address, port), poolType.(string), affinity.(string)}
		allTargets[target.PoolType] = append(allTargets[target.PoolType], target)
	}

	return allTargets, nil
}

const (
	// targetsTemplate is a HTML template to display the gate resolver's target hosts.
	targetsTemplate = `
<style>
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid #999;
    padding: 0.2rem;
  }
</style>
<table>
{{range $i, $p := .Pools}}  <tr>
    <th colspan="4">{{$p}}</th>
  </tr>
{{range index $.Targets $p}}  <tr>
	<td>{{.Hostname}}</td>
	<td>{{.Addr}}</td>
    <td>{{.Affinity}}</td>

  </tr>{{end}}
{{end}}
</table>
`
)
