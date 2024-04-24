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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"time"

	"google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
)

// File based discovery for vtgate grpc endpoints
//
// This loads the list of hosts from json and watches for changes to the list of hosts. It will select N connection to maintain to backend vtgates.
// Connections will rebalance every 5 minutes
//
// Example json config - based on the slack hosts format
//
// [
//     {
//         "address": "10.4.56.194",
//         "az_id": "use1-az1",
//         "port": 15999,
//         "type": "aux"
//     },
//
// URL scheme:
// vtgate://<type>?az_id=<string>
//
// num_connections: Option number of hosts to open connections to for round-robin selection
// az_id: Filter to just hosts in this az (optional)
// type: Only select from hosts of this type (required)
//

type JSONGateResolverBuilder struct {
	jsonPath      string
	addressField  string
	portField     string
	poolTypeField string
	affinityField string
	affinityValue string

	targets   map[string][]targetHost
	resolvers []*JSONGateResolver

	rand     *rand.Rand
	ticker   *time.Ticker
	checksum []byte
}

type targetHost struct {
	addr     string
	poolType string
	affinity string
}

// Resolver(https://godoc.org/google.golang.org/grpc/resolver#Resolver).
type JSONGateResolver struct {
	target     resolver.Target
	clientConn resolver.ClientConn
	poolType   string
	affinity   string
}

var (
	parseCount  = stats.NewCountersWithSingleLabel("JsonDiscoveryParseCount", "Count of results of JSON host file parsing (changed, unchanged, error)", "result")
	targetCount = stats.NewGaugesWithSingleLabel("JsonDiscoveryTargetCount", "Count of hosts returned from discovery by pool type", "pool")
)

func RegisterJSONGateResolver(
	jsonPath string,
	addressField string,
	portField string,
	poolTypeField string,
	affinityField string,
	affinityValue string,
) (*JSONGateResolverBuilder, error) {
	jsonDiscovery := &JSONGateResolverBuilder{
		targets:       map[string][]targetHost{},
		jsonPath:      jsonPath,
		addressField:  addressField,
		portField:     portField,
		poolTypeField: poolTypeField,
		affinityField: affinityField,
		affinityValue: affinityValue,
	}

	resolver.Register(jsonDiscovery)
	log.Infof("Registered JSON discovery scheme %v to watch: %v\n", jsonDiscovery.Scheme(), jsonPath)

	err := jsonDiscovery.start()
	if err != nil {
		return nil, err
	}

	return jsonDiscovery, nil
}

func (*JSONGateResolverBuilder) Scheme() string { return "vtgate" }

// Parse and validate the format of the file and start watching for changes
func (b *JSONGateResolverBuilder) start() error {

	b.rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	// Perform the initial parse
	_, err := b.parse()
	if err != nil {
		return err
	}

	// Validate some stats
	if len(b.targets) == 0 {
		return fmt.Errorf("no valid targets in file %s", b.jsonPath)
	}

	// Log some stats on startup
	poolTypes := map[string]int{}
	affinityTypes := map[string]int{}

	for _, ts := range b.targets {
		for _, t := range ts {
			count := poolTypes[t.poolType]
			poolTypes[t.poolType] = count + 1

			count = affinityTypes[t.affinity]
			affinityTypes[t.affinity] = count + 1
		}
	}

	parseCount.Add("changed", 1)

	log.Infof("loaded targets, pool types %v, affinity groups %v", poolTypes, affinityTypes)

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

			contentsChanged, err := b.parse()
			if err != nil {
				parseCount.Add("error", 1)
				if parseErr == nil || err.Error() != parseErr.Error() {
					parseErr = err
					log.Error(err)
				}
				continue
			}
			if !contentsChanged {
				parseCount.Add("unchanged", 1)
				continue
			}
			parseErr = nil
			parseCount.Add("changed", 1)

			// notify all the resolvers that the targets changed
			for _, r := range b.resolvers {
				b.update(r)
			}
		}
	}()

	return nil
}

// parse the file and build the target host list, returning whether or not the list was
// updated since the last parse, or if the checksum matched
func (b *JSONGateResolverBuilder) parse() (bool, error) {
	data, err := os.ReadFile(b.jsonPath)
	if err != nil {
		return false, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, bytes.NewReader(data)); err != nil {
		return false, err
	}
	sum := h.Sum(nil)

	if bytes.Equal(sum, b.checksum) {
		log.V(100).Infof("file did not change (checksum %x), skipping re-parse", sum)
		return false, nil
	}
	b.checksum = sum
	log.V(100).Infof("detected file change (checksum %x), parsing", sum)

	hosts := []map[string]interface{}{}
	err = json.Unmarshal(data, &hosts)
	if err != nil {
		return false, fmt.Errorf("error parsing JSON discovery file %s: %v", b.jsonPath, err)
	}

	var targets = map[string][]targetHost{}
	for _, host := range hosts {
		address, hasAddress := host[b.addressField]
		port, hasPort := host[b.portField]
		poolType, hasPoolType := host[b.poolTypeField]
		affinity, hasAffinity := host[b.affinityField]

		if !hasAddress {
			return false, fmt.Errorf("error parsing JSON discovery file %s: address field %s not present", b.jsonPath, b.addressField)
		}

		if !hasPort {
			return false, fmt.Errorf("error parsing JSON discovery file %s: port field %s not present", b.jsonPath, b.portField)
		}

		if b.poolTypeField != "" && !hasPoolType {
			return false, fmt.Errorf("error parsing JSON discovery file %s: pool type field %s not present", b.jsonPath, b.poolTypeField)
		}

		if b.affinityField != "" && !hasAffinity {
			return false, fmt.Errorf("error parsing JSON discovery file %s: affinity field %s not present", b.jsonPath, b.affinityField)
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
			return false, fmt.Errorf("error parsing JSON discovery file %s: port field %s has invalid value %v", b.jsonPath, b.portField, port)
		}

		target := targetHost{fmt.Sprintf("%s:%s", address, port), poolType.(string), affinity.(string)}
		targets[target.poolType] = append(targets[target.poolType], target)
	}

	for poolType := range targets {
		if b.affinityField != "" {
			sort.Slice(targets[poolType], func(i, j int) bool {
				return b.affinityValue == targets[poolType][i].affinity
			})
		}
		if len(targets[poolType]) > *numConnections {
			targets[poolType] = targets[poolType][:*numConnections]
		}
		targetCount.Set(poolType, int64(len(targets[poolType])))
	}

	b.targets = targets

	return true, nil
}

// Update the current list of hosts for the given resolver
func (b *JSONGateResolverBuilder) update(r *JSONGateResolver) {

	log.V(100).Infof("resolving target %s to %d connections\n", r.target.URL.String(), *numConnections)

	targets := b.targets[r.poolType]

	// Shuffle to ensure every host has a different order to iterate through, putting
	// the affinity matching (e.g. same az) hosts at the front and the non-matching ones
	// at the end.
	//
	// Only need to do n-1 swaps since the last host is always in the right place.
	n := len(targets)
	head := 0
	tail := n - 1
	for i := 0; i < n-1; i++ {
		j := head + b.rand.Intn(tail-head+1)

		if r.affinity == "" || r.affinity == targets[j].affinity {
			targets[head], targets[j] = targets[j], targets[head]
			head++
		} else {
			targets[tail], targets[j] = targets[j], targets[tail]
			tail--
		}
	}

	var addrs []resolver.Address
	for _, target := range targets {
		addrs = append(addrs, resolver.Address{Addr: target.addr})
	}

	log.V(100).Infof("updated targets for %s to %v", r.target.URL.String(), targets)

	r.clientConn.UpdateState(resolver.State{Addresses: addrs})
}

// Build a new Resolver to route to the given target
func (b *JSONGateResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	attrs := target.URL.Query()

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

	r := &JSONGateResolver{
		target:     target,
		clientConn: cc,
		poolType:   poolType,
		affinity:   b.affinityValue,
	}

	b.update(r)
	b.resolvers = append(b.resolvers, r)

	return r, nil
}

func (r *JSONGateResolver) ResolveNow(o resolver.ResolveNowOptions) {}

func (r *JSONGateResolver) Close() {
	log.Infof("Closing resolver for target %s", r.target.URL.String())
}
