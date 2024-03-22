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
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/vt/log"
)

var (
	jsonDiscoveryConfig = flag.String("json_config", "", "json file describing the host list to use fot vitess://vtgate resolution")
	numConnectionsInt   = flag.Int("num_connections", 4, "number of outbound GPRC connections to maintain")
)

// File based discovery for vtgate grpc endpoints
// This loads the list of hosts from json and watches for changes to the list of hosts. It will select N connection to maintain to backend vtgates.
// Connections will rebalance every 5 minutes
//
// Example json config - based on the slack hosts format
//
// [
//     {
//         "address": "10.4.56.194",
//         "az_id": "use1-az1",
//         "grpc": "15999",
//         "type": "aux"
//     },
//
// Naming scheme:
// vtgate://<type>?num_connections=<int>&az_id=<string>
//
// num_connections: Option number of hosts to open connections to for round-robin selection
// az_id: Filter to just hosts in this az (optional)
// type: Only select from hosts of this type (required)
//

type DiscoveryHost struct {
	Address       string
	NebulaAddress string `json:"nebula_address"`
	Grpc          string
	AZId          string `json:"az_id"`
	Type          string
}

type JSONGateConfigDiscovery struct {
	JsonPath string
}

const queryParamFilterPrefix = "filter_"

func (b *JSONGateConfigDiscovery) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	log.V(100).Infof("Start registration for target: %v\n", target.URL.String())
	queryOpts := target.URL.Query()
	gateType := target.URL.Host

	filters := hostFilters{}
	filters["type"] = gateType
	for k := range queryOpts {
		if strings.HasPrefix(k, queryParamFilterPrefix) {
			filteredPrefix := strings.TrimPrefix(k, queryParamFilterPrefix)
			filters[filteredPrefix] = queryOpts.Get(k)
		}
	}

	r := &JSONGateConfigResolver{
		target:   target,
		cc:       cc,
		jsonPath: b.JsonPath,
		filters:  filters,
	}
	r.start()
	return r, nil
}
func (*JSONGateConfigDiscovery) Scheme() string { return "vtgate" }

func RegisterJsonDiscovery() {
	jsonDiscovery := &JSONGateConfigDiscovery{
		JsonPath: *jsonDiscoveryConfig,
	}
	resolver.Register(jsonDiscovery)
	log.Infof("Registered JSON discovery scheme %v to watch: %v\n", jsonDiscovery.Scheme(), *jsonDiscoveryConfig)
}

type hostFilters = map[string]string

// exampleResolver is a
// Resolver(https://godoc.org/google.golang.org/grpc/resolver#Resolver).
type JSONGateConfigResolver struct {
	target   resolver.Target
	cc       resolver.ClientConn
	jsonPath string
	ticker   *time.Ticker
	rand     *rand.Rand // safe for concurrent use.
	filters  hostFilters
}

type matchesFilter struct{}

func (r *JSONGateConfigResolver) resolve() (*[]resolver.Address, []byte, error) {
	pairs := []map[string]interface{}{}

	log.V(100).Infof("resolving target %s to %d connections\n", r.target.URL.String(), *numConnectionsInt)

	data, err := os.ReadFile(r.jsonPath)
	if err != nil {
		return nil, nil, err
	}

	err = json.Unmarshal(data, &pairs)
	if err != nil {
		log.Errorf("error parsing JSON discovery file %s: %v\n", r.jsonPath, err)
		return nil, nil, err
	}

	allAddrs := []resolver.Address{}
	filteredAddrs := []resolver.Address{}
	var addrs []resolver.Address
	for _, pair := range pairs {
		matchesAll := true
		for k, v := range r.filters {
			if pair[k] != v {
				matchesAll = false
			}
		}

		if matchesAll {
			filteredAddrs = append(filteredAddrs, resolver.Address{
				Addr:               fmt.Sprintf("%s:%s", pair["nebula_address"], pair["grpc"]),
				BalancerAttributes: attributes.New(matchesFilter{}, "match"),
			})
		}

		// Must filter by type
		t, ok := r.filters["type"]
		if ok {
			if pair["type"] == t {
				// Add matching hosts to registration list
				allAddrs = append(allAddrs, resolver.Address{
					Addr:               fmt.Sprintf("%s:%s", pair["nebula_address"], pair["grpc"]),
					BalancerAttributes: attributes.New(matchesFilter{}, "nomatch"),
				})
			}
		}
	}

	// Nothing in the filtered list? Get them all
	if len(filteredAddrs) == 0 {
		addrs = allAddrs
	} else if *numConnectionsInt == 0 {
		addrs = allAddrs
	} else if len(filteredAddrs) > *numConnectionsInt {
		addrs = filteredAddrs[0:*numConnectionsInt]
	} else if len(allAddrs) > *numConnectionsInt {
		addrs = allAddrs[0:*numConnectionsInt]
	} else {
		addrs = allAddrs
	}

	// Shuffle to ensure every host has a different order to iterate through
	r.rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})

	h := sha256.New()
	if _, err := io.Copy(h, bytes.NewReader(data)); err != nil {
		return nil, nil, err
	}
	sum := h.Sum(nil)

	log.V(100).Infof("resolved %s to addrs: 0x%x, %v\n", r.target.URL.String(), sum, addrs)

	return &addrs, sum, nil
}

func (r *JSONGateConfigResolver) start() {
	log.V(100).Infof("Starting discovery checker\n")
	r.rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	// Immediately load the initial config
	addrs, hash, err := r.resolve()
	if err == nil {
		// if we parse ok, populate the local address store
		r.cc.UpdateState(resolver.State{Addresses: *addrs})
	}

	// Start a config watcher
	r.ticker = time.NewTicker(100 * time.Millisecond)
	fileStat, err := os.Stat(r.jsonPath)
	if err != nil {
		return
	}
	go func() {
		for range r.ticker.C {
			checkFileStat, err := os.Stat(r.jsonPath)
			if err != nil {
				log.Errorf("Error stat'ing config %v\n", err)
				continue
			}
			isUnchanged := checkFileStat.Size() == fileStat.Size() || checkFileStat.ModTime() == fileStat.ModTime()
			if isUnchanged {
				// no change
				continue
			}

			fileStat = checkFileStat
			log.V(100).Infof("Detected config change\n")

			addrs, newHash, err := r.resolve()
			if err != nil {
				// better luck next loop
				// TODO: log this
				log.Errorf("Error resolving config: %v\n", err)
				continue
			}

			// Make sure this wasn't a spurious change by checking the hash
			if bytes.Equal(hash, newHash) && newHash != nil {
				log.V(100).Infof("No content changed in discovery file... ignoring\n")
				continue
			}

			hash = newHash

			r.cc.UpdateState(resolver.State{Addresses: *addrs})
		}
	}()

	log.V(100).Infof("Loaded hosts, starting ticker\n")

}
func (r *JSONGateConfigResolver) ResolveNow(o resolver.ResolveNowOptions) {}
func (r *JSONGateConfigResolver) Close() {
	r.ticker.Stop()
}

func init() {
	// Register the example ResolverBuilder. This is usually done in a package's
	// init() function.
}
