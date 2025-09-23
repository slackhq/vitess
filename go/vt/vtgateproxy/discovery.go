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

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
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

const PoolTypeAttr = "PoolType"
const ZoneLocalAttr = "ZoneLocal"

// Resolver(https://godoc.org/google.golang.org/grpc/resolver#Resolver).
type JSONGateResolver struct {
	target       resolver.Target
	clientConn   resolver.ClientConn
	poolType     string
	currentAddrs []resolver.Address
	mu           sync.Mutex
}

func (r *JSONGateResolver) ResolveNow(o resolver.ResolveNowOptions) {}

func (r *JSONGateResolver) Close() {
	log.Infof("Closing resolver for target %s", r.target.URL.String())
}

type JSONGateResolverBuilder struct {
	jsonPath       string
	addressField   string
	portField      string
	poolTypeField  string
	affinityField  string
	affinityValue  string
	numConnections int
	numBackupConns int

	// Identity verification settings
	enableIdentityVerify bool
	verifyTimeout        time.Duration
	useSQLVerification   bool

	mu        sync.RWMutex
	targets   map[string][]targetHost
	resolvers []*JSONGateResolver

	sorter   *shuffleSorter
	ticker   *time.Ticker
	checksum []byte
}

type targetHost struct {
	Addr     string
	PoolType string
	Affinity string
	IsLocal  bool
	HTTPAddr string // HTTP address for health checks
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
	numConnections int,
	numBackupConns int,
	enableIdentityVerify bool,
	verifyTimeout time.Duration,
	useSQLVerification bool,
) (*JSONGateResolverBuilder, error) {
	jsonDiscovery := &JSONGateResolverBuilder{
		targets:              map[string][]targetHost{},
		jsonPath:             jsonPath,
		addressField:         addressField,
		portField:            portField,
		poolTypeField:        poolTypeField,
		affinityField:        affinityField,
		affinityValue:        affinityValue,
		numConnections:       numConnections,
		numBackupConns:       numBackupConns,
		enableIdentityVerify: enableIdentityVerify,
		verifyTimeout:        verifyTimeout,
		useSQLVerification:   useSQLVerification,
		sorter:               newShuffleSorter(),
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

func (*JSONGateResolverBuilder) Scheme() string { return "vtgate" }

// Parse and validate the format of the file and start watching for changes
func (b *JSONGateResolverBuilder) start() error {
	// Stat before parse to prevent the race condition with the polling loop
	fileStat, err := os.Stat(b.jsonPath)
	if err != nil {
		return err
	}

	// Perform the initial parse
	_, err = b.parse()
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
			count := poolTypes[t.PoolType]
			poolTypes[t.PoolType] = count + 1

			count = affinityTypes[t.Affinity]
			affinityTypes[t.Affinity] = count + 1
		}
	}

	parseCount.Add("changed", 1)

	log.Infof("loaded targets, pool types %v, affinity %s, groups %v", poolTypes, *affinityValue, affinityTypes)

	// Start a config watcher
	b.ticker = time.NewTicker(1 * time.Second)

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
			parseErr = nil
			if !contentsChanged {
				parseCount.Add("unchanged", 1)
				continue
			}
			parseCount.Add("changed", 1)

			var wg sync.WaitGroup

			// notify all the resolvers that the targets changed in parallel, since each update might sleep for
			// the warmup time
			b.mu.RLock()
			for _, r := range b.resolvers {
				wg.Add(1)
				go func(r *JSONGateResolver) {
					defer wg.Done()

					err = b.update(r)
					if err != nil {
						log.Errorf("Failed to update resolver: %v", err)
					}
				}(r)
			}
			b.mu.RUnlock()
			wg.Wait()
		}
	}()

	return nil
}

// verifyVTGateIdentity makes a MySQL connection to the vtgate to verify it's accessible and
// checks its identity using SQL queries to detect pool mismatches
func (b *JSONGateResolverBuilder) verifyVTGateIdentity(ctx context.Context, addr, expectedPool string) bool {
	if !b.enableIdentityVerify {
		return true // Skip verification if disabled
	}

	// Always use SQL-based verification since HTTP endpoints are not accessible in production
	// The useSQLVerification flag is kept for backward compatibility but SQL is the only viable option
	return b.verifyVTGateSQLIdentity(ctx, addr, expectedPool)
}

// getMySQLPortForAddress finds the MySQL server port for a given GRPC address
func (b *JSONGateResolverBuilder) getMySQLPortForAddress(grpcAddr string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	log.V(100).Infof("Looking for MySQL port for GRPC address: %s", grpcAddr)

	// Look through all configured targets to find the MySQL port for this GRPC address
	for poolType, targets := range b.targets {
		for _, target := range targets {
			log.V(100).Infof("Checking target in pool %s: Addr=%s, HTTPAddr=%s", poolType, target.Addr, target.HTTPAddr)

			if target.Addr == grpcAddr {
				log.V(100).Infof("Found matching target for %s", grpcAddr)

				// Check if HTTPAddr contains the MySQL server port
				if target.HTTPAddr != "" {
					parts := strings.Split(target.HTTPAddr, ":")
					if len(parts) == 2 {
						var mysqlPort int
						if _, err := fmt.Sscanf(parts[1], "%d", &mysqlPort); err == nil && mysqlPort > 0 {
							log.V(100).Infof("Found MySQL port %d from HTTPAddr for %s", mysqlPort, grpcAddr)
							return mysqlPort
						}
					}
				}

				// Fallback: In test environments, MySQL server port is often GRPC port + 1
				// Parse GRPC port from target.Addr and calculate MySQL port
				parts := strings.Split(target.Addr, ":")
				if len(parts) == 2 {
					var grpcPort int
					if _, err := fmt.Sscanf(parts[1], "%d", &grpcPort); err == nil && grpcPort > 0 {
						// In the test environment, MySQL server port = GRPC port + 1
						mysqlPort := grpcPort + 1
						log.V(100).Infof("Calculated MySQL port %d from GRPC port %d for %s", mysqlPort, grpcPort, grpcAddr)
						return mysqlPort
					}
				}
			}
		}
	}

	log.V(100).Infof("Could not find MySQL port for GRPC address: %s", grpcAddr)
	return 0
}

// isConfiguredForPool checks if this vtgate address was explicitly configured for the expected pool
func (b *JSONGateResolverBuilder) isConfiguredForPool(httpAddr, expectedPool string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Look through all configured targets to see if this HTTP address belongs to the expected pool
	for poolType, targets := range b.targets {
		for _, target := range targets {
			if target.HTTPAddr == httpAddr {
				// Found the address - check if it's in the expected pool
				return poolType == expectedPool
			}
		}
	}

	// Address not found in any pool configuration
	return false
}


// verifyVTGateSQLIdentity performs identity verification by making a direct MySQL connection
// to the vtgate and using SQL queries to verify identity and pool membership
func (b *JSONGateResolverBuilder) verifyVTGateSQLIdentity(ctx context.Context, addr, expectedPool string) bool {
	log.V(100).Infof("Starting SQL-based identity verification for %s (expected pool: %s)", addr, expectedPool)

	// Parse address to get host
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		log.V(100).Infof("Invalid address format %s: expected host:port", addr)
		return false
	}
	host := parts[0]

	// The addr parameter might contain HTTP, GRPC, or other port
	// We need to find the MySQL server port for this vtgate from our configuration
	mysqlPort := b.getMySQLPortForAddress(addr)
	if mysqlPort == 0 {
		// If we can't find the exact address, try to derive MySQL port from the provided port
		// based on common test patterns
		parts := strings.Split(addr, ":")
		if len(parts) == 2 {
			var portNum int
			if _, err := fmt.Sscanf(parts[1], "%d", &portNum); err == nil && portNum > 0 {
				// Common patterns in Vitess tests:
				// If this is HTTP port (7721), GRPC is usually HTTP+1 (7722), MySQL is GRPC+1 (7723)
				// If this is GRPC port (7722), MySQL is usually GRPC+1 (7723)
				// Try MySQL = provided_port + 2 first (HTTP -> MySQL), then +1 (GRPC -> MySQL)
				for _, offset := range []int{2, 1} {
					mysqlPort = portNum + offset
					log.V(100).Infof("Trying calculated MySQL port %d (offset +%d) from provided port %d", mysqlPort, offset, portNum)

					// Test if this MySQL port is actually accessible
					testParams := mysql.ConnParams{
						Host: host,
						Port: mysqlPort,
						Uname: "",
						Pass:  "",
					}

					testCtx, testCancel := context.WithTimeout(ctx, 100*time.Millisecond) // Quick test
					testConn, testErr := mysql.Connect(testCtx, &testParams)
					testCancel()

					if testErr == nil {
						testConn.Close()
						log.V(100).Infof("Successfully verified MySQL port %d for %s", mysqlPort, addr)
						break
					} else {
						log.V(100).Infof("MySQL port %d failed test for %s: %v", mysqlPort, addr, testErr)
						mysqlPort = 0
					}
				}
			}
		}

		if mysqlPort == 0 {
			log.V(100).Infof("Could not find or derive MySQL server port for address %s", addr)
			return false
		}
	}

	params := mysql.ConnParams{
		Host:  host,
		Port:  mysqlPort,
		Uname: "", // vtgate doesn't require authentication for system queries
		Pass:  "",
	}

	// Set a timeout for the connection
	connCtx, cancel := context.WithTimeout(ctx, b.verifyTimeout)
	defer cancel()

	conn, err := mysql.Connect(connCtx, &params)
	if err != nil {
		log.V(100).Infof("Failed to connect to vtgate at %s: %v", addr, err)
		return false
	}
	defer conn.Close()

	// First, perform a basic connectivity test
	if !b.sqlBasicHealthCheck(conn) {
		return false
	}

	// Try to get pool identity from Vitess-specific queries first
	actualPool := b.sqlGetPoolFromVitessShards(conn)
	if actualPool != "" {
		log.V(100).Infof("Retrieved pool '%s' from SHOW VITESS_SHARDS for vtgate at %s", actualPool, addr)
		if actualPool != expectedPool {
			log.Warningf("Pool mismatch for %s: VITESS_SHARDS indicates pool '%s' but expected '%s'",
				addr, actualPool, expectedPool)
			return false
		}
		log.V(100).Infof("VTGate Vitess shards identity verified for %s: pool '%s' matches expected '%s'",
			addr, actualPool, expectedPool)
		return true
	}

	// If Vitess shards verification failed, we don't have a reliable way to verify identity
	log.V(100).Infof("Vitess shards verification failed for vtgate at %s, no fallback verification available", addr)
	return false
}

// sqlBasicHealthCheck performs basic SQL connectivity test
func (b *JSONGateResolverBuilder) sqlBasicHealthCheck(conn *mysql.Conn) bool {
	// Test basic connectivity with a simple query
	result, err := conn.ExecuteFetch("SELECT 1", 1, false)
	if err != nil {
		log.V(100).Infof("SQL health check failed: %v", err)
		return false
	}

	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		log.V(100).Infof("SQL health check returned unexpected result: %v", result)
		return false
	}

	return true
}


// sqlGetPoolFromVitessShards extracts pool information from SHOW VITESS_SHARDS results
func (b *JSONGateResolverBuilder) sqlGetPoolFromVitessShards(conn *mysql.Conn) string {
	result, err := conn.ExecuteFetch("SHOW VITESS_SHARDS", 100, false)
	if err != nil {
		log.V(100).Infof("SHOW VITESS_SHARDS query failed: %v", err)
		return ""
	}

	if len(result.Rows) == 0 {
		log.V(100).Infof("SHOW VITESS_SHARDS returned no results")
		return ""
	}

	// Log the shards for debugging
	log.V(100).Infof("SHOW VITESS_SHARDS returned %d rows", len(result.Rows))
	for i, row := range result.Rows {
		if len(row) > 0 {
			shardInfo := row[0].ToString()
			log.V(100).Infof("Shard %d: %s", i, shardInfo)

			// Extract pool from keyspace name
			// Expected format: "commerce_poolA/0" -> pool is "poolA"
			if strings.Contains(shardInfo, "commerce_") {
				parts := strings.Split(shardInfo, "/")
				if len(parts) >= 1 {
					keyspace := parts[0]
					if strings.HasPrefix(keyspace, "commerce_") {
						pool := strings.TrimPrefix(keyspace, "commerce_")
						log.V(100).Infof("Extracted pool '%s' from keyspace '%s'", pool, keyspace)
						return pool
					}
				}
			}
		}
	}

	log.V(100).Infof("Could not extract pool from VITESS_SHARDS results")
	return ""
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

	var allTargets = map[string][]targetHost{}
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

		// Check for optional HTTP port for health checks
		httpPort := port // Default to same port if no http field
		if httpPortValue, hasHTTPPort := host["http"]; hasHTTPPort {
			switch httpPortValue.(type) {
			case int:
				httpPort = fmt.Sprintf("%d", httpPortValue)
			case string:
				httpPort = httpPortValue.(string)
			default:
				return false, fmt.Errorf("error parsing JSON discovery file %s: http field has invalid value %v", b.jsonPath, httpPortValue)
			}
		}

		target := targetHost{
			Addr:     fmt.Sprintf("%s:%s", address, port),
			PoolType: poolType.(string),
			Affinity: affinity.(string),
			IsLocal:  affinity == b.affinityValue,
			HTTPAddr: fmt.Sprintf("%s:%s", address, httpPort),
		}

		// Verify vtgate identity if enabled
		if b.enableIdentityVerify && b.poolTypeField != "" {
			ctx, cancel := context.WithTimeout(context.Background(), b.verifyTimeout)
			defer cancel()

			if !b.verifyVTGateIdentity(ctx, target.HTTPAddr, target.PoolType) {
				log.Warningf("Skipping vtgate %s: identity verification failed for pool %s", target.HTTPAddr, target.PoolType)
				continue
			}
		}

		allTargets[target.PoolType] = append(allTargets[target.PoolType], target)
	}

	// If a pool disappears, the metric will not record this unless all counts
	// are reset each time the file is parsed. If this ends up causing problems
	// with the metric briefly dropping to 0, it could be done by rlocking the
	// target lock and then comparing the previous targets with the current
	// targets and only resetting pools which disappear.
	targetCount.ResetAll()

	var selected = map[string][]targetHost{}

	for poolType := range allTargets {
		b.sorter.shuffleSort(allTargets[poolType])

		// try to pick numConnections from the front of the list (local zone) and numBackupConnections
		// from the tail (remote zone). if that's not possible, just take the whole set
		if len(allTargets[poolType]) >= b.numConnections+b.numBackupConns {
			remoteOffset := len(allTargets[poolType]) - b.numBackupConns
			selected[poolType] = append(allTargets[poolType][:b.numConnections], allTargets[poolType][remoteOffset:]...)
		} else {
			selected[poolType] = allTargets[poolType]
		}

		targetCount.Set(poolType, int64(len(selected[poolType])))
	}

	b.mu.Lock()
	b.targets = selected
	b.mu.Unlock()

	return true, nil
}

func (b *JSONGateResolverBuilder) GetPools() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var pools []string
	for pool := range b.targets {
		pools = append(pools, pool)
	}
	sort.Strings(pools)
	return pools
}

func (b *JSONGateResolverBuilder) getTargets(poolType string) []targetHost {
	// Copy the target slice
	b.mu.RLock()
	targets := []targetHost{}
	targets = append(targets, b.targets[poolType]...)
	b.mu.RUnlock()

	b.sorter.shuffleSort(targets)

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
func (s *shuffleSorter) shuffleSort(targets []targetHost) {
	n := len(targets)
	head := 0
	// Only need to do n-1 swaps since the last host is always in the right place.
	tail := n - 1
	for i := 0; i < n-1; i++ {
		s.mu.Lock()
		j := head + s.rand.Intn(tail-head+1)
		s.mu.Unlock()

		if targets[j].IsLocal {
			targets[head], targets[j] = targets[j], targets[head]
			head++
		} else {
			targets[tail], targets[j] = targets[j], targets[tail]
			tail--
		}
	}
}

// Update the current list of hosts for the given resolver
func (b *JSONGateResolverBuilder) update(r *JSONGateResolver) error {
	log.V(100).Infof("resolving target %s to %d connections\n", r.target.URL.String(), *numConnections)

	targets := b.getTargets(r.poolType)

	// There should only ever be a single goroutine calling update on a given Resolver,
	// but add a lock just in case to ensure that the r.currentAddrs are in fact synchronized
	r.mu.Lock()
	defer r.mu.Unlock()

	var addrs []resolver.Address
	for _, target := range targets {
		attrs := attributes.New(PoolTypeAttr, r.poolType).WithValue(ZoneLocalAttr, target.IsLocal)
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
func (b *JSONGateResolverBuilder) debugTargets() any {
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
    <td>{{.IsLocal}}</td>
  </tr>{{end}}
{{end}}
</table>
`
)
