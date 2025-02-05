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

This tests select/insert using the unshared keyspace added in main_test
*/
package vtgateproxy

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

func TestVtgateProxyRebalanceRoundRobin(t *testing.T) {
	testVtgateProxyRebalance(t, "round_robin")
}

func TestVtgateProxyRebalanceFirstReady(t *testing.T) {
	testVtgateProxyRebalance(t, "first_ready")
}

func TestVtgateProxyRebalanceStickyRandom(t *testing.T) {
	testVtgateProxyRebalance(t, "sticky_random")
}

func testVtgateProxyRebalance(t *testing.T, loadBalancer string) {
	defer cluster.PanicHandler(t)

	const targetAffinity = "use1-az1"
	const targetPool = "pool1"
	const vtgateCount = 5
	const vtgateproxyConnections = 4

	vtgates, err := startAdditionalVtgates(vtgateCount)
	if err != nil {
		t.Fatal(err)
	}
	defer teardownVtgates(vtgates)

	vtgateHostsFile := filepath.Join(clusterInstance.TmpDirectory, "hosts")
	var config []map[string]string

	for i, vtgate := range vtgates {
		config = append(config, map[string]string{
			"host":    fmt.Sprintf("vtgate%v", i),
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(vtgate.GrpcPort),
			"az_id":   targetAffinity,
			"type":    targetPool,
		})
	}

	if err := writeConfig(t, vtgateHostsFile, config, nil); err != nil {
		t.Fatal(err)
	}

	// Spin up proxy
	vtgateproxyHTTPPort := clusterInstance.GetAndReservePort()
	vtgateproxyGrpcPort := clusterInstance.GetAndReservePort()
	vtgateproxyMySQLPort := clusterInstance.GetAndReservePort()

	vtgateproxyProcInstance := NewVtgateProxyProcess(
		clusterInstance.TmpDirectory,
		vtgateHostsFile,
		targetAffinity,
		loadBalancer,
		vtgateproxyConnections,
		vtgateproxyHTTPPort,
		vtgateproxyGrpcPort,
		vtgateproxyMySQLPort,
	)
	if err := vtgateproxyProcInstance.Setup(); err != nil {
		t.Fatal(err)
	}
	defer vtgateproxyProcInstance.Teardown()

	conn, err := vtgateproxyProcInstance.GetMySQLConn(targetPool, targetAffinity)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		t.Fatal(err)
	}

	log.Info("Reading test value while adding vtgates")

	// Scale up
	for i := 1; i <= vtgateCount; i++ {
		if err := writeConfig(t, vtgateHostsFile, config[:i], vtgateproxyProcInstance); err != nil {
			t.Fatal(err)
		}

		// Run queries at each configuration
		for j := 0; j < 100; j++ {
			result, err := selectHelper[customerEntry](context.Background(), conn, "select id, email from customer")
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, []customerEntry{{1, "email1"}}, result)
		}
	}

	log.Info("Removing first vtgates")

	// Pop the first 2 vtgates off to force first_ready to pick a new target
	if err := writeConfig(t, vtgateHostsFile, config[2:], vtgateproxyProcInstance); err != nil {
		t.Fatal(err)
	}

	// Run queries in the last configuration
	for j := 0; j < 100; j++ {
		result, err := selectHelper[customerEntry](context.Background(), conn, "select id, email from customer")
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, []customerEntry{{1, "email1"}}, result)
	}

	var expectVtgatesWithQueries int

	switch loadBalancer {
	case "round_robin":
		// Every vtgate should get some queries. We went from 1 vtgates to
		// NumConnections+1 vtgates, and then removed the first vtgate.
		expectVtgatesWithQueries = len(vtgates)
	case "first_ready":
		// Only 2 vtgates should have queries. The first vtgate should get all
		// queries until it is removed, and then a new vtgate should be picked
		// to get all subsequent queries.
		expectVtgatesWithQueries = 2
	}

	var vtgatesWithQueries int
	for i, vtgate := range vtgates {
		queryCount, err := getVtgateQueryCount(vtgate)
		if err != nil {
			t.Fatal(err)
		}

		log.Infof("vtgate %v query counts: %+v", i, queryCount)

		if queryCount.Sum() > 0 {
			vtgatesWithQueries++
		}
	}

	assert.Equal(t, expectVtgatesWithQueries, vtgatesWithQueries)
}
