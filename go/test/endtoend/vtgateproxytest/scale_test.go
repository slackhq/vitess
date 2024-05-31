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
package vtgateproxytest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
)

func TestVtgateProxyScaleRoundRobin(t *testing.T) {
	testVtgateProxyScale(t, "round_robin")
}

func TestVtgateProxyScaleFirstReady(t *testing.T) {
	testVtgateProxyScale(t, "first_ready")
}

func testVtgateProxyScale(t *testing.T, loadBalancer string) {
	defer cluster.PanicHandler(t)

	// insert test value
	func() {
		conn, err := mysql.Connect(context.Background(), &vtParams)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		utils.Exec(t, conn, "insert into customer(id, email) values(1, 'email1')")
	}()

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
		pool := targetPool
		if i == 0 {
			// First vtgate is in a different pool and should not have any
			// queries routed to it.
			pool = "pool2"
		}

		config = append(config, map[string]string{
			"host":    fmt.Sprintf("vtgate%v", i),
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(vtgate.GrpcPort),
			"az_id":   targetAffinity,
			"type":    pool,
		})
	}
	b, err := json.Marshal(config[:1])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(vtgateHostsFile, b, 0644); err != nil {
		t.Fatal(err)
	}

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

	log.Info("Reading test value while scaling vtgates")

	// Start with an empty list of vtgates, then scale up, then scale back to
	// 0. We should expect to see immediate failure when there are no vtgates,
	// then success at each scale, until we hit 0 vtgates again, at which point
	// we should fail fast again.
	i := 0
	scaleUp := true
	for {
		t.Logf("writing config file with %v vtgates", i)
		b, err = json.Marshal(config[:i])
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(vtgateHostsFile, b, 0644); err != nil {
			t.Fatal(err)
		}

		if err := vtgateproxyProcInstance.WaitForConfig(config[:i], 5*time.Second); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		result, err := selectHelper[customerEntry](ctx, conn, "select id, email from customer")
		// 0 vtgates should fail
		// First vtgate is in the wrong pool, so it should also fail
		if i <= 1 {
			if err == nil {
				t.Fatal("query should have failed with no vtgates")
			}

			// In first_ready mode, we expect to fail fast and not time out.
			if loadBalancer == "first_ready" && errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("query timed out but it should have failed fast")
			}
		} else if err != nil {
			t.Fatalf("%v vtgates were present, but the query still failed: %v", i, err)
		} else {
			assert.Equal(t, []customerEntry{{1, "email1"}}, result)
		}

		if scaleUp {
			i++
			if i >= len(config) {
				scaleUp = false
				i -= 2
			}

			continue
		}

		i--
		if i < 0 {
			break
		}
	}
}
