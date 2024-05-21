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

func TestVtgateProxyRoundRobinScale(t *testing.T) {
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
	const vtgateCount = 10
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
			"type":    "pool1",
		})
	}

	// Start with an empty list of vtgates, then scale up, then scale back to
	// 0. We should expect to see immediate failure when there are no vtgates,
	// then success at each scale, until we hit 0 vtgates again, at which point
	// we should fail fast again.
	b, err := json.Marshal(config[:0])
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
		vtgateHostsFile,
		targetAffinity,
		"round_robin",
		vtgateproxyConnections,
		vtgateproxyHTTPPort,
		vtgateproxyGrpcPort,
		vtgateproxyMySQLPort,
	)
	if err := vtgateproxyProcInstance.Setup(); err != nil {
		t.Fatal(err)
	}
	defer vtgateproxyProcInstance.Teardown()

	conn, err := vtgateproxyProcInstance.GetMySQLConn()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		t.Fatal(err)
	}

	log.Info("Reading test value while scaling vtgates")

	for i := range config {
		b, err = json.Marshal(config[:i])
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(vtgateHostsFile, b, 0644); err != nil {
			t.Fatal(err)
		}

		time.Sleep(1 * time.Second)

		result, err := selectHelper[customerEntry](context.Background(), conn, "select id, email from customer")
		if i == 0 {
			if err == nil {
				t.Fatal("query should have failed with no vtgates")
			}
			continue
		} else if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, []customerEntry{{1, "email1"}}, result)
	}

	for i := len(vtgates) - 1; i >= 0; i-- {
		b, err = json.Marshal(config[:i])
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(vtgateHostsFile, b, 0644); err != nil {
			t.Fatal(err)
		}

		time.Sleep(1 * time.Second)

		result, err := selectHelper[customerEntry](context.Background(), conn, "select id, email from customer")
		if i == 0 {
			if err == nil {
				t.Fatal("query should have failed with no vtgates")
			}
			continue
		} else if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, []customerEntry{{1, "email1"}}, result)
	}
}
