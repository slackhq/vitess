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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

func TestVtgateProxyVtgateFailureRoundRobin(t *testing.T) {
	defer cluster.PanicHandler(t)

	const targetAffinity = "use1-az1"
	const vtgateCount = 4
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

	b, err := json.Marshal(config)
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

	conn, err := vtgateproxyProcInstance.GetMySQLConn("pool1", targetAffinity)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		t.Fatal(err)
	}

	log.Info("Stopping 1 vtgate")
	err = vtgates[0].TearDown()
	if err != nil {
		t.Fatal(err)
	}

	log.Info("Reading test value with one stopped vtgate")
	for i := 0; i < vtgateproxyConnections; i++ {
		result, err := selectHelper[customerEntry](context.Background(), conn, "select id, email from customer")
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, []customerEntry{{1, "email1"}}, result)
	}
}

func TestVtgateProxyVtgateFailureFirstReady(t *testing.T) {
	defer cluster.PanicHandler(t)

	const targetAffinity = "use1-az1"
	const vtgateCount = 4
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

	b, err := json.Marshal(config)
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
		"first_ready",
		vtgateproxyConnections,
		vtgateproxyHTTPPort,
		vtgateproxyGrpcPort,
		vtgateproxyMySQLPort,
	)
	if err := vtgateproxyProcInstance.Setup(); err != nil {
		t.Fatal(err)
	}
	defer vtgateproxyProcInstance.Teardown()

	conn, err := vtgateproxyProcInstance.GetMySQLConn("pool1", targetAffinity)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		t.Fatal(err)
	}

	// First send some queries to the active vtgate
	for i := 0; i < 10; i++ {
		result, err := selectHelper[customerEntry](context.Background(), conn, "select id, email from customer")
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, []customerEntry{{1, "email1"}}, result)
	}

	// Now kill the active vtgate
	for i := range vtgates {
		queryCount, err := getVtgateQueryCount(vtgates[i])
		if err != nil {
			t.Fatal(err)
		}

		if queryCount.Sum() > 0 {
			err = vtgates[i].TearDown()
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	log.Info("Reading test value after killing the active vtgate")
	result, err := selectHelper[customerEntry](context.Background(), conn, "select id, email from customer")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, []customerEntry{{1, "email1"}}, result)
}
