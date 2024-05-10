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

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

func TestVtgateProxyRebalanceRoundRobin(t *testing.T) {
	defer cluster.PanicHandler(t)

	const targetAffinity = "use1-az1"
	const vtgateCount = 10
	const vtgatesInAffinity = 8
	const vtgateproxyConnections = 4

	vtgates, err := startAdditionalVtgates(vtgateCount)
	if err != nil {
		t.Fatal(err)
	}
	defer teardownVtgates(vtgates)

	vtgateHostsFile := filepath.Join(clusterInstance.TmpDirectory, "hosts")
	var config []map[string]string

	for i, vtgate := range vtgates {
		affinity := targetAffinity
		if i >= vtgatesInAffinity {
			affinity = "use1-az2"
		}
		config = append(config, map[string]string{
			"host":    fmt.Sprintf("vtgate%v", i),
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(vtgate.GrpcPort),
			"az_id":   affinity,
			"type":    "pool1",
		})
	}

	vtgateIdx := vtgateproxyConnections
	b, err := json.Marshal(config[:vtgateIdx])
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

	log.Info("Inserting test value")
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec("insert into customer(id, email) values(1, 'email1')")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	log.Info("Reading test value while adding vtgates")

	const totalQueries = 1000
	const minQueryCountAffinity = 1
	const maxQueryCountNonAffinity = 0
	addVtgateEveryN := totalQueries / len(vtgates)

	for i := 0; i < totalQueries; i++ {
		if i%(addVtgateEveryN) == 0 && vtgateIdx <= len(vtgates) {
			log.Infof("Adding vtgate %v", vtgateIdx-1)
			b, err = json.Marshal(config[:vtgateIdx])
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(vtgateHostsFile, b, 0644); err != nil {
				t.Fatal(err)
			}

			time.Sleep(1 * time.Second)
			vtgateIdx++
		}

		result, err := selectHelper[customerEntry](context.Background(), conn, "select id, email from customer")
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, []customerEntry{{1, "email1"}}, result)
	}

	for i, vtgate := range vtgates {
		queryCount, err := getVtgateQueryCount(vtgate)
		if err != nil {
			t.Fatal(err)
		}

		affinity := config[i]["az_id"]

		log.Infof("vtgate %v (%v) query counts: %+v", i, affinity, queryCount)

		if affinity == targetAffinity {
			assert.GreaterOrEqual(t, queryCount.Sum(), minQueryCountAffinity, "vtgate %v did not recieve the expected number of queries", i)
		} else {
			assert.LessOrEqual(t, queryCount.Sum(), maxQueryCountNonAffinity, "vtgate %v recieved more than the expected number of queries", i)
		}
	}
}
