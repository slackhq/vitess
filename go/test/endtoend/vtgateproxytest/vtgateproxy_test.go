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
	"os"
	"path/filepath"
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

func TestVtgateproxyRoundRobinProcess(t *testing.T) {
	defer cluster.PanicHandler(t)

	config := []map[string]string{
		{
			"host":    "vtgate1",
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(clusterInstance.VtgateProcess.GrpcPort),
			"az_id":   "use1-az1",
			"type":    "pool1",
		},
	}
	b, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	vtgateHostsFile := filepath.Join(clusterInstance.TmpDirectory, "hosts")
	if err := os.WriteFile(vtgateHostsFile, b, 0644); err != nil {
		t.Fatal(err)
	}

	vtgateproxyHTTPPort := clusterInstance.GetAndReservePort()
	vtgateproxyGrpcPort := clusterInstance.GetAndReservePort()
	vtgateproxyMySQLPort := clusterInstance.GetAndReservePort()

	vtgateproxyProcInstance := NewVtgateProxyProcess(
		vtgateHostsFile,
		"use1-az1",
		"round_robin",
		1,
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

	log.Info("Reading test value")
	result, err := selectHelper[customerEntry](context.Background(), conn, "select id, email from customer")
	if err != nil {
		t.Fatal(err)
	}

	log.Infof("Read value %v", result)

	assert.Equal(t, []customerEntry{{1, "email1"}}, result)
}
