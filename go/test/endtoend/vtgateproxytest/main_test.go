/*
Copyright 2019 The Vitess Authors.

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

package vtgateproxytest

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "commerce"
	cell            = "zone1"
	sqlSchema       = `create table product(
		sku varbinary(128),
			description varbinary(128),
			price bigint,
			primary key(sku)
		) ENGINE=InnoDB;
		create table customer(
			id bigint not null auto_increment,
			email varchar(128),
			primary key(id)
		) ENGINE=InnoDB;
		create table corder(
			order_id bigint not null auto_increment,
			customer_id bigint,
			sku varbinary(128),
			price bigint,
			primary key(order_id)
		) ENGINE=InnoDB;`

	vSchema = `{
					"tables": {
						"product": {},
						"customer": {},
						"corder": {}
					}
				}`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, true)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func selectHelper[T any](ctx context.Context, conn *sql.DB, query string) ([]T, error) {
	var result []T

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var row T
		v := reflect.ValueOf(&row).Elem()

		var fields []any
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).CanAddr() {
				fields = append(fields, v.Field(i).Addr().Interface())
			}
		}

		err = rows.Scan(fields...)
		if err != nil {
			return nil, err
		}

		result = append(result, row)
	}

	return result, nil
}

type customerEntry struct {
	ID    int
	Email string
}

func NewVtgateProxyProcess(vtgateHostsFile, affinity, balancerType string, numConnections, httpPort, grpcPort, mySQLPort int) *VtgateProxyProcess {
	return &VtgateProxyProcess{
		Name:            "vtgateproxy",
		Binary:          "vtgateproxy",
		VtgateHostsFile: vtgateHostsFile,
		BalancerType:    balancerType,
		AddressField:    "address",
		PortField:       "grpc",
		PoolTypeField:   "type",
		AffinityField:   "az_id",
		AffinityValue:   affinity,
		NumConnections:  numConnections,
		HTTPPort:        httpPort,
		GrpcPort:        grpcPort,
		MySQLPort:       mySQLPort,
		VerifyURL:       "http://" + net.JoinHostPort("localhost", strconv.Itoa(httpPort)) + "/metrics",
	}
}

type VtgateProxyProcess struct {
	Name            string
	Binary          string
	VtgateHostsFile string
	BalancerType    string
	AddressField    string
	PortField       string
	PoolTypeField   string
	AffinityField   string
	AffinityValue   string
	NumConnections  int
	HTTPPort        int
	GrpcPort        int
	MySQLPort       int
	ExtraArgs       []string
	VerifyURL       string

	proc *exec.Cmd
	exit chan error
}

func (vt *VtgateProxyProcess) Setup() error {
	args := []string{
		"--port", strconv.Itoa(vt.HTTPPort),
		"--grpc_port", strconv.Itoa(vt.GrpcPort),
		"--mysql_server_port", strconv.Itoa(vt.MySQLPort),
		"--vtgate_hosts_file", vt.VtgateHostsFile,
		"--balancer", vt.BalancerType,
		"--address_field", vt.AddressField,
		"--port_field", vt.PortField,
		"--pool_type_field", vt.PoolTypeField,
		"--affinity_field", vt.AffinityField,
		"--affinity_value", vt.AffinityValue,
		"--num_connections", strconv.Itoa(vt.NumConnections),
		"--logtostderr",
		"--grpc_prometheus",
		"--mysql_auth_server_impl", "none",
	}
	args = append(args, vt.ExtraArgs...)

	vt.proc = exec.Command(
		vt.Binary,
		args...,
	)
	vt.proc.Env = append(vt.proc.Env, os.Environ()...)
	vt.proc.Stdout = os.Stdout
	vt.proc.Stderr = os.Stderr

	log.Infof("Running vtgateproxy with command: %v", strings.Join(vt.proc.Args, " "))

	err := vt.proc.Start()
	if err != nil {
		return err
	}
	vt.exit = make(chan error)
	go func() {
		if vt.proc != nil {
			vt.exit <- vt.proc.Wait()
		}
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if vt.WaitForStatus() {
			return nil
		}
		select {
		case err := <-vt.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vt.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", vt.Name, <-vt.exit)
}

// WaitForStatus function checks if vtgateproxy process is up and running
func (vt *VtgateProxyProcess) WaitForStatus() bool {
	resp, err := http.Get(vt.VerifyURL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200
}

func (vt *VtgateProxyProcess) Teardown() error {
	if err := vt.proc.Process.Kill(); err != nil {
		log.Errorf("Failed to kill %v: %v", vt.Name, err)
	}
	if vt.proc == nil || vt.exit == nil {
		return nil
	}
	vt.proc.Process.Signal(syscall.SIGTERM)

	select {
	case <-vt.exit:
		vt.proc = nil
		return nil

	case <-time.After(30 * time.Second):
		vt.proc.Process.Kill()
		vt.proc = nil
		return <-vt.exit
	}
}

func (vt *VtgateProxyProcess) GetMySQLConn() (*sql.DB, error) {
	// Use the go mysql driver since the vitess mysql client does not support
	// connectionAttributes.
	dsn := fmt.Sprintf("tcp(%v)/ks?connectionAttributes=type:pool1,az_id:use1-az1", net.JoinHostPort(clusterInstance.Hostname, strconv.Itoa(vt.MySQLPort)))
	log.Infof("Using DSN %v", dsn)

	return sql.Open("mysql", dsn)
}

func startAdditionalVtgates(count int) ([]*cluster.VtgateProcess, error) {
	var vtgates []*cluster.VtgateProcess
	var err error
	defer func() {
		if err != nil {
			teardownVtgates(vtgates)
		}
	}()

	for i := 0; i < count; i++ {
		vtgateInstance := newVtgateInstance(i)
		log.Infof("Starting additional vtgate on port %d", vtgateInstance.Port)
		if err = vtgateInstance.Setup(); err != nil {
			return nil, err
		}

		vtgates = append(vtgates, vtgateInstance)
	}

	return vtgates, nil
}

func newVtgateInstance(i int) *cluster.VtgateProcess {
	vtgateProcInstance := cluster.VtgateProcessInstance(
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		clusterInstance.GetAndReservePort(),
		clusterInstance.Cell,
		clusterInstance.Cell,
		clusterInstance.Hostname,
		"PRIMARY,REPLICA",
		clusterInstance.TopoProcess.Port,
		clusterInstance.TmpDirectory,
		clusterInstance.VtGateExtraArgs,
		clusterInstance.VtGatePlannerVersion,
	)
	vtgateProcInstance.MySQLServerSocketPath = path.Join(clusterInstance.TmpDirectory, fmt.Sprintf("mysql%v.sock", i))

	return vtgateProcInstance
}

func teardownVtgates(vtgates []*cluster.VtgateProcess) error {
	var err error
	for _, vtgate := range vtgates {
		if vErr := vtgate.TearDown(); vErr != nil {
			err = vErr
		}
	}

	return err
}

func getVtgateQueryCount(vtgate *cluster.VtgateProcess) (queryCount, error) {
	var result queryCount

	vars, err := vtgate.GetVars()
	if err != nil {
		return result, err
	}

	queriesProcessed, ok := vars["QueriesProcessed"]
	if !ok {
		return result, nil
	}

	v := reflect.ValueOf(&result).Elem()

	for k, val := range queriesProcessed.(map[string]any) {
		v.FieldByName(k).SetInt(int64(val.(float64)))
	}

	return result, err
}

type queryCount struct {
	Begin           int
	Commit          int
	Unsharded       int
	InsertUnsharded int
}

func (q queryCount) Sum() int {
	var result int
	v := reflect.ValueOf(q)
	for i := 0; i < v.NumField(); i++ {
		result += int(v.Field(i).Int())
	}

	return result
}
