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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

func TestIdentityVerificationEnabled(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Create a config with a valid vtgate
	config := []map[string]string{
		{
			"host":    "vtgate1",
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(clusterInstance.VtgateProcess.GrpcPort),
			"http":    strconv.Itoa(clusterInstance.VtgateProcess.Port),
			"az_id":   "use1-az1",
			"type":    "pool1",
		},
	}
	
	vtgateproxyProcInstance := setupVtgateProxyWithIdentityVerification(t, config, true, "2s")
	defer vtgateproxyProcInstance.Teardown()

	// Wait for config to be processed
	err := vtgateproxyProcInstance.WaitForConfig(config, 30*time.Second)
	require.NoError(t, err, "Failed to wait for config")

	// Test that connection works with identity verification enabled
	conn, err := vtgateproxyProcInstance.GetMySQLConn("pool1", "use1-az1")
	require.NoError(t, err, "Failed to get MySQL connection")
	defer conn.Close()

	err = conn.Ping()
	require.NoError(t, err, "Failed to ping through proxy with identity verification")

	log.Info("Identity verification test passed - connection successful")
}

func TestIdentityVerificationDisabled(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Create a config with a valid vtgate
	config := []map[string]string{
		{
			"host":    "vtgate1", 
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(clusterInstance.VtgateProcess.GrpcPort),
			"http":    strconv.Itoa(clusterInstance.VtgateProcess.Port),
			"az_id":   "use1-az1",
			"type":    "pool1",
		},
	}
	
	vtgateproxyProcInstance := setupVtgateProxyWithIdentityVerification(t, config, false, "2s")
	defer vtgateproxyProcInstance.Teardown()

	// Wait for config to be processed
	err := vtgateproxyProcInstance.WaitForConfig(config, 30*time.Second)
	require.NoError(t, err, "Failed to wait for config")

	// Test that connection works with identity verification disabled
	conn, err := vtgateproxyProcInstance.GetMySQLConn("pool1", "use1-az1")
	require.NoError(t, err, "Failed to get MySQL connection")
	defer conn.Close()

	err = conn.Ping()
	require.NoError(t, err, "Failed to ping through proxy with identity verification disabled")

	log.Info("Identity verification disabled test passed - connection successful")
}

func TestIdentityVerificationWithUnhealthyVtgate(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Create a mock server that returns unhealthy status
	unhealthyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/debug/health") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("not ok")) // Unhealthy response
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer unhealthyServer.Close()

	// Extract host and port from the test server URL
	serverURL := unhealthyServer.URL[7:] // Remove "http://"
	parts := strings.Split(serverURL, ":")
	serverHost := parts[0]
	serverPort := parts[1]

	// Create config with both real vtgate and unhealthy mock server
	config := []map[string]string{
		{
			"host":    "vtgate1",
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(clusterInstance.VtgateProcess.GrpcPort),
			"http":    strconv.Itoa(clusterInstance.VtgateProcess.Port),
			"az_id":   "use1-az1", 
			"type":    "pool1",
		},
		{
			"host":    "unhealthy_vtgate",
			"address": serverHost,
			"grpc":    serverPort,
			"az_id":   "use1-az1",
			"type":    "pool1",
		},
	}

	vtgateproxyProcInstance := setupVtgateProxyWithIdentityVerification(t, config, true, "2s")
	defer vtgateproxyProcInstance.Teardown()

	// Wait a bit for discovery to process the config
	time.Sleep(5 * time.Second)

	// Check that only the healthy vtgate is in the target count (should be 1, not 2)
	vars, err := vtgateproxyProcInstance.GetVars()
	require.NoError(t, err, "Failed to get vars")

	targetCount, ok := vars["JsonDiscoveryTargetCount"]
	require.True(t, ok, "JsonDiscoveryTargetCount not found in vars")

	targets := targetCount.(map[string]any)
	pool1Count, exists := targets["pool1"]
	require.True(t, exists, "pool1 not found in target count")

	// Should only have 1 target (the healthy one), unhealthy one should be filtered out
	assert.Equal(t, float64(1), pool1Count, "Expected 1 healthy target, unhealthy should be filtered out")

	log.Info("Identity verification with unhealthy vtgate test passed - unhealthy target filtered out")
}

func TestIdentityVerificationTimeout(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Create a mock server that times out
	timeoutServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Sleep longer than the verification timeout
		time.Sleep(3 * time.Second)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer timeoutServer.Close()

	// Extract host and port from the test server URL
	serverURL := timeoutServer.URL[7:] // Remove "http://"
	parts := strings.Split(serverURL, ":")
	serverHost := parts[0]
	serverPort := parts[1]

	// Create config with real vtgate and slow mock server
	config := []map[string]string{
		{
			"host":    "vtgate1",
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(clusterInstance.VtgateProcess.GrpcPort),
			"http":    strconv.Itoa(clusterInstance.VtgateProcess.Port),
			"az_id":   "use1-az1",
			"type":    "pool1",
		},
		{
			"host":    "slow_vtgate",
			"address": serverHost,
			"grpc":    serverPort,
			"az_id":   "use1-az1", 
			"type":    "pool1",
		},
	}

	// Use short timeout (1s) so the slow server times out
	vtgateproxyProcInstance := setupVtgateProxyWithIdentityVerification(t, config, true, "1s")
	defer vtgateproxyProcInstance.Teardown()

	// Wait a bit for discovery to process the config
	time.Sleep(5 * time.Second)

	// Check that only the responsive vtgate is in the target count
	vars, err := vtgateproxyProcInstance.GetVars()
	require.NoError(t, err, "Failed to get vars")

	targetCount, ok := vars["JsonDiscoveryTargetCount"] 
	require.True(t, ok, "JsonDiscoveryTargetCount not found in vars")

	targets := targetCount.(map[string]any)
	pool1Count, exists := targets["pool1"]
	require.True(t, exists, "pool1 not found in target count")

	// Should only have 1 target (the responsive one), slow one should be filtered out
	assert.Equal(t, float64(1), pool1Count, "Expected 1 responsive target, slow target should timeout")

	log.Info("Identity verification timeout test passed - slow target filtered out")
}

func TestPoolVerificationWithIPReuse(t *testing.T) {
	defer cluster.PanicHandler(t)

	const targetAffinity = "use1-az1"
	const expectedPoolA = "poolA"
	const expectedPoolB = "poolB"

	// Start pool-specific vtgates with separate keyspaces - this simulates production
	pools, err := startPoolSpecificVtgates([]string{expectedPoolA, expectedPoolB})
	require.NoError(t, err, "Failed to start pool-specific vtgates")
	defer teardownPoolSpecificVtgates(pools)

	// Get the specific pool vtgates
	var poolAInfo, poolBInfo *PoolVtgateInfo
	for _, pool := range pools {
		if pool.PoolName == expectedPoolA {
			poolAInfo = pool
		} else if pool.PoolName == expectedPoolB {
			poolBInfo = pool
		}
	}
	require.NotNil(t, poolAInfo, "Failed to find poolA vtgate")
	require.NotNil(t, poolBInfo, "Failed to find poolB vtgate")

	// Insert test data into each pool's keyspace
	err = insertPoolSpecificData(poolAInfo)
	require.NoError(t, err, "Failed to insert test data for poolA")
	err = insertPoolSpecificData(poolBInfo)
	require.NoError(t, err, "Failed to insert test data for poolB")

	vtgateHostsFile := filepath.Join(clusterInstance.TmpDirectory, fmt.Sprintf("hosts_pool_verification_%d", time.Now().UnixNano()))

	// Create a STALE config that claims the poolB vtgate belongs to poolA
	// This simulates the IP reuse scenario where our config file is outdated
	staleConfig := []map[string]string{
		{
			"host":    "vtgate_poolA",
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(poolAInfo.VtgateProc.GrpcPort),
			"http":    strconv.Itoa(poolAInfo.VtgateProc.Port),
			"az_id":   targetAffinity,
			"type":    expectedPoolA, // This is correct - poolA vtgate configured for poolA
		},
		{
			"host":    "vtgate_reused_ip",
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(poolBInfo.VtgateProc.GrpcPort), // This vtgate actually serves poolB keyspace
			"http":    strconv.Itoa(poolBInfo.VtgateProc.Port),
			"az_id":   targetAffinity,
			"type":    expectedPoolA, // This is WRONG - config says poolA but vtgate serves poolB keyspace
		},
	}

	log.Infof("Setting up stale config claiming poolB vtgate (port %d) belongs to poolA", poolBInfo.VtgateProc.GrpcPort)

	// Setup vtgateproxy with identity verification enabled
	// Write the stale config to file
	b, err := json.Marshal(staleConfig)
	require.NoError(t, err, "Failed to marshal stale config")
	err = os.WriteFile(vtgateHostsFile, b, 0644)
	require.NoError(t, err, "Failed to write stale config file")

	// Get ports for vtgateproxy
	vtgateproxyHTTPPort := clusterInstance.GetAndReservePort()
	vtgateproxyGrpcPort := clusterInstance.GetAndReservePort()
	vtgateproxyMySQLPort := clusterInstance.GetAndReservePort()

	// Create vtgateproxy process with identity verification settings
	vtgateproxyProcInstance := NewVtgateProxyProcess(
		clusterInstance.TmpDirectory,
		vtgateHostsFile,
		targetAffinity,
		"round_robin",
		2, // Allow 2 connections to accommodate our test scenario
		vtgateproxyHTTPPort,
		vtgateproxyGrpcPort,
		vtgateproxyMySQLPort,
	)

	// Add identity verification flags
	vtgateproxyProcInstance.ExtraArgs = append(vtgateproxyProcInstance.ExtraArgs,
		"--enable_identity_verify=true",
		"--identity_verify_timeout=2s",
	)

	// Start the process
	err = vtgateproxyProcInstance.Setup()
	require.NoError(t, err, "Failed to setup vtgateproxy process")
	defer vtgateproxyProcInstance.Teardown()

	// Wait for initial config to be processed
	time.Sleep(10 * time.Second)

	// Check target distribution - the mismatched vtgate should be filtered out by identity verification
	vars, err := vtgateproxyProcInstance.GetVars()
	require.NoError(t, err, "Failed to get vars")

	targetCount, ok := vars["JsonDiscoveryTargetCount"]
	require.True(t, ok, "JsonDiscoveryTargetCount not found in vars")

	targets := targetCount.(map[string]any)
	log.Infof("Target distribution with identity verification: %+v", targets)

	// poolA should have only 1 target - the mismatched vtgate should be filtered out
	poolACount, exists := targets[expectedPoolA]
	require.True(t, exists, fmt.Sprintf("%s not found in target count", expectedPoolA))

	// With identity verification enabled, the mismatched vtgate should be filtered out
	// - poolA vtgate serves commerce_poolA keyspace -> should be accepted for poolA
	// - poolB vtgate serves commerce_poolB keyspace -> should be rejected when misconfigured as poolA
	// So we should only have 1 target (the correctly configured poolA vtgate)
	assert.Equal(t, float64(1), poolACount,
		fmt.Sprintf("Expected 1 target in %s - the mismatched vtgate (poolB) should be filtered out by Vitess shard-based identity verification", expectedPoolA))

	// Test that connection to poolA works with the remaining valid vtgate
	conn, err := vtgateproxyProcInstance.GetMySQLConn(expectedPoolA, targetAffinity)
	require.NoError(t, err, fmt.Sprintf("Failed to get MySQL connection to %s", expectedPoolA))
	defer conn.Close()

	err = conn.Ping()
	require.NoError(t, err, fmt.Sprintf("Failed to ping %s", expectedPoolA))

	log.Infof("Pool verification test passed - identity verification correctly filtered out mismatched vtgate using Vitess shards, %s has %v targets",
		expectedPoolA, poolACount)
}

// Helper function to setup vtgateproxy with identity verification settings
func setupVtgateProxyWithIdentityVerification(t *testing.T, config []map[string]string, enableVerify bool, timeout string) *VtgateProxyProcess {
	// Write config to file
	b, err := json.Marshal(config)
	require.NoError(t, err, "Failed to marshal config")
	
	vtgateHostsFile := filepath.Join(clusterInstance.TmpDirectory, fmt.Sprintf("hosts_%d", time.Now().UnixNano()))
	err = os.WriteFile(vtgateHostsFile, b, 0644)
	require.NoError(t, err, "Failed to write hosts file")

	// Get ports for vtgateproxy
	vtgateproxyHTTPPort := clusterInstance.GetAndReservePort()
	vtgateproxyGrpcPort := clusterInstance.GetAndReservePort() 
	vtgateproxyMySQLPort := clusterInstance.GetAndReservePort()

	// Create vtgateproxy process with identity verification settings
	vtgateproxyProcInstance := NewVtgateProxyProcess(
		clusterInstance.TmpDirectory,
		vtgateHostsFile,
		"use1-az1",
		"round_robin",
		2, // Allow 2 connections to accommodate our test scenario
		vtgateproxyHTTPPort,
		vtgateproxyGrpcPort,
		vtgateproxyMySQLPort,
	)

	// Add identity verification flags
	if enableVerify {
		vtgateproxyProcInstance.ExtraArgs = append(vtgateproxyProcInstance.ExtraArgs,
			"--enable_identity_verify=true",
			"--identity_verify_timeout="+timeout,
		)
	} else {
		vtgateproxyProcInstance.ExtraArgs = append(vtgateproxyProcInstance.ExtraArgs,
			"--enable_identity_verify=false",
		)
	}

	// Start the process
	err = vtgateproxyProcInstance.Setup()
	require.NoError(t, err, "Failed to setup vtgateproxy process")

	return vtgateproxyProcInstance
}

func TestSQLBasedIdentityVerificationEnabled(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Create a config with a valid vtgate
	config := []map[string]string{
		{
			"host":    "vtgate1",
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(clusterInstance.VtgateProcess.GrpcPort),
			"http":    strconv.Itoa(clusterInstance.VtgateProcess.Port),
			"az_id":   "use1-az1",
			"type":    "pool1",
		},
	}

	vtgateproxyProcInstance := setupVtgateProxyWithSQLIdentityVerification(t, config, true, "2s")
	defer vtgateproxyProcInstance.Teardown()

	// Wait for config to be processed
	err := vtgateproxyProcInstance.WaitForConfig(config, 30*time.Second)
	require.NoError(t, err, "Failed to wait for config")

	// Test that connection works with SQL-based identity verification enabled
	conn, err := vtgateproxyProcInstance.GetMySQLConn("pool1", "use1-az1")
	require.NoError(t, err, "Failed to get MySQL connection")
	defer conn.Close()

	err = conn.Ping()
	require.NoError(t, err, "Failed to ping through proxy with SQL identity verification")

	log.Info("SQL-based identity verification test passed - connection successful")
}

func TestSQLBasedIdentityVerificationWithPortMismatch(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Create a config that tries to use the HTTP port as the GRPC port
	// This should fail SQL verification since we can't make a MySQL connection to the HTTP port
	config := []map[string]string{
		{
			"host":    "vtgate1",
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(clusterInstance.VtgateProcess.GrpcPort),
			"http":    strconv.Itoa(clusterInstance.VtgateProcess.Port),
			"az_id":   "use1-az1",
			"type":    "pool1",
		},
		{
			"host":    "vtgate_wrong_port",
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(clusterInstance.VtgateProcess.Port), // HTTP port used as GRPC port
			"http":    strconv.Itoa(clusterInstance.VtgateProcess.Port),
			"az_id":   "use1-az1",
			"type":    "pool1",
		},
	}

	vtgateproxyProcInstance := setupVtgateProxyWithSQLIdentityVerification(t, config, true, "2s")
	defer vtgateproxyProcInstance.Teardown()

	// Wait a bit for discovery to process the config
	time.Sleep(5 * time.Second)

	// Check that only the properly configured vtgate is in the target count
	vars, err := vtgateproxyProcInstance.GetVars()
	require.NoError(t, err, "Failed to get vars")

	targetCount, ok := vars["JsonDiscoveryTargetCount"]
	require.True(t, ok, "JsonDiscoveryTargetCount not found in vars")

	targets := targetCount.(map[string]any)
	pool1Count, exists := targets["pool1"]
	require.True(t, exists, "pool1 not found in target count")

	// Should only have 1 target (the correctly configured one), the misconfigured one should be filtered out
	assert.Equal(t, float64(1), pool1Count, "Expected 1 properly configured target, misconfigured should be filtered out by SQL verification")

	log.Info("SQL-based identity verification with port mismatch test passed - misconfigured target filtered out")
}

func TestSQLVerificationVsDisabled(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Create the same config for both SQL verification enabled and disabled
	config := []map[string]string{
		{
			"host":    "vtgate1",
			"address": clusterInstance.Hostname,
			"grpc":    strconv.Itoa(clusterInstance.VtgateProcess.GrpcPort),
			"http":    strconv.Itoa(clusterInstance.VtgateProcess.Port),
			"az_id":   "use1-az1",
			"type":    "pool1",
		},
	}

	// Test with identity verification disabled
	disabledProxy := setupVtgateProxyWithSQLIdentityVerification(t, config, false, "2s")
	defer disabledProxy.Teardown()

	// Wait for disabled config to be processed
	err := disabledProxy.WaitForConfig(config, 30*time.Second)
	require.NoError(t, err, "Failed to wait for disabled verification config")

	// Test that disabled verification works
	disabledConn, err := disabledProxy.GetMySQLConn("pool1", "use1-az1")
	require.NoError(t, err, "Failed to get MySQL connection with disabled verification")
	defer disabledConn.Close()

	err = disabledConn.Ping()
	require.NoError(t, err, "Failed to ping through proxy with disabled verification")

	// Test SQL-based verification
	sqlProxy := setupVtgateProxyWithSQLIdentityVerification(t, config, true, "2s")
	defer sqlProxy.Teardown()

	// Wait for SQL config to be processed
	err = sqlProxy.WaitForConfig(config, 30*time.Second)
	require.NoError(t, err, "Failed to wait for SQL-based config")

	// Test that SQL verification works
	sqlConn, err := sqlProxy.GetMySQLConn("pool1", "use1-az1")
	require.NoError(t, err, "Failed to get MySQL connection with SQL verification")
	defer sqlConn.Close()

	err = sqlConn.Ping()
	require.NoError(t, err, "Failed to ping through proxy with SQL verification")

	log.Info("Both disabled and SQL verification methods work successfully")

	// Compare target counts - both should have the same number of targets since we're using valid vtgates
	disabledVars, err := disabledProxy.GetVars()
	require.NoError(t, err, "Failed to get disabled proxy vars")

	sqlVars, err := sqlProxy.GetVars()
	require.NoError(t, err, "Failed to get SQL proxy vars")

	disabledTargets := disabledVars["JsonDiscoveryTargetCount"].(map[string]any)
	sqlTargets := sqlVars["JsonDiscoveryTargetCount"].(map[string]any)

	assert.Equal(t, disabledTargets["pool1"], sqlTargets["pool1"],
		"Disabled and SQL verification should result in the same number of valid targets")

	log.Info("Disabled vs SQL verification comparison test passed - both methods produce consistent results for valid vtgates")
}

// Helper function to setup vtgateproxy with SQL-based identity verification settings
func setupVtgateProxyWithSQLIdentityVerification(t *testing.T, config []map[string]string, enableVerify bool, timeout string) *VtgateProxyProcess {
	// Write config to file
	b, err := json.Marshal(config)
	require.NoError(t, err, "Failed to marshal config")

	vtgateHostsFile := filepath.Join(clusterInstance.TmpDirectory, fmt.Sprintf("hosts_sql_%d", time.Now().UnixNano()))
	err = os.WriteFile(vtgateHostsFile, b, 0644)
	require.NoError(t, err, "Failed to write hosts file")

	// Get ports for vtgateproxy
	vtgateproxyHTTPPort := clusterInstance.GetAndReservePort()
	vtgateproxyGrpcPort := clusterInstance.GetAndReservePort()
	vtgateproxyMySQLPort := clusterInstance.GetAndReservePort()

	// Create vtgateproxy process with SQL-based identity verification settings
	vtgateproxyProcInstance := NewVtgateProxyProcess(
		clusterInstance.TmpDirectory,
		vtgateHostsFile,
		"use1-az1",
		"round_robin",
		2, // Allow 2 connections to accommodate our test scenario
		vtgateproxyHTTPPort,
		vtgateproxyGrpcPort,
		vtgateproxyMySQLPort,
	)

	// Add SQL-based identity verification flags
	if enableVerify {
		vtgateproxyProcInstance.ExtraArgs = append(vtgateproxyProcInstance.ExtraArgs,
			"--enable_identity_verify=true",
			"--identity_verify_timeout="+timeout,
			"--use_sql_verification=true", // Enable SQL-based verification
		)
	} else {
		vtgateproxyProcInstance.ExtraArgs = append(vtgateproxyProcInstance.ExtraArgs,
			"--enable_identity_verify=false",
			"--use_sql_verification=false",
		)
	}

	// Start the process
	err = vtgateproxyProcInstance.Setup()
	require.NoError(t, err, "Failed to setup vtgateproxy process")

	return vtgateproxyProcInstance
}