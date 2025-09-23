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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
)

func TestVerifyVTGateIdentityDisabledVsSQL(t *testing.T) {
	tests := []struct {
		name                 string
		enableIdentityVerify bool
		useSQLVerification   bool
		expectedResult       bool
	}{
		{
			name:                 "identity verification disabled",
			enableIdentityVerify: false,
			useSQLVerification:   false,
			expectedResult:       true, // Should always pass when disabled
		},
		{
			name:                 "SQL verification enabled but will fail on invalid address",
			enableIdentityVerify: true,
			useSQLVerification:   true,
			expectedResult:       false, // Should fail since we can't connect to invalid address
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create resolver builder with test settings
			builder := &JSONGateResolverBuilder{
				enableIdentityVerify: tt.enableIdentityVerify,
				useSQLVerification:   tt.useSQLVerification,
				verifyTimeout:        1 * time.Second,
			}

			ctx := context.Background()
			got := builder.verifyVTGateIdentity(ctx, "invalid:9999", "test_pool")

			if got != tt.expectedResult {
				t.Errorf("verifyVTGateIdentity() = %v, want %v", got, tt.expectedResult)
			}
		})
	}
}

func TestVerifyVTGateIdentityDisabled(t *testing.T) {
	// Create resolver builder with identity verification disabled
	builder := &JSONGateResolverBuilder{
		enableIdentityVerify: false,
		verifyTimeout:        1 * time.Second,
	}

	ctx := context.Background()
	// Should return true when verification is disabled, regardless of address
	got := builder.verifyVTGateIdentity(ctx, "invalid:9999", "any_pool")

	if !got {
		t.Errorf("verifyVTGateIdentity() with disabled verification = %v, want true", got)
	}
}

func TestVerifyVTGateSQLIdentityTimeout(t *testing.T) {
	// Test SQL verification timeout behavior
	builder := &JSONGateResolverBuilder{
		enableIdentityVerify: true,
		useSQLVerification:   true,
		verifyTimeout:        1 * time.Millisecond, // Very short timeout
	}

	ctx := context.Background()

	// Should fail due to timeout when trying to connect to non-existent MySQL server
	got := builder.verifyVTGateIdentity(ctx, "127.0.0.1:9999", "vifl")

	if got {
		t.Errorf("verifyVTGateIdentity() with SQL timeout = %v, want false", got)
	}
}

// Mock MySQL connection for testing SQL-based verification
type mockMySQLConn struct {
	hostname      string
	shouldFail    bool
	queryFailures map[string]bool
}

func (m *mockMySQLConn) ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock connection failure")
	}

	if m.queryFailures != nil && m.queryFailures[query] {
		return nil, fmt.Errorf("mock query failure for: %s", query)
	}

	switch query {
	case "SELECT 1":
		return &sqltypes.Result{
			Rows: [][]sqltypes.Value{{sqltypes.NewVarChar("1")}},
		}, nil
	case "SELECT @@hostname":
		return &sqltypes.Result{
			Rows: [][]sqltypes.Value{{sqltypes.NewVarChar(m.hostname)}},
		}, nil
	case "SHOW VARIABLES LIKE 'hostname'":
		return &sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarChar("hostname"), sqltypes.NewVarChar(m.hostname)},
			},
		}, nil
	case "SELECT HOST_NAME() as hostname":
		return &sqltypes.Result{
			Rows: [][]sqltypes.Value{{sqltypes.NewVarChar(m.hostname)}},
		}, nil
	case "SHOW STATUS LIKE 'Hostname'":
		return &sqltypes.Result{
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarChar("Hostname"), sqltypes.NewVarChar(m.hostname)},
			},
		}, nil
	}

	return &sqltypes.Result{}, fmt.Errorf("unexpected query: %s", query)
}

func (m *mockMySQLConn) Close() {
	// no-op for mock
}

func TestVerifyVTGateSQLIdentity(t *testing.T) {
	tests := []struct {
		name         string
		hostname     string
		expectedPool string
		want         bool
	}{
		{
			name:         "successful verification - vifl pool",
			hostname:     "vtgate-bedrock-vifl-dev-iad-c8b75c8c8-rb6cw",
			expectedPool: "vifl",
			want:         true,
		},
		{
			name:         "successful verification - interop pool",
			hostname:     "vtgate-bedrock-interop-dev-iad-57b5795c8b-72zpr",
			expectedPool: "interop",
			want:         true,
		},
		{
			name:         "pool mismatch - hostname indicates different pool",
			hostname:     "vtgate-bedrock-vifl-dev-iad-c8b75c8c8-rb6cw",
			expectedPool: "interop",
			want:         false,
		},
		{
			name:         "test environment - localhost hostname",
			hostname:     "localhost",
			expectedPool: "vifl",
			want:         false, // Should return empty pool for localhost
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the hostname extraction logic directly
			actualPool := extractPoolFromHostname(tt.hostname)
			got := (actualPool == tt.expectedPool)

			if got != tt.want {
				t.Errorf("SQL identity verification result = %v, want %v (hostname: %s, expected: %s, actual: %s)",
					got, tt.want, tt.hostname, tt.expectedPool, actualPool)
			}
		})
	}
}

func TestSQLBasicHealthCheck(t *testing.T) {
	tests := []struct {
		name       string
		shouldFail bool
		want       bool
	}{
		{
			name:       "successful health check",
			shouldFail: false,
			want:       true,
		},
		{
			name:       "failed health check",
			shouldFail: true,
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockConn := &mockMySQLConn{
				shouldFail: tt.shouldFail,
			}

			// Test the mock connection directly
			result, err := mockConn.ExecuteFetch("SELECT 1", 1, false)

			if tt.shouldFail {
				if err == nil {
					t.Errorf("Expected error for failed health check, got result: %v", result)
				}
			} else {
				if err != nil {
					t.Errorf("Expected successful health check, got error: %v", err)
				}
				if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
					t.Errorf("Unexpected health check result: %v", result)
				}
			}
		})
	}
}

func TestSQLGetHostname(t *testing.T) {
	tests := []struct {
		name          string
		hostname      string
		queryFailures map[string]bool
		want          string
		wantError     bool
	}{
		{
			name:      "successful hostname retrieval via @@hostname",
			hostname:  "vtgate-bedrock-vifl-dev-iad-c8b75c8c8-rb6cw",
			want:      "vtgate-bedrock-vifl-dev-iad-c8b75c8c8-rb6cw",
			wantError: false,
		},
		{
			name:     "@@hostname fails, fallback to SHOW VARIABLES",
			hostname: "vtgate-bedrock-interop-dev-iad-57b5795c8b-72zpr",
			queryFailures: map[string]bool{
				"SELECT @@hostname": true,
			},
			want:      "vtgate-bedrock-interop-dev-iad-57b5795c8b-72zpr",
			wantError: false,
		},
		{
			name:     "all queries fail",
			hostname: "any-hostname",
			queryFailures: map[string]bool{
				"SELECT @@hostname":              true,
				"SHOW VARIABLES LIKE 'hostname'": true,
				"SELECT HOST_NAME() as hostname": true,
				"SHOW STATUS LIKE 'Hostname'":    true,
			},
			want:      "",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockConn := &mockMySQLConn{
				hostname:      tt.hostname,
				queryFailures: tt.queryFailures,
			}

			// Test hostname retrieval logic by directly calling the mock
			// Since we can't easily inject the mock into the builder, we test the query logic separately
			var got string
			var err error

			// Test @@hostname query
			if tt.queryFailures == nil || !tt.queryFailures["SELECT @@hostname"] {
				result, queryErr := mockConn.ExecuteFetch("SELECT @@hostname", 1, false)
				if queryErr == nil && len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
					got = result.Rows[0][0].ToString()
				} else {
					// Try fallback queries
					for _, query := range []string{"SHOW VARIABLES LIKE 'hostname'", "SELECT HOST_NAME() as hostname", "SHOW STATUS LIKE 'Hostname'"} {
						if tt.queryFailures != nil && tt.queryFailures[query] {
							continue
						}
						result, queryErr = mockConn.ExecuteFetch(query, 10, false)
						if queryErr == nil {
							if strings.Contains(query, "SHOW") {
								for _, row := range result.Rows {
									if len(row) >= 2 && strings.ToLower(row[0].ToString()) == "hostname" {
										got = row[1].ToString()
										break
									}
								}
							} else if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
								got = result.Rows[0][0].ToString()
							}
							if got != "" {
								break
							}
						}
					}
					if got == "" {
						err = fmt.Errorf("failed to retrieve hostname using any method")
					}
				}
			} else {
				err = fmt.Errorf("@@hostname query failed")
			}

			if tt.wantError {
				if err == nil {
					t.Errorf("Expected error, got hostname: %s", got)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if got != tt.want {
					t.Errorf("hostname retrieval = %s, want %s", got, tt.want)
				}
			}
		})
	}
}

func TestExtractPoolFromHostname(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
		want     string
	}{
		{
			name:     "vifl pool hostname",
			hostname: "vtgate-bedrock-vifl-dev-iad-c8b75c8c8-rb6cw",
			want:     "vifl",
		},
		{
			name:     "interop pool hostname",
			hostname: "vtgate-bedrock-interop-dev-iad-57b5795c8b-72zpr",
			want:     "interop",
		},
		{
			name:     "mainteam pool hostname",
			hostname: "vtgate-bedrock-mainteam-dev-iad-7bbc44998f-p6ghr",
			want:     "mainteam",
		},
		{
			name:     "loadtest pool hostname",
			hostname: "vtgate-bedrock-loadtest-dev-iad-9df96bfc7-4d5td",
			want:     "loadtest",
		},
		{
			name:     "localhost - test environment",
			hostname: "localhost",
			want:     "",
		},
		{
			name:     "invalid hostname format",
			hostname: "invalid-hostname-format",
			want:     "",
		},
		{
			name:     "IP address",
			hostname: "127.0.0.1",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPoolFromHostname(tt.hostname)
			if got != tt.want {
				t.Errorf("extractPoolFromHostname(%s) = %s, want %s", tt.hostname, got, tt.want)
			}
		})
	}
}

func TestVerifyVTGateIdentitySQL(t *testing.T) {
	// Test SQL verification behavior with different configurations
	tests := []struct {
		name           string
		useSQLVerify   bool
		expectedResult bool
	}{
		{
			name:           "SQL-based verification with invalid address",
			useSQLVerify:   true,
			expectedResult: false, // Should fail since we can't connect to invalid address
		},
		{
			name:           "Non-SQL verification disabled",
			useSQLVerify:   false,
			expectedResult: false, // Should fail since HTTP endpoints aren't accessible in production
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := &JSONGateResolverBuilder{
				enableIdentityVerify: true,
				useSQLVerification:   tt.useSQLVerify,
				verifyTimeout:        1 * time.Second,
				targets: map[string][]targetHost{
					"vifl": {{HTTPAddr: "127.0.0.1:8080", PoolType: "vifl"}},
				},
			}

			ctx := context.Background()
			// Use an invalid address that we know will fail
			got := builder.verifyVTGateIdentity(ctx, "127.0.0.1:9999", "vifl")

			if got != tt.expectedResult {
				t.Errorf("verifyVTGateIdentity() = %v, want %v", got, tt.expectedResult)
			}
		})
	}
}