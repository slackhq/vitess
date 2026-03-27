/*
Copyright 2025 The Vitess Authors.

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

package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtorc/config"
)

// recoveriesCooldownSkipped counts the number of cluster-wide recoveries skipped due to shard cooldown
var recoveriesCooldownSkipped = stats.NewCountersWithMultiLabels("RecoveriesCooldownSkipped", "Count of cluster-wide recoveries skipped due to shard cooldown", recoveriesCounterLabels)

const recoveryCooldownKeyPrefix = "recovery_cooldown"

type recoveryCooldownMarker struct {
	Timestamp    time.Time `json:"timestamp"`
	AnalysisCode string    `json:"analysis_code"`
	RecoveryName string    `json:"recovery_name"`
	InstanceID   string    `json:"instance_id"`
}

func recoveryCooldownKey(keyspace, shard string) string {
	return fmt.Sprintf("%s/%s/%s", recoveryCooldownKeyPrefix, keyspace, shard)
}

// checkRecoveryCooldown returns true if a cooldown is active for the shard.
func checkRecoveryCooldown(ctx context.Context, keyspace, shard string) (bool, error) {
	cooldownDuration := config.GetRecoveryCooldownDuration()
	if cooldownDuration <= 0 {
		return false, nil
	}

	key := recoveryCooldownKey(keyspace, shard)
	val, err := ts.GetSingleMetadata(ctx, key)
	if err != nil {
		return false, fmt.Errorf("failed to check recovery cooldown for %s/%s: %w", keyspace, shard, err)
	}
	if val == "" {
		return false, nil
	}

	var marker recoveryCooldownMarker
	if err := json.Unmarshal([]byte(val), &marker); err != nil {
		log.Warningf("Corrupted recovery cooldown marker for %s/%s, ignoring: %v", keyspace, shard, err)
		return false, nil
	}

	if time.Since(marker.Timestamp) >= cooldownDuration {
		return false, nil
	}

	return true, nil
}

// writeRecoveryCooldown writes a cooldown marker after a successful cluster-wide recovery.
func writeRecoveryCooldown(keyspace, shard, analysisCode, recoveryName string) {
	if config.GetRecoveryCooldownDuration() <= 0 {
		return
	}

	hostname, _ := os.Hostname()
	marker := recoveryCooldownMarker{
		Timestamp:    time.Now(),
		AnalysisCode: analysisCode,
		RecoveryName: recoveryName,
		InstanceID:   hostname,
	}
	data, err := json.Marshal(marker)
	if err != nil {
		log.Errorf("Failed to marshal recovery cooldown marker: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ts.UpsertMetadata(ctx, recoveryCooldownKey(keyspace, shard), string(data)); err != nil {
		log.Errorf("Failed to write recovery cooldown marker for %s/%s: %v", keyspace, shard, err)
	}
}
