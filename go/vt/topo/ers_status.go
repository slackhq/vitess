/*
Copyright 2026 The Vitess Authors.

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

package topo

import (
	"context"
	"encoding/json"
	"path"
)

// ERSStatus represents the status of the most recent EmergencyReparentShard
// operation on a given shard. It is stored in the global topo under
// keyspaces/{keyspace}/shards/{shard}/PreviousERSStatus.
type ERSStatus struct {
	Identity  string `json:"identity"`  // hostname:port of the vtorc that performed/is performing the ERS
	Timestamp string `json:"timestamp"` // RFC3339 timestamp
	Status    string `json:"status"`    // completed, errored_shard_unchanged, errored_shard_unknown
}

const (
	ERSStatusCompleted            = "completed"
	ERSStatusErroredShardUnchanged = "errored_shard_unchanged"
	ERSStatusErroredShardUnknown   = "errored_shard_unknown"
)

func ersStatusFilePath(keyspace, shard string) string {
	return path.Join(KeyspacesPath, keyspace, ShardsPath, shard, PreviousERSStatusFile)
}

// UpdateERSStatus writes the ERS status for the given keyspace/shard to the global topo.
// It performs an unconditional upsert (nil version).
func (ts *Server) UpdateERSStatus(ctx context.Context, keyspace, shard string, status *ERSStatus) error {
	data, err := json.Marshal(status)
	if err != nil {
		return err
	}
	filePath := ersStatusFilePath(keyspace, shard)
	_, err = ts.globalCell.Update(ctx, filePath, data, nil)
	return err
}

// GetERSStatus reads the ERS status for the given keyspace/shard from the global topo.
// Returns nil, nil, nil if no status has been written yet.
func (ts *Server) GetERSStatus(ctx context.Context, keyspace, shard string) (*ERSStatus, Version, error) {
	filePath := ersStatusFilePath(keyspace, shard)
	data, version, err := ts.globalCell.Get(ctx, filePath)
	if err != nil {
		if IsErrType(err, NoNode) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	status := &ERSStatus{}
	if err := json.Unmarshal(data, status); err != nil {
		return nil, nil, err
	}
	return status, version, nil
}
