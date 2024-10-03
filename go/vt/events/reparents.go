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

package events

import (
	"time"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

type EmergencyReparentShardEvent struct {
	Source     Source             `json:"source"`
	Time       time.Time          `json:"time"`
	ShardInfo  topo.ShardInfo     `json:"shard_info"`
	OldPrimary *topodatapb.Tablet `json:"old_primary"`
	NewPrimary *topodatapb.Tablet `json:"new_primary"`
	Error      error              `json:"error"`
}

type PlannedReparentShardEvent struct {
	Source     Source             `json:"source"`
	Time       time.Time          `json:"time"`
	ShardInfo  topo.ShardInfo     `json:"shard_info"`
	OldPrimary *topodatapb.Tablet `json:"old_primary"`
	NewPrimary *topodatapb.Tablet `json:"new_primary"`
	Error      error              `json:"error"`
}
