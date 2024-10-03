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
