package eventer

import (
	"vitess.io/vitess/go/vt/events"
	"vitess.io/vitess/go/vt/log"
)

type LogEventer struct{}

func NewLogEventer() (Eventer, error) {
	return &LogEventer{}, nil
}

func (le *LogEventer) DeleteTablet(ev *events.DeleteTabletEvent) {
	log.Infof("Received DeleteTabletEvent: %v", ev)
}

func (le *LogEventer) EmergencyReparentShard(ev *events.EmergencyReparentShardEvent) {
	log.Infof("Received EmergencyReparentShardEvent: %v", ev)
}

func (le *LogEventer) PlannedReparentShard(ev *events.PlannedReparentShardEvent) {
	log.Infof("Received PlannedReparentShardEvent: %v", ev)
}

func init() {
	RegisterEventer("log", NewLogEventer)
}
