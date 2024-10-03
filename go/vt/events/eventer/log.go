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
