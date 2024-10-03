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
	"fmt"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/events"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var eventerName string

func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringVar(&eventerName, "eventer", eventerName, "the eventer to be used to broadcast internal events")
}

type Eventer interface {
	DeleteTablet(ev *events.DeleteTabletEvent)
	EmergencyReparentShard(ev *events.EmergencyReparentShardEvent)
	PlannedReparentShard(ev *events.PlannedReparentShardEvent)
}

type NewEventer func() (Eventer, error)

var eventers = make(map[string]NewEventer)

func RegisterEventer(name string, eventerFunc NewEventer) {
	if eventers[name] != nil {
		log.Fatalf("eventer %v already registered", name)
	}
	eventers[name] = eventerFunc
}

func Get() (Eventer, error) {
	if eventerFunc, ok := eventers[eventerName]; ok {
		return eventerFunc()
	}
	return nil, fmt.Errorf("no eventer %v registered", eventerName)
}

func init() {
	servenv.OnParse(func(fs *pflag.FlagSet) {
		RegisterFlags(fs)
	})
}
