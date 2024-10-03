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
