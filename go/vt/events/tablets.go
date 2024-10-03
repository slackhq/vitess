package events

import (
	"time"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// DeleteTabletEvent represents a DeleteTablet event from vtctl.
type DeleteTabletEvent struct {
	Source Source              `json:"source"`
	Time   time.Time           `json:"time"`
	Tablet *topodatapb.Tablet `json:"tablet"`
	Error  error               `json:"error"`
}
