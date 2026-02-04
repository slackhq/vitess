package policy

import "github.com/slackhq/vitess-addons/go/durability"

// slackCrossCellWrapper wraps durability.SlackCrossCell to implement the Durabler interface
type slackCrossCellWrapper struct {
	*durability.SlackCrossCell
}

func (w *slackCrossCellWrapper) HasSemiSync() bool {
	return true
}

func init() {
	RegisterDurability("slack_cross_cell", func() Durabler {
		return &slackCrossCellWrapper{
			SlackCrossCell: &durability.SlackCrossCell{},
		}
	})
}
