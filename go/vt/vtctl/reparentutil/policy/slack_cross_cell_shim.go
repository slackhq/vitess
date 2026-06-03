package policy

import "vitess.io/vitess/go/durability"

func init() {
	RegisterDurability("slack_cross_cell", func() Durabler {
		return &durability.SlackCrossCell{}
	})
}
