package reparentutil

import (
	slackdur "github.com/slackhq/vitess-addons/go/durability/cross_cell"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

// slackCrossCell is a wrapper-struct that wraps the SlackCrossCell Durabler-interface
// implementation (from github.com/slackhq/vitess-addons) using the private struct
// methods the Durabiler interface (unfortunately) requires.
type slackCrossCell struct {
	*slackdur.SlackCrossCell
}

func (scc *slackCrossCell) promotionRule(tablet *topodatapb.Tablet) promotionrule.CandidatePromotionRule {
	return scc.PromotionRule(tablet)
}

func (scc *slackCrossCell) semiSyncAckers(tablet *topodatapb.Tablet) int {
	return scc.SemiSyncAckers(tablet)
}

func (scc *slackCrossCell) isReplicaSemiSync(primary, replica *topodatapb.Tablet) bool {
	return scc.IsReplicaSemiSync(primary, replica)
}

func init() {
	RegisterDurability("slack_cross_cell", func() Durabler {
		return &slackCrossCell{}
	})
}
