package policy

import (
	"github.com/slackhq/vitess-addons/go/durability"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

// slackCrossCellWrapper wraps the vitess-addons SlackCrossCell type
// to add the HasSemiSync method required by the Durabler interface.
type slackCrossCellWrapper struct {
	*durability.SlackCrossCell
}

func (w *slackCrossCellWrapper) PromotionRule(tablet *topodatapb.Tablet) promotionrule.CandidatePromotionRule {
	return w.SlackCrossCell.PromotionRule(tablet)
}

func (w *slackCrossCellWrapper) SemiSyncAckers(tablet *topodatapb.Tablet) int {
	return w.SlackCrossCell.SemiSyncAckers(tablet)
}

func (w *slackCrossCellWrapper) IsReplicaSemiSync(primary, replica *topodatapb.Tablet) bool {
	return w.SlackCrossCell.IsReplicaSemiSync(primary, replica)
}

// HasSemiSync returns true since SlackCrossCell uses semi-sync.
func (w *slackCrossCellWrapper) HasSemiSync() bool {
	// SlackCrossCell is based on cross-cell durability which uses semi-sync
	return true
}

func init() {
	RegisterDurability("slack_cross_cell", func() Durabler {
		return &slackCrossCellWrapper{
			SlackCrossCell: &durability.SlackCrossCell{},
		}
	})
}
