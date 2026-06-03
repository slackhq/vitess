package durability

import (
	"strconv"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

const (
	defaultSemiSyncAckers = 1

	// TabletTagPromotionRule is a tablet-tag used to override the default promotion rule of a REPLICA tablet.
	// This can be used to make a tablet ineligible for promotion during a reparent.
	TabletTagPromotionRule = "promotion_rule"
	// TabletTagSemiSyncAckers is a tablet-tag used to override the default semi-sync ackers required by a PRIMARY tablet.
	// This can be used to increase the number of semi-sync ackers.
	TabletTagSemiSyncAckers = "semi_sync_ackers"
	// TabletTagSemiSyncNeverAck is a tablet-tag used to ensure a PRIMARY or REPLICA tablet will never become a semi-sync acker.
	// This tag can introduce availability risks. Consider using a different tablet type instead.
	TabletTagSemiSyncNeverAck = "semi_sync_never_ack"
)

// SlackCrossCell is a copy of the builtin "cross_cell" Durability Policy with the addition of
// tablet-tag-based overrides. The policy only allows Primary and Replica type servers from a
// different cell to acknowledge semi sync. This means that a transaction must be in two cells
// for it to be acknowledged. By default it returns NeutralPromoteRule for Primary and Replica
// tablet types, MustNotPromoteRule for everything else.
type SlackCrossCell struct{}

// PromotionRule implements the Durabler interfacer. The logic duplicates the built-in "cross_cell"
// policy unless a valid override is set using the tablet-tag: "promotion_rule".
func (d *SlackCrossCell) PromotionRule(tablet *topodatapb.Tablet) promotionrule.CandidatePromotionRule {
	switch tablet.Type {
	case topodatapb.TabletType_PRIMARY:
		return promotionrule.Neutral
	case topodatapb.TabletType_REPLICA:
		if tagValue := tablet.Tags[TabletTagPromotionRule]; tagValue != "" {
			if promotionRule, err := promotionrule.Parse(tagValue); err == nil {
				return promotionRule
			} else {
				log.Errorf("failed to parse promotion rule tablet tag: %v", err)
			}
		}
		return promotionrule.Neutral
	}
	return promotionrule.MustNot
}

// SemiSyncAckers implements the Durabler interface. The logic duplicates
// the built-in "cross_cell" policy unless a valid override is set using
// the tablet-tag: "semi_sync_ackers".
func (d *SlackCrossCell) SemiSyncAckers(tablet *topodatapb.Tablet) int {
	if tagValue := tablet.Tags[TabletTagSemiSyncAckers]; tagValue != "" {
		if ackers, err := strconv.Atoi(tagValue); err == nil {
			if ackers > 0 {
				return ackers
			}
		} else {
			log.Errorf("failed to parse semi-sync ackers tablet tag: %v", err)
		}
	}
	return defaultSemiSyncAckers
}

// IsReplicaSemiSync implements the Durabler interface. The logic duplicates the built-in
// "cross_cell" policy unless overridden by the tablet-tag "semi_sync_never_ack" being
// set to "true".
func (d *SlackCrossCell) IsReplicaSemiSync(primary, replica *topodatapb.Tablet) bool {
	switch replica.Type {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA:
		// WARNING: this tablet tag could lead to availability risks. Deploy this tag with caution!
		if tagValue := replica.Tags[TabletTagSemiSyncNeverAck]; tagValue == "true" {
			return false
		}
		return primary.Alias.Cell != replica.Alias.Cell
	}
	return false
}

// HasSemiSync returns whether the durability policy uses semi-sync.
func (d *SlackCrossCell) HasSemiSync() bool {
	return true
}
