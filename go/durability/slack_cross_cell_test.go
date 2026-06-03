package durability

import (
	"testing"

	"github.com/stretchr/testify/assert"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

func TestSlackCrossCellPromotionRule(t *testing.T) {
	t.Parallel()
	scc := &SlackCrossCell{}

	t.Run("PRIMARY", func(t *testing.T) {
		assert.Equal(t, promotionrule.Neutral, scc.PromotionRule(&topodatapb.Tablet{
			Hostname: t.Name(),
			Type:     topodatapb.TabletType_PRIMARY,
			Tags: map[string]string{
				TabletTagPromotionRule: string(promotionrule.MustNot), // tag should be ignored on PRIMARY
			},
		}))
	})

	t.Run("REPLICA", func(t *testing.T) {
		assert.Equal(t, promotionrule.Neutral, scc.PromotionRule(&topodatapb.Tablet{
			Hostname: t.Name(),
			Type:     topodatapb.TabletType_REPLICA,
		}))
	})

	t.Run("REPLICA_invalid_override_tag", func(t *testing.T) {
		assert.Equal(t, promotionrule.Neutral, scc.PromotionRule(&topodatapb.Tablet{
			Hostname: t.Name(),
			Type:     topodatapb.TabletType_REPLICA,
			Tags: map[string]string{
				TabletTagPromotionRule: "invalid-rule", // should be ignored
			},
		}))
	})

	t.Run("REPLICA_valid_override_tag", func(t *testing.T) {
		assert.Equal(t, promotionrule.PreferNot, scc.PromotionRule(&topodatapb.Tablet{
			Hostname: t.Name(),
			Type:     topodatapb.TabletType_REPLICA,
			Tags: map[string]string{
				TabletTagPromotionRule: string(promotionrule.PreferNot),
			},
		}))
	})

	t.Run("DRAINED_valid_override_tag_ignored", func(t *testing.T) {
		assert.Equal(t, promotionrule.MustNot, scc.PromotionRule(&topodatapb.Tablet{
			Hostname: t.Name(),
			Type:     topodatapb.TabletType_DRAINED,
			Tags: map[string]string{
				TabletTagPromotionRule: string(promotionrule.PreferNot),
			},
		}))
	})

	// test 'must_not' tablet types
	for _, tabletType := range []topodatapb.TabletType{
		topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_DRAINED,
		topodatapb.TabletType_BACKUP,
		topodatapb.TabletType_RESTORE,
	} {
		t.Run(tabletType.String(), func(t *testing.T) {
			assert.Equal(t, promotionrule.MustNot, scc.PromotionRule(&topodatapb.Tablet{
				Hostname: t.Name(),
				Type:     tabletType,
			}))
		})
	}
}

func TestSlackCrossCellSemiSyncAckers(t *testing.T) {
	t.Parallel()
	scc := &SlackCrossCell{}

	t.Run("default", func(t *testing.T) {
		assert.Equal(t, defaultSemiSyncAckers, scc.SemiSyncAckers(&topodatapb.Tablet{
			Hostname: t.Name(),
		}))
	})

	t.Run("invalid_override_tag1", func(t *testing.T) {
		assert.Equal(t, defaultSemiSyncAckers, scc.SemiSyncAckers(&topodatapb.Tablet{
			Hostname: t.Name(),
			Tags: map[string]string{
				TabletTagSemiSyncAckers: "0",
			},
		}))
	})

	t.Run("invalid_override_tag2", func(t *testing.T) {
		assert.Equal(t, defaultSemiSyncAckers, scc.SemiSyncAckers(&topodatapb.Tablet{
			Hostname: t.Name(),
			Tags: map[string]string{
				TabletTagSemiSyncAckers: "not-a-number",
			},
		}))
	})

	t.Run("valid_override_tag", func(t *testing.T) {
		assert.Equal(t, 3, scc.SemiSyncAckers(&topodatapb.Tablet{
			Hostname: t.Name(),
			Tags: map[string]string{
				TabletTagSemiSyncAckers: "3",
			},
		}))
	})
}

func TestSlackCrossCellIsReplicaSemiSync(t *testing.T) {
	t.Parallel()
	scc := &SlackCrossCell{}

	primary := &topodatapb.Tablet{
		Hostname: "primary",
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  123,
		},
		Type: topodatapb.TabletType_PRIMARY,
	}

	t.Run("cross_cell", func(t *testing.T) {
		assert.True(t, scc.IsReplicaSemiSync(primary, &topodatapb.Tablet{
			Hostname: t.Name(),
			Alias: &topodatapb.TabletAlias{
				Cell: "cell2",
				Uid:  1234,
			},
			Type: topodatapb.TabletType_REPLICA,
		}))
	})

	t.Run("cross_cell_override_tag", func(t *testing.T) {
		assert.False(t, scc.IsReplicaSemiSync(primary, &topodatapb.Tablet{
			Hostname: t.Name(),
			Alias: &topodatapb.TabletAlias{
				Cell: "cell2",
				Uid:  1234,
			},
			Type: topodatapb.TabletType_REPLICA,
			Tags: map[string]string{
				TabletTagSemiSyncNeverAck: "true",
			},
		}))
	})

	t.Run("cross_cell_override_tag_false", func(t *testing.T) {
		assert.True(t, scc.IsReplicaSemiSync(primary, &topodatapb.Tablet{
			Hostname: t.Name(),
			Alias: &topodatapb.TabletAlias{
				Cell: "cell2",
				Uid:  1234,
			},
			Type: topodatapb.TabletType_REPLICA,
			Tags: map[string]string{
				TabletTagSemiSyncNeverAck: "false",
			},
		}))
	})

	t.Run("same_cell", func(t *testing.T) {
		assert.False(t, scc.IsReplicaSemiSync(primary, &topodatapb.Tablet{
			Hostname: t.Name(),
			Alias: &topodatapb.TabletAlias{
				Cell: primary.Alias.Cell,
				Uid:  12345,
			},
			Type: topodatapb.TabletType_REPLICA,
		}))
	})
}

func TestSlackCrossCellHasSemiSync(t *testing.T) {
	t.Parallel()
	scc := &SlackCrossCell{}

	assert.True(t, scc.HasSemiSync())
}
