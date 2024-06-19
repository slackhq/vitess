package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCountStarFormat(t *testing.T) {
	{
		buf := NewTrackedBuffer(nil)
		node := &CountStar{}
		node.Format(buf)
		assert.Equal(t, &ParsedQuery{Query: "count(*)"}, buf.ParsedQuery())
	}
	{
		buf := NewTrackedBuffer(nil)
		node := &CountStar{Name: "Count"}
		node.Format(buf)
		assert.Equal(t, &ParsedQuery{Query: "Count(*)"}, buf.ParsedQuery())
	}
	{
		buf := NewTrackedBuffer(nil)
		node := &CountStar{Name: "COUNT"}
		node.Format(buf)
		assert.Equal(t, &ParsedQuery{Query: "COUNT(*)"}, buf.ParsedQuery())
	}
}
