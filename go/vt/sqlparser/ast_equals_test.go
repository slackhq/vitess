package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCountStarEqualsRefOfCountStar(t *testing.T) {
	{
		a := &CountStar{}
		b := &CountStar{}
		assert.True(t, EqualsRefOfCountStar(a, b))
	}
	{
		a := &CountStar{Name: "a"}
		b := &CountStar{Name: "a"}
		assert.True(t, EqualsRefOfCountStar(a, b))
	}
	{
		a := &CountStar{Name: "a"}
		b := &CountStar{Name: "b"}
		assert.False(t, EqualsRefOfCountStar(a, b))
	}
	{
		a := &CountStar{Name: "a"}
		assert.False(t, EqualsRefOfCountStar(a, nil))
	}
}
