package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTabletTagsFromPosArgs(t *testing.T) {
	t.Parallel()

	{
		tags, err := TabletTagsFromPosArgs([]string{"fail"})
		assert.Error(t, err)
		assert.Nil(t, tags)
	}
	{
		tags, err := TabletTagsFromPosArgs([]string{"hello=world"})
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{
			"hello": "world",
		}, tags)
	}
	{
		tags, err := TabletTagsFromPosArgs([]string{"hello=world", "test=123"})
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{
			"hello": "world",
			"test":  "123",
		}, tags)
	}
}
