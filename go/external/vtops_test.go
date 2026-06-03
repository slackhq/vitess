package external

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExecCommand(t *testing.T) {
	t.Parallel()

	t.Run("no-binary-path", func(t *testing.T) {
		e := NewExecVTOps()
		e.Service = t.Name()
		assert.NoError(t, e.execCommand("test", ""))
		assert.Empty(t, statsVtopsFailedExecutions.Counts())
		assert.Empty(t, statsVtopsSuccessfulExecutions.Counts())
	})

	t.Run("success", func(t *testing.T) {
		defer statsVtopsSuccessfulExecutions.ResetAll()

		e := NewExecVTOps()
		e.Service = t.Name()
		e.BinaryPath = "/bin/echo"
		e.outputHandler = func(stderr, stdout *bytes.Buffer) {
			assert.Zero(t, stderr.Len())
			assert.Equal(t, "hello world!", strings.TrimSpace(stdout.String()))
		}
		assert.NoError(t, e.execCommand("test", "hello world!"))
		assert.Equal(t, map[string]int64{"test": 1}, statsVtopsSuccessfulExecutions.Counts())
		assert.Empty(t, statsVtopsFailedExecutions.Counts())
	})

	t.Run("does-not-exist", func(t *testing.T) {
		defer statsVtopsFailedExecutions.ResetAll()

		e := NewExecVTOps()
		e.Service = t.Name()
		e.BinaryPath = "/bin/does-not-exist"
		assert.ErrorContains(t, e.execCommand("test", "test"), "fork/exec /bin/does-not-exist: no such file or directory")
		assert.Equal(t, map[string]int64{"test.1": 1}, statsVtopsFailedExecutions.Counts())
		assert.Empty(t, statsVtopsSuccessfulExecutions.Counts())
	})

	t.Run("failed", func(t *testing.T) {
		defer statsVtopsFailedExecutions.ResetAll()

		e := NewExecVTOps()
		e.Service = t.Name()
		e.BinaryPath = "/usr/bin/false"
		assert.ErrorContains(t, e.execCommand("test", "test"), "exit status 1")
		assert.Equal(t, map[string]int64{"test.1": 1}, statsVtopsFailedExecutions.Counts())
		assert.Empty(t, statsVtopsSuccessfulExecutions.Counts())
	})

	t.Run("timeout", func(t *testing.T) {
		vtopsExecTimeoutOrig := vtopsExecTimeout
		defer func() {
			statsVtopsFailedExecutions.ResetAll()
			vtopsExecTimeout = vtopsExecTimeoutOrig
		}()
		vtopsExecTimeout = 500 * time.Millisecond

		e := NewExecVTOps()
		e.Service = t.Name()
		e.BinaryPath = "/bin/sleep"
		assert.ErrorContains(t, e.execCommand("test", "60"), "signal: killed")
		assert.Equal(t, map[string]int64{"test.-1": 1}, statsVtopsFailedExecutions.Counts())
		assert.Empty(t, statsVtopsSuccessfulExecutions.Counts())
	})
}
