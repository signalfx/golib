package safeexec

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEchoExec(t *testing.T) {
	if runtime.GOOS != "windows" {
		stdout, stderr, err := Execute("echo", "", "hi")
		assert.NoError(t, err)
		assert.Equal(t, stdout, "hi\n")
		assert.Equal(t, stderr, "")
	}
}

func TestNotHereExec(t *testing.T) {
	_, _, err := Execute("asdgfhnjasdgnadsjkgnjadhfgnjkadf", "", "hi")
	assert.Error(t, err)
}
