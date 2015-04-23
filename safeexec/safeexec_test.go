package safeexec

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var Bob = "abc"

func TestEchoExec(t *testing.T) {
	stdout, stderr, err := Execute("echo", "", "hi")
	assert.NoError(t, err)
	assert.Equal(t, stdout, "hi\n")
	assert.Equal(t, stderr, "")
}

func TestNotHereExec(t *testing.T) {
	_, _, err := Execute("asdgfhnjasdgnadsjkgnjadhfgnjkadf", "", "hi")
	assert.Error(t, err)
}
