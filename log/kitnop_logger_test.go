package log_test

import (
	"testing"

	"github.com/signalfx/golib/v3/log"
)

func TestNopLogger(_ *testing.T) {
	logger := log.Discard
	logger.Log("abc", 123)
	log.NewContext(logger).With("def", "ghi").Log()
}
