package log_test

import (
	"testing"

	"github.com/signalfx/golib/v3/log"
)

func benchmarkRunner(b *testing.B, logger log.Logger, f func(log.Logger)) {
	b.Helper()
	lc := log.NewContext(logger).With("common_key", "common_value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f(lc)
	}
}

var (
	baseMessage = func(logger log.Logger) { logger.Log("foo_key", "foo_value") }
	withMessage = func(logger log.Logger) { log.NewContext(logger).With("a", "b").Log("c", "d") }
)
