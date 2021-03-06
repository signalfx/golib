package log_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/signalfx/golib/v3/log"
)

// These test are designed to be run with the race detector.

func testConcurrency(_ *testing.T, logger log.Logger) {
	for _, n := range []int{10, 100, 500} {
		wg := sync.WaitGroup{}
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() { spam(logger); wg.Done() }()
		}
		wg.Wait()
	}
}

func spam(logger log.Logger) {
	for i := 0; i < 100; i++ {
		logger.Log("key", strconv.FormatInt(int64(i), 10))
	}
}
