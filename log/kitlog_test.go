package log_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/go-stack/stack"
	"github.com/signalfx/golib/v3/log"
	"github.com/stretchr/testify/assert"
)

type panicLogger struct{}

func (d *panicLogger) Disabled() bool {
	return true
}

func (d *panicLogger) Log(...interface{}) {
	panic("bad")
}

func (d *panicLogger) ErrorLogger(error) log.Logger {
	return d
}

func TestContext(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	logger := log.NewLogfmtLogger(buf, &panicLogger{})

	kvs := []interface{}{"a", 123}
	lc := log.NewContext(logger).With(kvs...)
	kvs[1] = 0 // With should copy its key values

	lc = lc.With("b", "c") // With should stack
	lc.Log("msg", "message")
	if want, have := "a=123 b=c msg=message\n", buf.String(); want != have {
		t.Errorf("\nwant: %shave: %s", want, have)
	}

	buf.Reset()
	lc = lc.WithPrefix("p", "first")
	lc.Log("msg", "message")
	if want, have := "p=first a=123 b=c msg=message\n", buf.String(); want != have {
		t.Errorf("\nwant: %shave: %s", want, have)
	}
}

func TestContextMissingValue(t *testing.T) {
	t.Parallel()
	var output []interface{}
	logger := log.LoggerFunc(func(keyvals ...interface{}) {
		output = keyvals
	})

	lc := log.NewContext(logger)

	lc.Log("k")
	if want, have := 1, len(output); want != have {
		t.Errorf("want len(output) == %v, have %v", want, have)
	}

	var rec interface{}
	func() {
		defer func() {
			rec = recover()
		}()
		lc.With("k1").WithPrefix("k0").Log("k2")
	}()
	if rec == nil {
		t.Errorf("Expected an error!")
	}
}

// Test that Context.Log has a consistent function stack depth when binding
// log.Valuers, regardless of how many times Context.With has been called or
// whether Context.Log is called via an interface typed variable or a concrete
// typed variable.
func TestContextStackDepth(t *testing.T) {
	fn := fmt.Sprintf("%n", stack.Caller(0))

	var output []interface{}

	logger := log.LoggerFunc(func(keyvals ...interface{}) {
		output = keyvals
	})

	stackValuer := log.DynamicFunc(func() interface{} {
		for i, c := range stack.Trace() {
			if fmt.Sprintf("%n", c) == fn {
				return i
			}
		}
		t.Fatal("Test function not found in stack trace.")
		return nil
	})

	concrete := log.NewContext(logger).With("stack", stackValuer)
	var iface log.Logger = concrete

	// Call through interface to get baseline.
	iface.Log("k", "v")
	want, ok := output[1].(int)
	assert.True(t, ok)
	for len(output) < 10 {
		concrete.Log("k", "v")
		if have := output[1]; have != want {
			t.Errorf("%d Withs: have %v, want %v", len(output)/2-1, have, want)
		}

		iface.Log("k", "v")
		if have := output[1]; have != want {
			t.Errorf("%d Withs: have %v, want %v", len(output)/2-1, have, want)
		}

		wrapped := log.NewContext(concrete)
		wrapped.Log("k", "v")
		if have := output[1]; have != want {
			t.Errorf("%d Withs: have %v, want %v", len(output)/2-1, have, want)
		}

		concrete = concrete.With("k", "v")
		iface = concrete
	}
}

// Test that With returns a Logger safe for concurrent use. This test
// validates that the stored logging context does not get corrupted when
// multiple clients concurrently log additional keyvals.
//
// This test must be run with go test -cpu 2 (or more) to achieve its goal.
func TestWithConcurrent(t *testing.T) {
	// Create some buckets to count how many events each goroutine logs.
	const goroutines = 8
	counts := [goroutines]int{}

	// This logger extracts a goroutine id from the last value field and
	// increments the referenced bucket.
	logger := log.LoggerFunc(func(kv ...interface{}) {
		goroutine, ok := kv[len(kv)-1].(int)
		if ok {
			counts[goroutine]++
		}
	})

	// With must be careful about handling slices that can grow without
	// copying the underlying array, so give it a challenge.
	l := log.NewContext(logger).With(make([]interface{}, 0, 2)...)

	// Start logging concurrently. Each goroutine logs its id so the logger
	// can bucket the event counts.
	var wg sync.WaitGroup
	wg.Add(goroutines)
	const n = 1000
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < n; j++ {
				l.Log("goroutineIdx", idx)
			}
		}(i)
	}
	wg.Wait()

	for bucket, have := range counts {
		if want := n; want != have {
			t.Errorf("bucket %d: want %d, have %d", bucket, want, have) // note Errorf
		}
	}
}

func BenchmarkDiscard(b *testing.B) {
	logger := log.Discard
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Log("k", "v")
	}
}

func BenchmarkOneWith(b *testing.B) {
	logger := log.Discard
	lc := log.NewContext(logger).With("k", "v")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lc.Log("k", "v")
	}
}

func BenchmarkTwoWith(b *testing.B) {
	logger := log.Discard
	lc := log.NewContext(logger).With("k", "v")
	for i := 1; i < 2; i++ {
		lc = lc.With("k", "v")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lc.Log("k", "v")
	}
}

func BenchmarkTenWith(b *testing.B) {
	logger := log.Discard
	lc := log.NewContext(logger).With("k", "v")
	for i := 1; i < 10; i++ {
		lc = lc.With("k", "v")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lc.Log("k", "v")
	}
}
