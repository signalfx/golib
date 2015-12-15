package log
import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
	"sync"
	"runtime"
	"strconv"
	"bytes"
	"io"
)

func TestWithConcurrent(t *testing.T) {
	t.Logf("Max proc is %d", runtime.GOMAXPROCS(0))
	// Create some buckets to count how many events each goroutine logs.
	const goroutines = 10
	counts := [goroutines]int{}

	// This logger extracts a goroutine id from the last value field and
	// increments the referenced bucket.
	logger := LoggerFunc(func(kv ...interface{}) {
		goroutine := kv[len(kv)-1].(int)
		counts[goroutine]++
		if len(kv) != 10 {
			panic(kv)
		}
	})

	// With must be careful about handling slices that can grow without
	// copying the underlying array, so give it a challenge.
	l := NewContext(logger).With(make([]interface{}, 2, 40)...).With(make([]interface{}, 2, 40)...)

	// Start logging concurrently. Each goroutine logs its id so the logger
	// can bucket the event counts.
	var wg sync.WaitGroup
	wg.Add(goroutines)
	const n = 1000
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < n; j++ {
				l.With("a", "b").WithPrefix("c", "d").Log("goroutineIdx", idx)
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

func TestErrorHandlerFunc(t *testing.T) {
	Convey("ErrorHandlerFunc should wrap", t, func() {
		c := &Counter{}
		f := ErrorHandlerFunc(func(error) Logger {
			return c
		})
		f.ErrorLogger(nil).Log()
		So(c.Count, ShouldEqual, 1)
	})
}

func TestContextOptimizations(t *testing.T) {
	Convey("A normal context", t, func() {
		count := Counter{}
		c := NewContext(&count)
		Convey("should wrap itself", func() {
			So(NewContext(c), ShouldEqual, c)
		})
		Convey("should early exit empty with", func() {
			So(c.With(), ShouldEqual, c)
			So(c.WithPrefix(), ShouldEqual, c)
		})
	})
}

func toStr(in []interface{}) []string {
	ret := make([]string, 0, len(in))
	for i := range in {
		ret = append(ret, in[i].(string))
	}
	return ret
}

func TestLoggingBasics(t *testing.T) {
	Convey("A normal logger", t, func() {
		mem := NewChannelLogger(10, nil)
		c := NewContext(mem)
		Convey("Should not remember with statements", func() {
			c.With("name", "john")
			c.Log()
			So(len(<- mem.Out), ShouldEqual, 0)
		})
		Convey("should even out context values on with", func() {
			c = c.With("name")
			c.Log()
			So(len(<- mem.Out), ShouldEqual, 2)
		})
		Convey("should even out context values on withprefix", func() {
			c = c.WithPrefix("name")
			c.Log()
			So(len(<- mem.Out), ShouldEqual, 2)
		})
		Convey("should even out context values on log", func() {
			c.Log("name")
			So(len(<- mem.Out), ShouldEqual, 2)
		})
		Convey("Should convey params using with", func() {
			c = c.With("name", "john")
			c.Log()
			So(len(<- mem.Out), ShouldEqual, 2)
			Convey("and Log()", func() {
				c.Log("age", "10")
				So(toStr(<- mem.Out), ShouldResemble, []string{"name", "john", "age", "10"})
			})
			Convey("should put WithPrefix first", func() {
				c = c.WithPrefix("name", "jack")
				c.Log()
				So(toStr(<- mem.Out), ShouldResemble, []string{"name", "jack", "name", "john"})
			})
		})
	})
}

func BenchmarkEmptyLogDisabled(b *testing.B) {
	c := NewContext(Discard)
	for i:=0;i<b.N;i++ {
		c.Log()
	}
}

func BenchmarkEmptyLogNotDisabled(b *testing.B) {
	count := Counter{}
	c := NewContext(&count)
	for i:=0;i<b.N;i++ {
		c.Log()
	}
}

func BenchmarkContextWithLog(b *testing.B) {
	count := Counter{}
	c := NewContext(&count)
	c = c.With("hello", "world")
	for i:=0;i<b.N;i++ {
		c.Log("name", "bob")
	}
}

func BenchmarkContextWithOnly(b *testing.B) {
	count := Counter{}
	c := NewContext(&count)
	b.ReportAllocs()
	b.ResetTimer()
	for i:=0;i<b.N;i++ {
		c.With("", "")
	}
}

func BenchmarkContextWithWithLog(b *testing.B) {
	count := Counter{}
	c := NewContext(&count)
	c = c.With("hello", "world")
	for i:=0;i<b.N;i++ {
		c.With("type", "dog").Log("name", "bob")
	}
}
func BenchmarkDiscard(b *testing.B) {
	logger := Discard
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Log("k", "v")
	}
}

func BenchmarkOneWith(b *testing.B) {
	logger := Discard
	lc := NewContext(logger).With("k", "v")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lc.Log("k", "v")
	}
}

func BenchmarkTwoWith(b *testing.B) {
	logger := Discard
	lc := NewContext(logger).With("k", "v")
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
	logger := Discard
	lc := NewContext(logger).With("k", "v")
	for i := 1; i < 10; i++ {
		lc = lc.With("k", "v")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lc.Log("k", "v")
	}
}

func TestDisabledLog(t *testing.T) {
	Convey("A log that is disabled", t, func() {
		count := Counter{}
		gate := Gate{
			Logger: &count,
		}
		gate.Disable()
		c := NewContext(&gate)
		Convey("should not log", func() {
			wrapped := c.With("hello", "world")
			c.Log("hello", "world")
			wrapped.Log("do not", "do it")
			c.WithPrefix("hello", "world").Log("do not", "do it")

			So(count.Count, ShouldEqual, 0)
			Convey("until disabled is off", func() {
				gate.Enable()
				c.Log("hi")
				wrapped.Log("hi")
				So(count.Count, ShouldEqual, 2)

				gate.Disable()
				c.Log("hi")
				So(count.Count, ShouldEqual, 2)
			})
		})
	})
}

type lockWriter struct {
	out io.Writer
	mu sync.Mutex
}

func (l *lockWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.out.Write(b)
}

func TestNewChannelLoggerRace(t *testing.T) {
	l := NewChannelLogger(0, Discard)
	raceCheck(l)
}

func TestNewJSONLoggerRace(t *testing.T) {
	l := NewJSONLogger(&lockWriter{out:&bytes.Buffer{}}, Discard)
	raceCheck(l)
}

func TestNewLogfmtLoggerRace(t *testing.T) {
	b := &bytes.Buffer{}
	l := NewLogfmtLogger(&lockWriter{out:b}, Discard)
	raceCheck(l)
}

func TestCounterRace(t *testing.T) {
	l := &Counter{}
	raceCheck(l)
}

func raceCheck(l Logger) {
	raceCheckerIter(l, 3, 10)
}

func raceCheckerIter(l Logger, deep int, iter int) {
	if deep == 0 {
		return
	}
	l.Log("deep", deep)
	ctx := NewContext(l)
	wg := sync.WaitGroup{}
	wg.Add(iter)
	for i :=0; i < iter; i++ {
		go func(i int) {
			raceCheckerIter(ctx.With(strconv.FormatInt(int64(deep), 10), i), deep-1, iter)
			wg.Done()
		}(i)
	}
	wg.Wait()
}