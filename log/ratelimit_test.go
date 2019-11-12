package log

import (
	"testing"
	"time"

	"github.com/signalfx/golib/v3/eventcounter"
	"github.com/signalfx/golib/v3/timekeeper/timekeepertest"
	. "github.com/smartystreets/goconvey/convey"
)

func TestRateLimitedLogger(t *testing.T) {
	Convey("a default rate limit logger", t, func() {
		r := RateLimitedLogger{}
		Convey("should start off disabled", func() {
			So(IsDisabled(&r), ShouldBeTrue)
			So(func() {
				r.Log("hi", "bob")
			}, ShouldNotPanic)
		})
		Convey("NewOnePerSecond should limit to one per second", func() {
			counter := &Counter{}
			rptr := NewOnePerSecond(counter)
			rptr.Log()
			rptr.Log()
			So(counter.Count, ShouldEqual, 1)
		})
		Convey("setup to count", func() {
			tk := timekeepertest.NewStubClock(time.Now())
			counter := &Counter{}
			counter2 := &Counter{}
			r = RateLimitedLogger{
				EventCounter: eventcounter.New(tk.Now(), time.Second*2),
				Now:          tk.Now,
				Limit:        10,
				Logger:       counter,
				LimitLogger:  counter2,
			}
			Convey("Should log 9 times", func() {
				for i := int64(1); i <= r.Limit-1; i++ {
					r.Log()
					So(counter.Count, ShouldEqual, i)
					So(counter2.Count, ShouldEqual, 0)
				}
				Convey("and a 10th when time advances", func() {
					tk.Incr(time.Second)
					r.Log()
					So(counter.Count, ShouldEqual, 10)
					Convey("but not a 11th", func() {
						for i := int64(1); i <= r.Limit; i++ {
							r.Log()
							So(counter.Count, ShouldEqual, r.Limit)
							So(counter2.Count, ShouldEqual, i)
						}
						Convey("until time advances", func() {
							tk.Incr(time.Second)
							r.Log()
							So(counter.Count, ShouldEqual, r.Limit+1)
						})
					})
				})
			})
		})
	})
}

func BenchmarkRateLimitedWithContext(b *testing.B) {
	tk := timekeepertest.NewStubClock(time.Now())
	l := NewContext(Discard).With(Key("caller"), DefaultCaller)
	r := RateLimitedLogger{
		EventCounter: eventcounter.New(tk.Now(), time.Second*2),
		Now:          tk.Now,
		Limit:        1,
		Logger:       l,
		LimitLogger:  l,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < 10; i++ {
			r.Log("hello")
		}
		tk.Incr(time.Second)
		r.Log("world")
		tk.Incr(time.Second)
	}
}

func BenchmarkRateLimitedWithOutContext(b *testing.B) {
	tk := timekeepertest.NewStubClock(time.Now())
	r := RateLimitedLogger{
		EventCounter: eventcounter.New(tk.Now(), time.Second*2),
		Now:          tk.Now,
		Limit:        1,
		Logger:       Discard,
		LimitLogger:  Discard,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < 10; i++ {
			r.Log("hello")
		}
		tk.Incr(time.Second)
		r.Log("world")
		tk.Incr(time.Second)
	}
}
