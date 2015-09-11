package web

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/signalfx/golib/timekeeper/timekeepertest"
	. "github.com/smartystreets/goconvey/convey"
)

type limitStub time.Duration

func (l limitStub) Get() time.Duration {
	return time.Duration(l)
}

func TestReqLatencyCounter(t *testing.T) {
	starttime := time.Date(1981, time.March, 19, 6, 0, 0, 0, time.UTC)
	fastRequestLimit := 100 * time.Millisecond
	limitStub := limitStub(fastRequestLimit)

	Convey("When setup,", t, func() {
		timeStub := timekeepertest.NewStubClock(starttime)
		counter := ReqLatencyCounter{
			fastRequestLimitDuration: limitStub,
			timeKeeper:               timeStub,
		}
		ctx := context.Background()
		Convey("having no requests results in 2 stat metrics.", func() {
			stats := counter.Stats(map[string]string{})
			So(stats, ShouldNotBeNil)
			So(len(stats), ShouldEqual, 2)
		})
		Convey("slow increment the slow request count.", func() {
			ctx = AddTime(ctx, starttime)
			timeStub.Incr(fastRequestLimit + 1)
			counter.ModStats(ctx)
			So(counter.slowRequests, ShouldEqual, 1)
			So(counter.fastRequests, ShouldEqual, 0)
		})
		Convey("fast increment the fast request count.", func() {
			ctx = AddTime(ctx, starttime)
			timeStub.Incr(fastRequestLimit - 1)
			counter.ModStats(ctx)
			So(counter.slowRequests, ShouldEqual, 0)
			So(counter.fastRequests, ShouldEqual, 1)
		})
		Convey("having request results in 2 stat metrics.", func() {
			counter.fastRequests++
			counter.slowRequests++
			stats := counter.Stats(map[string]string{})
			So(stats, ShouldNotBeNil)
			So(len(stats), ShouldEqual, 2)
		})
	})

}
