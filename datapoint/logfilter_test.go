package datapoint

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestFilter(t *testing.T) {
	Convey("filtered logger", t, func() {
		filter := &LogFilter{
			MetricKeyName: "sf_metric",
		}
		abcDP := New("mname1", map[string]string{"org": "abc"}, nil, 0, time.Now())
		efgDP := New("mname2", map[string]string{"org": "efg"}, nil, 0, time.Now())
		Convey("should be disabled", func() {
			So(filter.Disabled(), ShouldBeTrue)
			Convey("unless checking log signal", func() {
				filter.CheckLogSignal = true
				So(filter.Disabled(), ShouldBeFalse)
				So(filter.WouldLog(LogKey, abcDP, "hello world"), ShouldBeFalse)
				So(filter.WouldLog(LogKey, SetLogSignal(efgDP), "hello world"), ShouldBeTrue)
			})
		})
		Convey("should not log no datapoint", func() {
			So(filter.WouldLog("hello world"), ShouldBeFalse)
		})
		Convey("should not log when dp is not a datapoint", func() {
			So(filter.WouldLog(LogKey, "hello world"), ShouldBeFalse)
		})
		Convey("should not log by default", func() {
			So(filter.WouldLog(LogKey, abcDP, "hello world"), ShouldBeFalse)
		})
		Convey("Should check log signal", func() {
			So(HasLogSignal(efgDP), ShouldBeFalse)
			SetLogSignal(efgDP)
			So(HasLogSignal(efgDP), ShouldBeTrue)
			So(filter.WouldLog(LogKey, efgDP, "hello world"), ShouldBeFalse)
			filter.CheckLogSignal = true
			So(filter.WouldLog(LogKey, efgDP, "hello world"), ShouldBeTrue)
		})

		Convey("When enabled for the Metric", func() {
			filter.SetDimensions(map[string]string{
				"sf_metric": "mname1",
			})
			Convey("Should log keys correctly", func() {
				So(filter.WouldLog(LogKey, abcDP, "hello world"), ShouldBeTrue)
				So(filter.WouldLog(LogKey, efgDP, "hello world"), ShouldBeFalse)
			})

		})
		Convey("When enabled", func() {
			filter.SetDimensions(map[string]string{
				"org": "abc",
			})
			Convey("should export stats", func() {
				So(filter.Var().String(), ShouldContainSubstring, "abc")
			})
			Convey("Should log keys correctly", func() {
				So(filter.WouldLog(LogKey, abcDP, "hello world"), ShouldBeTrue)
				So(filter.WouldLog(LogKey, efgDP, "hello world"), ShouldBeFalse)
				So(filter.WouldLog("hello world"), ShouldBeFalse)
				So(filter.WouldLog(LogKey, "hello world"), ShouldBeFalse)
			})
		})
	})
}
