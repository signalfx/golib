package sfxclient

import (
	"runtime"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGoMetricsSource(t *testing.T) {
	Convey("go stats should fetch", t, func() {
		GoMetricsSource.AddCallback(func(stats *runtime.MemStats) {})
		So(GoMetricsSource.(*goMetrics).cb, ShouldNotBeNil)
		So(len(GoMetricsSource.Datapoints()), ShouldEqual, 30)
	})
}
