package sfxclient

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTimeCounter(t *testing.T) {
	t.Parallel()
	Convey("Default error handler should not panic", t, func() {
		tc := TimeCounter{
			NsBarrier: time.Second.Nanoseconds(),
		}
		tc.Add(time.Millisecond)
		tc.Add(time.Millisecond * 100)
		tc.Add(time.Millisecond * 2000)
		So(tc.Above, ShouldEqual, 1)
		So(tc.Below, ShouldEqual, 2)
		dp1 := tc.Collector("mname").Datapoints()[0]
		So(dp1.Metric, ShouldEqual, "mname")
		So(dp1.Value.String(), ShouldEqual, "1")
	})
}
