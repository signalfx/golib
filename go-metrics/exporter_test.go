package go_metrics

import (
	"testing"

	"github.com/rcrowley/go-metrics"
	. "github.com/smartystreets/goconvey/convey"
)

func Test(t *testing.T) {
	Convey("test the go-metrics exporter", t, func() {
		r := metrics.NewRegistry()
		e := New(r, map[string]string{"foo": "bar"})
		r.Register("timer", metrics.NewTimer())
		r.Register("histo", metrics.NewHistogram(metrics.NewUniformSample(10)))
		r.Register("gague", metrics.NewGauge())
		r.Register("f64", metrics.NewGaugeFloat64())
		r.Register("meter", metrics.NewMeter())
		r.Register("counter", metrics.NewCounter())

		dps := e.Datapoints()
		So(len(dps), ShouldEqual, 26)
	})
}
