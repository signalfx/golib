package go_metrics

import (
	"strings"
	"testing"

	"github.com/rcrowley/go-metrics"
	"github.com/signalfx/golib/v3/datapoint/dptest"
	"github.com/signalfx/golib/v3/log"
	. "github.com/smartystreets/goconvey/convey"
)

func Test(t *testing.T) {
	Convey("test the go-metrics exporter", t, func() {
		r := metrics.NewRegistry()
		e := New(r, map[string]string{"foo": "bar"}, kafkaHarvester, tedoHarvester)
		r.Register("timer", metrics.NewTimer())
		r.Register("records-per-request-for-topic-trace_spans_firehose_477610686596251648", metrics.NewHistogram(metrics.NewUniformSample(10)))
		r.Register("slices-of-pizza-for-tedo-dominos", metrics.NewGauge())
		r.Register("f64", metrics.NewGaugeFloat64())
		r.Register("meter", metrics.NewMeter())
		r.Register("counter", metrics.NewCounter())

		dps := e.Datapoints()
		So(len(dps), ShouldEqual, 26)
		for _, d := range dps {
			log.DefaultLogger.Log(d.Metric, d.Dimensions)
		}
		dp := dptest.ExactlyOne(dps, "records-per-request.count")
		So(dp.Dimensions["topic"], ShouldEqual, "trace_spans_firehose_477610686596251648")
		So(dp.Dimensions["foo"], ShouldEqual, "bar")
		So(len(dp.Dimensions), ShouldEqual, 2)
		dp = dptest.ExactlyOne(dps, "timer.count")
		So(dp, ShouldNotBeNil)
		So(len(dp.Dimensions), ShouldEqual, 1)
		So(dp.Dimensions["foo"], ShouldEqual, "bar")
		dp = dptest.ExactlyOne(dps, "slices-of-pizza")
		So(len(dp.Dimensions), ShouldEqual, 2)
		So(dp.Dimensions["vendor"], ShouldEqual, "dominos")
		So(dp.Dimensions["foo"], ShouldEqual, "bar")
		So(len(e.memory), ShouldEqual, 6)
		log.DefaultLogger.Log(e.memory)
	})
}

func kafkaHarvester(name string) (bool, string, map[string]string) {
	pieces := strings.Split(name, "-for-topic-")
	if len(pieces) > 1 {
		return true, pieces[0], map[string]string{"topic": pieces[1]}
	}
	return false, "", nil
}

func tedoHarvester(name string) (bool, string, map[string]string) {
	pieces := strings.Split(name, "-for-tedo-")
	if len(pieces) > 1 {
		return true, pieces[0], map[string]string{"vendor": pieces[1]}
	}
	return false, "", nil
}
