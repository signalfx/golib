package dpsink

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/timekeeper/timekeepertest"
	"github.com/signalfx/golib/trace"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

const numTests = 22

func TestCounterSink(t *testing.T) {
	dps := []*datapoint.Datapoint{
		{},
		{},
	}
	ctx := context.Background()
	bs := dptest.NewBasicSink()
	count := &Counter{
		Logger: log.Discard,
	}
	histo := NewHistoCounter(count)
	middleSink := NextWrap(histo)(bs)
	go func() {
		// Allow time for us to get in the middle of a call
		time.Sleep(time.Millisecond)
		assert.Equal(t, int64(1), atomic.LoadInt64(&count.CallsInFlight), "After a sleep, should be in flight")
		datas := <-bs.PointsChan
		assert.Equal(t, 2, len(datas), "Original datas should be sent")
	}()
	log.IfErr(log.Panic, middleSink.AddDatapoints(ctx, dps))
	assert.Equal(t, int64(0), atomic.LoadInt64(&count.CallsInFlight), "Call is finished")
	assert.Equal(t, int64(0), atomic.LoadInt64(&count.TotalProcessErrors), "No errors so far (see above)")
	assert.Equal(t, numTests, len(histo.Datapoints()), "Just checking stats len()")

	bs.RetError(errors.New("nope"))
	if err := middleSink.AddDatapoints(ctx, dps); err == nil {
		t.Fatal("Expected an error!")
	}

	assert.Equal(t, int64(1), atomic.LoadInt64(&count.TotalProcessErrors), "Error should be sent through")
}

func TestCounterSinkEvent(t *testing.T) {
	es := []*event.Event{
		{},
		{},
	}
	ctx := context.Background()
	bs := dptest.NewBasicSink()
	count := &Counter{}
	histo := NewHistoCounter(count)
	middleSink := NextWrap(histo)(bs)
	go func() {
		// Allow time for us to get in the middle of a call
		time.Sleep(time.Millisecond)
		assert.Equal(t, int64(1), atomic.LoadInt64(&count.CallsInFlight), "After a sleep, should be in flight")
		datas := <-bs.EventsChan
		assert.Equal(t, 2, len(datas), "Original datas should be sent")
	}()
	log.IfErr(log.Panic, middleSink.AddEvents(ctx, es))
	assert.Equal(t, int64(0), atomic.LoadInt64(&count.CallsInFlight), "Call is finished")
	assert.Equal(t, int64(0), atomic.LoadInt64(&count.TotalProcessErrors), "No errors so far (see above)")
	assert.Equal(t, numTests, len(histo.Datapoints()), "Just checking stats len()")

	bs.RetError(errors.New("nope"))
	if err := middleSink.AddEvents(ctx, es); err == nil {
		t.Fatal("Expected an error!")
	}
	assert.Equal(t, int64(1), atomic.LoadInt64(&count.TotalProcessErrors), "Error should be sent through")
}

func TestCounterSinkTrace(t *testing.T) {
	es := []*trace.Span{
		{},
		{},
	}
	ctx := context.Background()
	bs := dptest.NewBasicSink()
	count := &Counter{}
	histo := NewHistoCounter(count)
	middleSink := trace.NextWrap(histo)(bs)
	go func() {
		// Allow time for us to get in the middle of a call
		time.Sleep(time.Millisecond)
		assert.Equal(t, int64(1), atomic.LoadInt64(&count.CallsInFlight), "After a sleep, should be in flight")
		datas := <-bs.TracesChan
		assert.Equal(t, 2, len(datas), "Original datas should be sent")
	}()
	log.IfErr(log.Panic, middleSink.AddSpans(ctx, es))
	assert.Equal(t, int64(0), atomic.LoadInt64(&count.CallsInFlight), "Call is finished")
	assert.Equal(t, int64(0), atomic.LoadInt64(&count.TotalProcessErrors), "No errors so far (see above)")
	assert.Equal(t, numTests, len(histo.Datapoints()), "Just checking stats len()")

	bs.RetError(errors.New("nope"))
	if err := middleSink.AddSpans(ctx, es); err == nil {
		t.Fatal("Expected an error!")
	}
	assert.Equal(t, int64(1), atomic.LoadInt64(&count.TotalProcessErrors), "Error should be sent through")
}

func dpNamed(name string, dps []*datapoint.Datapoint) *datapoint.Datapoint {
	for _, dp := range dps {
		if dp.Metric == name {
			return dp
		}
	}
	return nil
}

func TestDatapointBatchSize(t *testing.T) {
	dp1 := &datapoint.Datapoint{}
	dp1.Value = datapoint.NewIntValue(int64(123456))
	dp1.Timestamp = time.Now()
	dp1.Dimensions = make(map[string]string)
	dp1.Dimensions["name"] = "test1"
	dp1.Dimensions["location"] = "underEarth"
	dp1.Metric = "test1_underEarth"
	dp1.MetricType = datapoint.Gauge

	dp2 := &datapoint.Datapoint{}
	dp2.Value = datapoint.NewIntValue(int64(12345678))
	dp2.Timestamp = time.Now()
	dp2.Dimensions = make(map[string]string)
	dp2.Dimensions["name"] = "test2"
	dp2.Dimensions["location"] = "inTheSky"
	dp2.Metric = "test2_inTheSky"
	dp2.MetricType = datapoint.Counter

	count := &Counter{
		Logger:                      log.Discard,
		IncomingDatapointsBatchSize: sfxclient.NewRollingBucket("incoming_datapoint_batch_size", map[string]string{"path": "server"}),
	}
	r := count.IncomingDatapointsBatchSize
	tk := timekeepertest.NewStubClock(time.Now())
	r.Timer = tk
	Convey("Get datapoints should work", t, func() {
		dps := r.Datapoints()
		So(len(dps), ShouldEqual, 3)
		So(dpNamed("incoming_datapoint_batch_size.sum", dps).Value.String(), ShouldEqual, "0")
	})

	Convey("Unadvanced clock should get only the normal points", t, func() {
		r.Add(float64(dp1.Value.(datapoint.IntValue).Int()))
		dps := r.Datapoints()
		So(len(dps), ShouldEqual, 3)
		So(dpNamed("incoming_datapoint_batch_size.sum", dps).Value.String(), ShouldEqual, dp1.Value.String())
	})

	Convey("Advanced clock should get the one point", t, func() {
		r.Add(float64(dp1.Value.(datapoint.IntValue).Int()))
		tk.Incr(r.BucketWidth)
		r.Add(float64(dp2.Value.(datapoint.IntValue).Int()))
		dps := r.Datapoints()
		So(len(dps), ShouldEqual, 3+len(r.Quantiles)+2)
		So(dpNamed("incoming_datapoint_batch_size.sum", dps).Value.String(), ShouldEqual, "12592590")
		So(dpNamed("incoming_datapoint_batch_size.p90", dps).Value.String(), ShouldEqual, "123456")
	})

	Convey("Percentiles testing for datapoints batch size", t, func() {
		tk.Incr(r.BucketWidth)
		for i := 1; i <= 100; i++ {
			r.Add(float64(i * 1000))
		}
		dps := r.Datapoints()
		So(dpNamed("incoming_datapoint_batch_size.sum", dps).Value.String(), ShouldEqual, "17642590")
		So(dpNamed("incoming_datapoint_batch_size.p25", dps).Value.String(), ShouldEqual, "12345678")
		So(dpNamed("incoming_datapoint_batch_size.p50", dps).Value.String(), ShouldEqual, "12345678")
		So(dpNamed("incoming_datapoint_batch_size.p90", dps).Value.String(), ShouldEqual, "12345678")
		So(dpNamed("incoming_datapoint_batch_size.p99", dps).Value.String(), ShouldEqual, "12345678")
		So(dpNamed("incoming_datapoint_batch_size.min", dps).Value.String(), ShouldEqual, "12345678")
		So(dpNamed("incoming_datapoint_batch_size.max", dps).Value.String(), ShouldEqual, "12345678")
	})
}
