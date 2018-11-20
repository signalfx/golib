package dpsink

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/trace"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

const numTests = 20

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
	assert.Equal(t, int64(16), atomic.LoadInt64(&count.IncomingDatapointsBatchSize), "Error reading bytes from the datapoints")

	bs.RetError(errors.New("nope"))
	if err := middleSink.AddDatapoints(ctx, dps); err == nil {
		t.Fatal("Expected an error!")
	}

	assert.Equal(t, int64(1), atomic.LoadInt64(&count.TotalProcessErrors), "Error should be sent through")
	assert.Equal(t, int64(32), atomic.LoadInt64(&count.IncomingDatapointsBatchSize), "Error reading bytes from the datapoints")
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

	dps := []*datapoint.Datapoint{dp1, dp2}

	count := &Counter{
		Logger: log.Discard,
	}

	Convey("Counting Datapoints batch size", t, func() {
		count.countIncomingBatchSize(dps)
		expectedBatchSize := int64(0)
		for _, point := range dps {
			expectedBatchSize += int64(unsafe.Sizeof(point))
		}
		assert.Equal(t, expectedBatchSize, atomic.LoadInt64(&count.IncomingDatapointsBatchSize), "datapoint batch size calculation error")
	})
}
