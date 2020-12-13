package dpsink

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dptest"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/log"
	"github.com/signalfx/golib/v3/logkey"
	"github.com/signalfx/golib/v3/sfxclient/spanfilter"
	"github.com/signalfx/golib/v3/trace"
	"github.com/stretchr/testify/assert"
)

const numTests = 19

func TestCounterSink(t *testing.T) {
	dps := []*datapoint.Datapoint{
		{},
		{},
	}
	i := int64(0)
	ctx := context.Background()
	bs := dptest.NewBasicSink()
	count := &Counter{
		Logger:    log.Discard,
		LoggerKey: logkey.Caller,
		LoggerFunc: func(ctx context.Context) string {
			atomic.AddInt64(&i, 1)
			return "blarg"
		},
		DroppedReason: "mwp™",
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
	dptest.ExactlyOneDims(count.Datapoints(), "dropped_points", map[string]string{"reason": "mwp™"})

	bs.RetError(errors.New("nope"))
	if err := middleSink.AddDatapoints(ctx, dps); err == nil {
		t.Fatal("Expected an error!")
	}
	assert.Equal(t, int64(1), atomic.LoadInt64(&count.TotalProcessErrors), "Error should be sent through")
	assert.Equal(t, int64(1), i, "Number of errors should equal log statements")
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
	assert.Equal(t, int64(2), atomic.LoadInt64(&count.ProcessErrorSpans), "Num should be 2")
	m := &spanfilter.Map{}
	m.Add("blarg", "foo")
	bs.RetError(m)
	if err := middleSink.AddSpans(ctx, es); err == nil {
		t.Fatal("Expected an error!")
	}
	assert.Equal(t, int64(2), atomic.LoadInt64(&count.TotalProcessErrors), "Error should be sent through")
	assert.Equal(t, int64(3), atomic.LoadInt64(&count.ProcessErrorSpans), "Num should be one more")
}
