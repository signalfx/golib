package sfxclient

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/juju/errors"
	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/trace"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAsyncMultiTokenSinkStartup(t *testing.T) {
	Convey("A default sink", t, func() {
		So(NewAsyncMultiTokenSink(int64(1), int64(1), 5, 30, IngestEndpointV2, EventIngestEndpointV2, TraceIngestEndpointV1, DefaultUserAgent, newDefaultHTTPClient, DefaultErrorHandler, 0), ShouldNotBeNil)

		Convey("should be able to startup successfully", func() {
			So(NewAsyncMultiTokenSink(int64(1), int64(1), 5, 30, IngestEndpointV2, EventIngestEndpointV2, TraceIngestEndpointV1, DefaultUserAgent, newDefaultHTTPClient, nil, 0), ShouldNotBeNil)
		})

		Convey("should be able to startup successfully without a timebuffer", func() {
			So(NewAsyncMultiTokenSink(int64(3), int64(3), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0), ShouldNotBeNil)
		})
	})
}

func TestAddDataToAsyncMultitokenSink(t *testing.T) {
	Convey("A default sink", t, func() {
		s := NewAsyncMultiTokenSink(int64(2), int64(2), 5, 5000, "", "", "", "", newDefaultHTTPClient, nil, 0)
		ctx := context.Background()
		dps := GoMetricsSource.Datapoints()
		evs := GoEventSource.Events()
		spans := GoSpanSource.Spans()

		Convey("shouldn't accept dps and events with a context if a token isn't provided in the context", func() {
			So(errors.Details(s.AddEvents(ctx, evs)), ShouldContainSubstring, "no value was found on the context with key")
			So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "no value was found on the context with key")
			So(errors.Details(s.AddSpans(ctx, spans)), ShouldContainSubstring, "no value was found on the context with key")
		})

		Convey("shouldn't accept dps and events if the sink has started, but the workers have shutdown", func() {
			ctx = context.WithValue(ctx, TokenCtxKey, "HELLOOOOOO")
			s.ShutdownTimeout = time.Second * 1
			So(s.Close(), ShouldBeNil)
			_ = s.AddEvents(ctx, evs)
			_ = s.AddDatapointsWithToken("HELLOOOOOO", dps)
			So(errors.Details(s.AddEvents(ctx, evs)), ShouldContainSubstring, "unable to add events: the worker has been stopped")
			So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "unable to add datapoints: the worker has been stopped")
			So(errors.Details(s.AddEventsWithToken("HELLOOOOO", evs)), ShouldContainSubstring, "unable to add events: the worker has been stopped")
			So(errors.Details(s.AddDatapointsWithToken("HELLOOOOOO", dps)), ShouldContainSubstring, "unable to add datapoints: the worker has been stopped")
		})
	})
}

func TestAsyncMultiTokenSinkClose(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should be able to close successfully when no data has been added to it", func() {
			s := NewAsyncMultiTokenSink(int64(2), int64(2), 5, 25, "", "", "", "", newDefaultHTTPClient, nil, 0)
			So(s, ShouldNotBeNil)
			s.ShutdownTimeout = time.Millisecond * 500
			So(s.Close(), ShouldBeNil)
		})
	})
}

func AddDatapointsGetError(ctx context.Context, dps []*datapoint.Datapoint) (err error) {
	err = &SFXAPIError{
		StatusCode:   http.StatusRequestTimeout,
		ResponseBody: "HELLO",
	}
	return
}

func AddDatapointsGetSuccess(ctx context.Context, dps []*datapoint.Datapoint) (err error) {
	return
}

func AddEventsGetError(ctx context.Context, evs []*event.Event) (err error) {
	err = &SFXAPIError{
		StatusCode:   http.StatusRequestTimeout,
		ResponseBody: "HELLO",
	}
	return
}

func AddEventsGetSuccess(ctx context.Context, evs []*event.Event) (err error) {
	return
}

func AddSpansGetError(ctx context.Context, evs []*trace.Span) (err error) {
	err = &SFXAPIError{
		StatusCode:   http.StatusRequestTimeout,
		ResponseBody: "HELLO",
	}
	return
}

func AddSpansGetSuccess(ctx context.Context, evs []*trace.Span) (err error) {
	return
}

func TestGetHTTPStatusCode(t *testing.T) {
	ts := &tokenStatus{}
	Convey("should handle different type of errors", t, func() {
		So(getHTTPStatusCode(ts, nil).status, ShouldEqual, http.StatusOK)

		So(getHTTPStatusCode(ts, &SFXAPIError{
			StatusCode:   http.StatusBadRequest,
			ResponseBody: "",
		}).status, ShouldEqual, http.StatusBadRequest)

		err := &TooManyRequestError{
			ThrottleType: "",
			RetryAfter:   0,
			Err: &SFXAPIError{
				StatusCode:   http.StatusTooManyRequests,
				ResponseBody: "",
			},
		}
		So(getHTTPStatusCode(ts, err).status, ShouldEqual, http.StatusTooManyRequests)
	})
}

func TestWorkerErrorHandlerDps(t *testing.T) {
	Convey("An AsyncMultiTokeSink Worker Dps", t, func() {
		Convey("should handle errors while emitting datapoints", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 7, "", "", "", "", newDefaultHTTPClient, nil, 0)
			s.ShutdownTimeout = time.Second * 5
			s.dpChannels[0].workers[0].handleError(fmt.Errorf("this is an error"), "HELLOOOOO", []*datapoint.Datapoint{Cumulative("metricname", nil, 64)}, AddDatapointsGetSuccess)
			runtime.Gosched()
			time.Sleep(1 * time.Second) // wait for counts to be processed
			data := s.Datapoints()
			So(data, ShouldNotBeEmpty)
			t.Log(data)
			dpDropped, _, _, _, _, _ := ProcessDatapoints(data)
			So(dpDropped, ShouldEqual, 1)
		})
		Convey("should handle nil errors while emitting datapoints", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 7, "", "", "", "", newDefaultHTTPClient, nil, 3)
			s.ShutdownTimeout = time.Second * 5
			s.dpChannels[0].workers[0].handleError(nil, "HELLOOOOO", []*datapoint.Datapoint{Cumulative("metricname", nil, 64)}, AddDatapointsGetSuccess)
			runtime.Gosched()
			time.Sleep(1 * time.Second) // wait for counts to be processed
			data := s.Datapoints()
			So(data, ShouldNotBeEmpty)
			t.Log(data)
			dpDropped, _, _, _, _, _ := ProcessDatapoints(data)
			So(dpDropped, ShouldEqual, 0)
		})
		Convey("should handle errors and retry while emitting datapoints", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 7, "", "", "", "", newDefaultHTTPClient, nil, 3)
			s.ShutdownTimeout = time.Second * 5
			err := &SFXAPIError{
				StatusCode:   http.StatusRequestTimeout,
				ResponseBody: string("HELLO"),
			}
			s.dpChannels[0].workers[0].handleError(err, "HELLOOOOO", []*datapoint.Datapoint{Cumulative("metricname", nil, 64)}, AddDatapointsGetError)
			runtime.Gosched()
			time.Sleep(1 * time.Second) // wait for counts to be processed
			data := s.Datapoints()
			So(data, ShouldNotBeEmpty)
			t.Log(data)
			dpDropped, _, _, _, _, _ := ProcessDatapoints(data)
			So(dpDropped, ShouldEqual, 1)
		})
	})
}

func TestWorkerErrorHandlerEvents(t *testing.T) {
	Convey("An AsyncMultiTokeSink Worker Events", t, func() {
		Convey("should handle errors while emitting events", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 7, "", "", "", "", newDefaultHTTPClient, nil, 0)
			s.ShutdownTimeout = time.Second * 5
			s.evChannels[0].workers[0].handleError(fmt.Errorf("this is an error"), "HELLOOOOO", []*event.Event{event.New("TotalAlloc", event.COLLECTD, nil, time.Time{})}, AddEventsGetSuccess)
			time.Sleep(1 * time.Second) // wait for counts to be processed
			data := s.Datapoints()
			So(data, ShouldNotBeEmpty)
			t.Log(data)
			_, evDropped, _, _, _, _ := ProcessDatapoints(data)
			So(evDropped, ShouldEqual, 1)
		})
		Convey("should handle nil errors while emitting events", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 7, "", "", "", "", newDefaultHTTPClient, nil, 3)
			s.ShutdownTimeout = time.Second * 5
			s.evChannels[0].workers[0].handleError(nil, "HELLOOOOO", []*event.Event{event.New("TotalAlloc", event.COLLECTD, nil, time.Time{})}, AddEventsGetSuccess)
			time.Sleep(1 * time.Second) // wait for counts to be processed
			data := s.Datapoints()
			So(data, ShouldNotBeEmpty)
			t.Log(data)
			_, evDropped, _, _, _, _ := ProcessDatapoints(data)
			So(evDropped, ShouldEqual, 0)
		})
		Convey("should handle errors and retry while emitting events", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 7, "", "", "", "", newDefaultHTTPClient, nil, 3)
			s.ShutdownTimeout = time.Second * 5
			err := &SFXAPIError{
				StatusCode:   http.StatusRequestTimeout,
				ResponseBody: "HELLO",
			}
			s.evChannels[0].workers[0].handleError(err, "HELLOOOOO", []*event.Event{event.New("TotalAlloc", event.COLLECTD, nil, time.Time{})}, AddEventsGetError)
			time.Sleep(1 * time.Second) // wait for counts to be processed
			data := s.Datapoints()
			So(data, ShouldNotBeEmpty)
			t.Log(data)
			_, evDropped, _, _, _, _ := ProcessDatapoints(data)
			So(evDropped, ShouldEqual, 1)
		})
	})
}

// TODO (charlie) these sleeps you have aren't awesome, better to do a loop with runtime.GoSched
// TODO this is not guaranteed and makes tests take a lot longer. if you need to do scheduling, use a timekeeper you can incrememnt manually
func TestWorkerErrorHandlerSpans(t *testing.T) {
	Convey("An AsyncMultiTokeSink Worker Spans", t, func() {
		Convey("should handle errors while emitting traces", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 7, "", "", "", "", newDefaultHTTPClient, nil, 0)
			s.ShutdownTimeout = time.Second * 5
			s.spanChannels[0].workers[0].handleError(fmt.Errorf("this is an error"), "HELLOOOOO", []*trace.Span{{}}, AddSpansGetSuccess)
			time.Sleep(1 * time.Second) // wait for counts to be processed
			data := s.Datapoints()
			So(data, ShouldNotBeEmpty)
			t.Log(data)
			_, _, spanDropped, _, _, _ := ProcessDatapoints(data)
			So(spanDropped, ShouldEqual, 1)
		})
		Convey("should handle nil errors while emitting traces", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 7, "", "", "", "", newDefaultHTTPClient, nil, 3)
			s.ShutdownTimeout = time.Second * 5
			s.spanChannels[0].workers[0].handleError(nil, "HELLOOOOO", []*trace.Span{{}}, AddSpansGetSuccess)
			time.Sleep(1 * time.Second) // wait for counts to be processed
			data := s.Datapoints()
			So(data, ShouldNotBeEmpty)
			t.Log(data)
			_, _, spanDropped, _, _, _ := ProcessDatapoints(data)
			So(spanDropped, ShouldEqual, 0)
		})
		Convey("should handle errors and retry while emitting traces", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 7, "", "", "", "", newDefaultHTTPClient, nil, 3)
			s.ShutdownTimeout = time.Second * 5
			err := &SFXAPIError{
				StatusCode:   http.StatusRequestTimeout,
				ResponseBody: string("HELLO"),
			}
			s.spanChannels[0].workers[0].handleError(err, "HELLOOOOO", []*trace.Span{{}}, AddSpansGetError)
			time.Sleep(1 * time.Second) // wait for counts to be processed
			data := s.Datapoints()
			So(data, ShouldNotBeEmpty)
			t.Log(data)
			_, _, spanDropped, _, _, _ := ProcessDatapoints(data)
			So(spanDropped, ShouldEqual, 1)
		})
	})
}

func TestAsyncMultiTokenSinkShutdownDroppedDatapoints(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should raise an error if it's possible that datapoints were dropped", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 25, "", "", "", "", newDefaultHTTPClient, nil, 0)
			dps := GoMetricsSource.Datapoints()
			s.ShutdownTimeout = (time.Second * 0)
			// increase the number of datapoints added to the sink in a single call
			for i := 0; i < 3; i++ {
				dps = append(dps, GoMetricsSource.Datapoints()...)
			}
			// intentionally slow down emission to test shutdown timeout
			s.errorHandler = func(e error) error {
				time.Sleep(3 * time.Second)
				return DefaultErrorHandler(e)
			}
			for i := 0; i < 5; i++ {
				go func() {
					for i := 0; i < 500000; i++ {
						_ = s.AddDatapointsWithToken("HELLOOOOOO", dps)
					}
				}()
			}
			time.Sleep(500 * time.Millisecond) // wait half a second to start filling the buffer
			So(errors.Details(s.Close()), ShouldContainSubstring, "may have been dropped")
		})
	})
}

func TestAsyncMultiTokenSinkShutdownDroppedEvents(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should raise an error if it's possible that events were dropped", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 25, "", "", "", "", newDefaultHTTPClient, nil, 0)
			evs := GoEventSource.Events()
			s.ShutdownTimeout = (time.Second * 0)
			// increase the number of events added to the sink in a single call
			for i := 0; i < 3; i++ {
				evs = append(evs, GoEventSource.Events()...)
			}
			// intentionally slow down emission to test shutdown timeout
			s.errorHandler = func(e error) error {
				time.Sleep(3 * time.Second)
				return DefaultErrorHandler(e)
			}
			for i := 0; i < 5; i++ {
				go func() {
					for i := 0; i < 500000; i++ {
						_ = s.AddEventsWithToken("HELLOOOOOO", evs)
					}
				}()
			}
			time.Sleep(500 * time.Millisecond) // wait half a second to start filling the buffer
			So(errors.Details(s.Close()), ShouldContainSubstring, "may have been dropped")
		})
	})
}

func TestAsyncMultiTokenSinkShutdownDroppedSpans(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should raise an error if it's possible that spans were dropped", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 25, "", "", "", "", newDefaultHTTPClient, nil, 0)
			evs := GoSpanSource.Spans()
			s.ShutdownTimeout = time.Second * 0
			// increase the number of spans added to the sink in a single call
			for i := 0; i < 3; i++ {
				evs = append(evs, GoSpanSource.Spans()...)
			}
			// intentionally slow down emission to test shutdown timeout
			s.errorHandler = func(e error) error {
				time.Sleep(3 * time.Second)
				return DefaultErrorHandler(e)
			}
			for i := 0; i < 5; i++ {
				go func() {
					for i := 0; i < 500000; i++ {
						_ = s.AddSpansWithToken("HELLOOOOOO", evs)
					}
				}()
			}
			time.Sleep(500 * time.Millisecond) // wait half a second to start filling the buffer
			So(errors.Details(s.Close()), ShouldContainSubstring, "may have been dropped")
		})
	})
}

func TestAsyncMultiTokenSinkCleanCloseDatapoints(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should gracefully shutdown after some datapoints are added to it", func() {
			s := NewAsyncMultiTokenSink(int64(2), int64(2), 5, 2500, "", "", "", "", newDefaultHTTPClient, nil, 0)
			dps := GoMetricsSource.Datapoints()
			s.ShutdownTimeout = (time.Second * 5)

			go func() {
				for i := 0; i < 500000; i++ {
					_ = s.AddDatapointsWithToken("HELLOOOOOO", dps)
					_ = s.AddDatapointsWithToken("HELLOOOOOO2", dps)
				}
			}()

			time.Sleep(500 * time.Millisecond) // wait half a second to start filling the buffer
			So(s.Close(), ShouldBeNil)
		})
	})
}

func TestAsyncMultiTokenSinkCleanCloseEvents(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should gracefully shutdown after some events are added to it", func() {
			s := NewAsyncMultiTokenSink(int64(2), int64(2), 5, 2500, "", "", "", "", newDefaultHTTPClient, nil, 0)
			evs := GoEventSource.Events()
			s.ShutdownTimeout = (time.Second * 5)

			go func() {
				for i := 0; i < 500000; i++ {
					_ = s.AddEventsWithToken("HELLOOOOOO", evs)
					_ = s.AddEventsWithToken("HELLOOOOOO2", evs)
				}
			}()

			time.Sleep(500 * time.Millisecond) // wait half a second to start filling the buffer
			So(s.Close(), ShouldBeNil)
		})
	})
}

func TestAsyncMultiTokenSinkCleanCloseSpans(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should gracefully shutdown after some spans are added to it", func() {
			s := NewAsyncMultiTokenSink(int64(2), int64(2), 5, 2500, "", "", "", "", newDefaultHTTPClient, nil, 0)
			evs := GoSpanSource.Spans()
			s.ShutdownTimeout = time.Second * 5

			go func() {
				for i := 0; i < 500000; i++ {
					_ = s.AddSpansWithToken("HELLOOOOOO", evs)
					_ = s.AddSpansWithToken("HELLOOOOOO2", evs)
				}
			}()

			time.Sleep(500 * time.Millisecond) // wait half a second to start filling the buffer
			So(s.Close(), ShouldBeNil)
		})
	})
}

func TestAsyncTokenStatusCounter(t *testing.T) {
	s := NewAsyncTokenStatusCounter("testCounter", 5000, 1, map[string]string{"testdim1": "testdimval"})
	wg := sync.WaitGroup{}
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			d := &tokenStatus{
				status: http.StatusOK,
				token:  "HELLOOO",
				val:    5,
			}
			for i := 0; i < 5; i++ {
				s.Increment(d)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	time.Sleep(1 * time.Second)
	dps := s.Datapoints()
	Convey("An AsyncTokenStatusMap should be able to accept simultaneous calls to Increment", t, func() {
		So(dps, ShouldNotBeNil)
		So(dps[0].Value.(datapoint.IntValue).Int(), ShouldEqual, 125)
	})
}

func TestAsyncMultiTokenSinkCleanCloseDatapointsAndEvents(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should gracefully shutdown after some data is added to it", func() {
			s := NewAsyncMultiTokenSink(int64(2), int64(2), 5, 2500, "", "", "", "", newDefaultHTTPClient, nil, 0)
			dps := GoMetricsSource.Datapoints()
			evs := GoEventSource.Events()
			s.ShutdownTimeout = (time.Second * 5)

			go func() {
				for i := 0; i < 500000; i++ {
					_ = s.AddDatapointsWithToken("HELLOOOOOO", dps)
					_ = s.AddDatapointsWithToken("HELLOOOOOO2", dps)
				}
			}()

			go func() {
				for i := 0; i < 500000; i++ {
					_ = s.AddEventsWithToken("HELLOOOOOO", evs)
					_ = s.AddEventsWithToken("HELLOOOOOO2", evs)
				}
			}()

			time.Sleep(500 * time.Millisecond) // wait half a second to start filling the buffer
			So(s.Close(), ShouldBeNil)
		})
	})
}

func TestAsyncMultiTokenSinkHasherError(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		dps := GoMetricsSource.Datapoints()
		evs := GoEventSource.Events()
		spans := GoSpanSource.Spans()
		Convey("should not be able to add datapoints or events if the hasher is nil", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(3), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0)
			s.Hasher = nil
			So(s.AddDatapointsWithToken("HELLOOOOOO", dps), ShouldNotBeNil)
			So(s.AddEventsWithToken("HELLOOOOOO", evs), ShouldNotBeNil)
			So(s.AddSpansWithToken("HELLOOOOOO", spans), ShouldNotBeNil)
		})
		Convey("should not be able to add datapoints or events if there are no workers", func() {
			s := NewAsyncMultiTokenSink(int64(0), int64(0), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0)
			So(s.AddDatapointsWithToken("HELLOOOOOO", dps), ShouldNotBeNil)
			So(s.AddEventsWithToken("HELLOOOOOO", evs), ShouldNotBeNil)
			So(s.AddSpansWithToken("HELLOOOOOO", spans), ShouldNotBeNil)
		})
	})
}

func process(dp *datapoint.Datapoint, metric string, success bool) (count int64) {
	if dp.Metric == metric {
		for dim, val := range dp.Dimensions {
			if dim == "status" && (val == http.StatusText(http.StatusOK) == success) {
				count += dp.Value.(datapoint.IntValue).Int()
			}
		}
	}
	return
}

// ProcessDatapoints is a helper function for parsing out the datapoint values from an array of AsyncMultiTokenSink datapoints
func ProcessDatapoints(data []*datapoint.Datapoint) (dpDropped, evDropped, spansDropped, dpEmitted, evEmitted, spansEmitted int64) {
	for _, dp := range data {
		dpDropped += process(dp, "total_datapoints_by_token", false)
		evDropped += process(dp, "total_events_by_token", false)
		spansDropped += process(dp, "total_spans_by_token", false)
		dpEmitted += process(dp, "total_datapoints_by_token", true)
		evEmitted += process(dp, "total_events_by_token", true)
		spansEmitted += process(dp, "total_spans_by_token", true)
	}
	return
}

func TestAsyncMultiTokenSinkDatapoints(t *testing.T) {
	Convey("An AsyncMultiTokenSink", t, func() {
		Convey("should account for datapoints and events pushed through the sink", func() {
			s := NewAsyncMultiTokenSink(int64(1), int64(2), 5, 5000, "", "", "", "", newDefaultHTTPClient, nil, 0)
			dps := GoMetricsSource.Datapoints()
			evs := GoEventSource.Events()
			spans := GoSpanSource.Spans()
			ctx := context.Background()
			ctx = context.WithValue(ctx, TokenCtxKey, "HELLOOOOOO")
			s.ShutdownTimeout = time.Second * 5
			data := s.Datapoints()
			So(data, ShouldNotBeEmpty)
			for _, d := range data {
				t.Log(d)
			}
			dpDropped, evDropped, spansDropped, dpEmitted, evEmitted, spansEmitted := ProcessDatapoints(data)
			So(dpDropped, ShouldEqual, datapoint.NewIntValue(0))
			So(evDropped, ShouldEqual, datapoint.NewIntValue(0))
			So(spansDropped, ShouldEqual, datapoint.NewIntValue(0))
			So(dpEmitted, ShouldEqual, datapoint.NewIntValue(0))
			So(evEmitted, ShouldEqual, datapoint.NewIntValue(0))
			So(spansEmitted, ShouldEqual, datapoint.NewIntValue(0))
			t.Log("Adding events")
			s.AddEvents(ctx, evs)
			t.Log("Adding Datapoints")
			s.AddDatapointsWithToken("HELLOOOOOO", dps)
			t.Log("Adding Spans")
			s.AddSpans(ctx, spans)
			time.Sleep(time.Second * 3)
			data = s.Datapoints()
			So(data, ShouldNotBeEmpty)
			for _, d := range data {
				t.Log(d)
			}
			dpDropped, evDropped, spansDropped, dpEmitted, evEmitted, spansEmitted = ProcessDatapoints(data)
			So(dpDropped, ShouldEqual, datapoint.NewIntValue(int64(len(dps))))
			So(evDropped, ShouldEqual, datapoint.NewIntValue(int64(len(evs))))
			So(spansDropped, ShouldEqual, datapoint.NewIntValue(int64(len(spans))))
			So(dpEmitted, ShouldEqual, datapoint.NewIntValue(0))
			So(evEmitted, ShouldEqual, datapoint.NewIntValue(0))
			So(spansEmitted, ShouldEqual, datapoint.NewIntValue(0))
			err := s.Close() // close to ensure that all of the datapoints and events are processed
			data = s.Datapoints()
			So(len(data), ShouldEqual, 13) // only the data buffered and the batch sizes should be reported
			So(err, ShouldBeNil)
		})
	})
}

func BenchmarkAsyncMultiTokenSinkCreate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NewAsyncMultiTokenSink(int64(1), int64(1), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0)
	}
}

func BenchmarkAsyncMultiTokenSinkAddIndividualDatapoints(b *testing.B) {
	points := GoMetricsSource.Datapoints()
	sink := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0)
	ctx := context.Background()
	l := len(points)
	for i := 0; i < b.N; i++ {
		for j := 0; j < l; j++ {
			dp := make([]*datapoint.Datapoint, 0)
			dp = append(dp, points[j])
			_ = sink.AddDatapoints(ctx, dp)
		}
	}
}

func BenchmarkAsyncMultiTokenSinkAddSeveralDatapoints(b *testing.B) {
	points := GoMetricsSource.Datapoints()
	sink := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_ = sink.AddDatapoints(ctx, points)
	}
}

func BenchmarkAsyncMultiTokenSinkAddIndividualEvents(b *testing.B) {
	events := GoEventSource.Events()
	sink := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0)
	ctx := context.Background()
	l := len(events)
	for i := 0; i < b.N; i++ {
		for j := 0; j < l; j++ {
			ev := make([]*event.Event, 0)
			ev = append(ev, events[j])
			_ = sink.AddEvents(ctx, ev)
		}
	}
}

func BenchmarkAsyncMultiTokenSinkAddSeveralEvents(b *testing.B) {
	events := GoEventSource.Events()
	sink := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_ = sink.AddEvents(ctx, events)
	}
}

func BenchmarkAsyncMultiTokenSinkWithBufferAddIndividualDatapoints(b *testing.B) {
	points := GoMetricsSource.Datapoints()
	sink := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0)
	ctx := context.Background()
	l := len(points)
	for i := 0; i < b.N; i++ {
		for j := 0; j < l; j++ {
			dp := make([]*datapoint.Datapoint, 0)
			dp = append(dp, points[j])
			_ = sink.AddDatapoints(ctx, dp)
		}
	}
}

func BenchmarkAsyncMultiTokenSinkWithBufferAddSeveralDatapoints(b *testing.B) {
	points := GoMetricsSource.Datapoints()
	sink := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_ = sink.AddDatapoints(ctx, points)
	}
}

func BenchmarkAsyncMultiTokenSinkWithBufferAddIndividualEvents(b *testing.B) {
	events := GoEventSource.Events()
	sink := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0)
	ctx := context.Background()
	l := len(events)
	for i := 0; i < b.N; i++ {
		for j := 0; j < l; j++ {
			ev := make([]*event.Event, 0)
			ev = append(ev, events[j])
			_ = sink.AddEvents(ctx, ev)
		}
	}
}

func BenchmarkAsyncMultiTokenSinkWithBufferAddSeveralEvents(b *testing.B) {
	events := GoEventSource.Events()
	sink := NewAsyncMultiTokenSink(int64(1), int64(1), 5, 30, "", "", "", "", newDefaultHTTPClient, nil, 0)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_ = sink.AddEvents(ctx, events)
	}
}
