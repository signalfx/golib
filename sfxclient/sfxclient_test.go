package sfxclient

import (
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fmt"

	"context"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/timekeeper/timekeepertest"
	. "github.com/smartystreets/goconvey/convey"
)

type testSink struct {
	retErr         error
	lastDatapoints chan []*datapoint.Datapoint
}

func (t *testSink) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) (err error) {
	t.lastDatapoints <- points
	return t.retErr
}

var (
	collectors []Collector
)

func addBucketValues(bucket *RollingBucket, wg *sync.WaitGroup) {
	rangeInt := 10
	for idx := 0; idx < rangeInt; idx++ {
		bucket.Add(float64(idx) * 2.233)
	}
	wg.Done()
}

func setupCollectors(collectorsCount int) {
	wg := sync.WaitGroup{}
	ccCollectorRange := 10
	wg.Add(collectorsCount - ccCollectorRange - 1)
	for idx := 0; idx < collectorsCount; idx++ {
		if idx < ccCollectorRange {
			cb := &CumulativeBucket{
				MetricName: "dataSet" + strconv.Itoa(idx),
				Dimensions: map[string]string{"type": "ccBucket"},
			}
			cb.Add(100)
			collectors = append(collectors, cb)
		} else if idx == ccCollectorRange {
			collectors = append(collectors, GoMetricsSource)
		} else {
			metricName := "dataSet" + strconv.Itoa(idx)
			bucket := NewRollingBucket(metricName, map[string]string{"type": "regularBucket"})
			go addBucketValues(bucket, &wg)
			collectors = append(collectors, bucket)
		}
	}
	wg.Wait()
}

func TestScheduler_ReportOnce(t *testing.T) {
	var handleErrors []error
	var handleErrRet error
	s := &Scheduler{
		Sink: &testSink{
			lastDatapoints: make(chan []*datapoint.Datapoint, 1),
		},
		Timer: timekeepertest.NewStubClock(time.Now()),
		ErrorHandler: func(e error) error {
			handleErrors = append(handleErrors, e)
			return errors.Wrap(handleErrRet, e)
		},
		ReportingDelayNs: time.Second.Nanoseconds(),
		callbackMap:      make(map[string]*callbackPair),
	}

	setupCollectors(25)

	ctx := context.Background()
	for _, collector := range collectors {
		s.AddCallback(collector)
	}
	Convey("testing report once", t, func() {
		So(s.ReportOnce(ctx), ShouldBeNil)
	})
}

func TestScheduler_ReportingTimeout(t *testing.T) {
	var handleErrors []error
	var handleErrRet error
	s := &Scheduler{
		Sink: &testSink{
			lastDatapoints: make(chan []*datapoint.Datapoint, 1),
		},
		ErrorHandler: func(e error) error {
			handleErrors = append(handleErrors, e)
			return errors.Wrap(handleErrRet, e)
		},
		ReportingDelayNs: time.Second.Nanoseconds(),
		callbackMap:      make(map[string]*callbackPair),
	}
	tk := timekeepertest.NewStubClock(time.Now())
	s.Timer = tk

	ctx := context.Background()
	Convey("testing longer processing time for ReportOnce", t, func() {
		s.ReportingTimeout(time.Nanosecond)
		setupCollectors(250 * 10)
		for _, collector := range collectors {
			s.AddCallback(collector)
		}
		go s.Schedule(ctx)
		for atomic.LoadInt64(&s.stats.reportingTimeoutCounts) == 0 {
			tk.Incr(time.Duration(s.ReportingDelayNs))
			runtime.Gosched()
		}
		So(atomic.LoadInt64(&s.stats.reportingTimeoutCounts), ShouldEqual, int64(1))
	})
}

func TestCollectDatapointDebug(t *testing.T) {
	var handleErrors []error
	var handleErrRet error
	s := &Scheduler{
		ErrorHandler: func(e error) error {
			handleErrors = append(handleErrors, e)
			return errors.Wrap(handleErrRet, e)
		},
		ReportingDelayNs: time.Second.Nanoseconds(),
		callbackMap:      make(map[string]*callbackPair),
	}
	sink := &testSink{
		lastDatapoints: make(chan []*datapoint.Datapoint, 1),
	}
	s.Sink = sink
	tk := timekeepertest.NewStubClock(time.Now())
	s.Timer = tk

	ctx := context.Background()
	Convey("testing collect datapoints with debug mode enabled", t, func() {
		s.Debug(true)
		s.AddCallback(GoMetricsSource)
		s.AddGroupedCallback("goMetrics", GoMetricsSource)
		go s.Schedule(ctx)
		for atomic.LoadInt64(&s.stats.scheduledSleepCounts) == 0 {
			runtime.Gosched()
			tk.Incr(time.Duration(s.ReportingDelayNs))
			runtime.Gosched()
		}
		dps := <-sink.lastDatapoints
		So(len(dps), ShouldEqual, 60)
	})
}

func TestNewScheduler(t *testing.T) {
	Convey("Default error handler should not panic", t, func() {
		So(func() { errors.PanicIfErr(DefaultErrorHandler(errors.New("test")), "unexpected") }, ShouldNotPanic)
	})

	Convey("with a testing scheduler", t, func() {
		s := NewScheduler()

		tk := timekeepertest.NewStubClock(time.Now())
		s.Timer = tk

		sink := &testSink{
			lastDatapoints: make(chan []*datapoint.Datapoint, 1),
		}
		s.Sink = sink
		s.ReportingDelay(time.Second)

		var handledErrors []error
		var handleErrRet error
		var mu sync.Mutex
		s.ErrorHandler = func(e error) error {
			mu.Lock()
			handledErrors = append(handledErrors, e)
			mu.Unlock()
			return errors.Wrap(handleErrRet, e)
		}

		ctx := context.Background()

		Convey("removing a callback that doesn't exist should work", func() {
			s.RemoveCallback(nil)
			s.RemoveGroupedCallback("_", nil)
		})
		Convey("CollectorFunc should work", func() {
			c := 0
			cf := CollectorFunc(func() []*datapoint.Datapoint {
				c++
				return []*datapoint.Datapoint{}
			})
			So(len(cf.Datapoints()), ShouldEqual, 0)
			So(c, ShouldEqual, 1)
			s.AddCallback(cf)
			So(len(s.CollectDatapoints()), ShouldEqual, 0)
		})

		Convey("callbacks should be removable", func() {
			s.AddCallback(GoMetricsSource)
			So(s.ReportOnce(ctx), ShouldBeNil)
			firstPoints := <-sink.lastDatapoints
			So(len(firstPoints), ShouldEqual, 30)
			s.RemoveCallback(GoMetricsSource)
			So(s.ReportOnce(ctx), ShouldBeNil)
			firstPoints = <-sink.lastDatapoints
			So(len(firstPoints), ShouldEqual, 0)
		})

		Convey("datapoints should have the prefix if scheduler specifies", func() {
			s.AddCallback(GoMetricsSource)
			s.Prefix = "prefix"
			So(s.ReportOnce(ctx), ShouldBeNil)
			firstPoints := <-sink.lastDatapoints
			So(len(firstPoints), ShouldEqual, 30)
			for _, firstPoint := range firstPoints {
				So(strings.HasPrefix(firstPoint.Metric, s.Prefix), ShouldBeTrue)
			}
			s.RemoveCallback(GoMetricsSource)
			So(s.ReportOnce(ctx), ShouldBeNil)
			firstPoints = <-sink.lastDatapoints
			So(len(firstPoints), ShouldEqual, 0)
		})

		Convey("a single report should work", func() {
			So(s.ReportOnce(ctx), ShouldBeNil)
			firstPoints := <-sink.lastDatapoints
			So(len(firstPoints), ShouldEqual, 0)
			So(len(sink.lastDatapoints), ShouldEqual, 0)
		})
		Convey("with a single callback", func() {
			dimsToSend := map[string]string{"type": "test"}
			collector := CollectorFunc(func() []*datapoint.Datapoint {
				return []*datapoint.Datapoint{
					Gauge("mname", dimsToSend, 1),
				}
			})
			s.AddCallback(collector)
			Convey("empty dims should be sendable", func() {
				dimsToSend = nil
				So(s.ReportOnce(ctx), ShouldBeNil)
				firstPoints := <-sink.lastDatapoints
				So(len(firstPoints), ShouldEqual, 1)
				So(firstPoints[0].Dimensions, ShouldResemble, map[string]string{})
				Convey("so should only defaults", func() {
					s.DefaultDimensions(map[string]string{"host": "bob"})
					So(s.ReportOnce(ctx), ShouldBeNil)
					firstPoints := <-sink.lastDatapoints
					So(len(firstPoints), ShouldEqual, 1)
					So(firstPoints[0].Dimensions, ShouldResemble, map[string]string{"host": "bob"})
				})
			})
			Convey("default dimensions should set", func() {
				s.DefaultDimensions(map[string]string{"host": "bob"})
				s.GroupedDefaultDimensions("_", map[string]string{"host": "bob2"})
				So(s.ReportOnce(ctx), ShouldBeNil)
				firstPoints := <-sink.lastDatapoints
				So(len(firstPoints), ShouldEqual, 1)
				So(firstPoints[0].Dimensions, ShouldResemble, map[string]string{"host": "bob", "type": "test"})
				So(firstPoints[0].Metric, ShouldEqual, "mname")
				So(firstPoints[0].MetricType, ShouldEqual, datapoint.Gauge)
				So(firstPoints[0].Timestamp.UnixNano(), ShouldEqual, tk.Now().UnixNano())
				Convey("and var should return previous points", func() {
					So(s.Var().String(), ShouldContainSubstring, "bob")
				})
			})
			Convey("zero time should be sendable", func() {
				s.SendZeroTime = true
				So(s.ReportOnce(ctx), ShouldBeNil)
				firstPoints := <-sink.lastDatapoints
				So(len(firstPoints), ShouldEqual, 1)
				So(firstPoints[0].Timestamp.IsZero(), ShouldBeTrue)
			})
			Convey("and made to error out", func() {
				sink.retErr = errors.New("nope bad done")
				handleErrRet = errors.New("handle error is bad")
				Convey("scheduled should end", func() {
					scheduleOver := int64(0)
					go func() {
						for atomic.LoadInt64(&scheduleOver) == 0 {
							tk.Incr(time.Duration(s.ReportingDelayNs))
							runtime.Gosched()
						}
					}()
					err := s.Schedule(ctx)
					atomic.StoreInt64(&scheduleOver, 1)
					So(err.Error(), ShouldEqual, "nope bad done")
					So(errors.Details(err), ShouldContainSubstring, "handle error is bad")
				})
			})
			Convey("and scheduled", func() {
				scheduledContext, cancelFunc := context.WithCancel(ctx)
				scheduleRetErr := make(chan error)
				go func() {
					scheduleRetErr <- s.Schedule(scheduledContext)
				}()
				Convey("should collect when time advances", func() {
					for len(sink.lastDatapoints) == 0 {
						tk.Incr(time.Duration(s.ReportingDelayNs))
						runtime.Gosched()
					}
					firstPoints := <-sink.lastDatapoints
					So(len(firstPoints), ShouldEqual, 1)
					Convey("and should skip an interval if we sleep too long", func() {
						// Should eventually end
						for atomic.LoadInt64(&s.stats.resetIntervalCounts) == 0 {
							// Keep skipping time and draining outstanding points until we skip an interval
							tk.Incr(time.Duration(s.ReportingDelayNs) * 3)
							for len(sink.lastDatapoints) > 0 {
								<-sink.lastDatapoints
							}
							runtime.Gosched()
						}
					})
				})
				Reset(func() {
					cancelFunc()
					<-scheduleRetErr
				})
			})
			Reset(func() {
				s.RemoveCallback(collector)
			})
		})

		Reset(func() {
			close(sink.lastDatapoints)
		})
	})
}

func ExampleScheduler() {
	s := NewScheduler()
	s.Sink.(*HTTPSink).AuthToken = "ABCD-XYZ"
	s.AddCallback(GoMetricsSource)
	bucket := NewRollingBucket("req.time", map[string]string{"env": "test"})
	s.AddCallback(bucket)
	bucket.Add(1.2)
	bucket.Add(3)
	ctx := context.Background()
	err := s.Schedule(ctx)
	fmt.Println("Schedule result: ", err)
}

func BenchmarkScheduler_ReportOnce(b *testing.B) {
	var handleErrors []error
	var handleErrRet error
	s := &Scheduler{
		ErrorHandler: func(e error) error {
			handleErrors = append(handleErrors, e)
			return errors.Wrap(handleErrRet, e)
		},
		ReportingDelayNs: time.Second.Nanoseconds(),
		callbackMap:      make(map[string]*callbackPair),
	}
	tk := timekeepertest.NewStubClock(time.Now())
	s.Timer = tk
	sink := &testSink{
		lastDatapoints: make(chan []*datapoint.Datapoint, 1),
	}
	s.Sink = sink

	ctx := context.Background()
	totalCb := 20
	for idx := 0; idx < totalCb; idx++ {
		if idx < 5 {
			s.AddCallback(GoMetricsSource)
		} else if idx < 10 {
			s.AddGroupedCallback("group1", GoMetricsSource)
		} else {
			s.AddGroupedCallback("group2", GoMetricsSource)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; b.N < n; n++ {
		go s.Schedule(ctx)
		for atomic.LoadInt64(&s.stats.scheduledSleepCounts) == 0 {
			runtime.Gosched()
			tk.Incr(time.Duration(s.ReportingDelayNs))
			runtime.Gosched()
		}
		dps := <-sink.lastDatapoints
		So(len(dps), ShouldEqual, 30*totalCb)
	}
}

func BenchmarkScheduler_ReportOnce_With_Debug(b *testing.B) {
	var handleErrors []error
	var handleErrRet error
	s := &Scheduler{
		ErrorHandler: func(e error) error {
			handleErrors = append(handleErrors, e)
			return errors.Wrap(handleErrRet, e)
		},
		debug:            true,
		ReportingDelayNs: time.Second.Nanoseconds(),
		callbackMap:      make(map[string]*callbackPair),
	}
	tk := timekeepertest.NewStubClock(time.Now())
	s.Timer = tk
	sink := &testSink{
		lastDatapoints: make(chan []*datapoint.Datapoint, 1),
	}
	s.Sink = sink

	ctx := context.Background()
	totalCb := 20
	for idx := 0; idx < totalCb; idx++ {
		if idx < 5 {
			s.AddCallback(GoMetricsSource)
		} else if idx < 10 {
			s.AddGroupedCallback("group1", GoMetricsSource)
		} else {
			s.AddGroupedCallback("group2", GoMetricsSource)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; b.N < n; n++ {
		go s.Schedule(ctx)
		for atomic.LoadInt64(&s.stats.scheduledSleepCounts) == 0 {
			runtime.Gosched()
			tk.Incr(time.Duration(s.ReportingDelayNs))
			runtime.Gosched()
		}
		dps := <-sink.lastDatapoints
		So(len(dps), ShouldEqual, 30*totalCb)
	}
}
