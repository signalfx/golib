package writer

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/trace"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSpanWriter(t *testing.T) {
	t.Parallel()

	Convey("A trace writer", t, func() {
		var received []*trace.Span
		l := sync.Mutex{}

		ctx, cancel := context.WithCancel(context.Background())

		// Use atomic.Value to avoid race detection
		var shouldSend atomic.Value
		shouldSend.Store(true)

		var sendShouldFail atomic.Value
		sendShouldFail.Store(false)

		filter := func(inst *trace.Span) bool {
			return shouldSend.Load().(bool)
		}

		sender := func(ctx context.Context, insts []*trace.Span) error {
			if sendShouldFail.Load().(bool) {
				return errors.New("failed")
			}
			l.Lock()
			received = append(received, insts...)
			l.Unlock()
			return nil
		}

		writer := NewSpanRingWriter(filter, sender)
		ch := writer.InputChan()

		assertAllReceived := func(expectedCount int) {
			So(len(received), ShouldEqual, expectedCount)

			sort.SliceStable(received, func(i, j int) bool {
				return received[i].Meta["i"].(int) < received[j].Meta["i"].(int)
			})
			for i := 0; i < expectedCount; i++ {
				So(received[i].Meta["i"].(int), ShouldEqual, i)
			}
		}

		Convey("Should send all traces received", func() {
			writer.MaxBuffered = 20000
			go writer.Run(ctx)

			count := 0
			for i := 0; i < 10000; i++ {
				ch <- &trace.Span{Meta: map[string]interface{}{"i": i}}
				count++
			}

			cancel()
			writer.WaitForShutdown()

			So(len(received), ShouldEqual, count)
		})

		Convey("Should panic if waiting without starting", func() {
			So(writer.WaitForShutdown, ShouldPanic)
		})

		Convey("Should cycle buffer without losing anything", func() {
			writer.MaxBuffered = 7999
			go writer.Run(ctx)

			count := 0
			for i := 0; i < 20000; i++ {
				ch <- &trace.Span{Meta: map[string]interface{}{"i": i}}
				count++
			}

			cancel()
			writer.WaitForShutdown()

			assertAllReceived(count)
		})

		Convey("Should overflow cleanly", func() {
			writer.MaxBuffered = 4999
			go writer.Run(ctx)

			// Prevent things from being sent
			l.Lock()

			count := 0
			for i := 0; i < 10000; i++ {
				ch <- &trace.Span{Meta: map[string]interface{}{"i": i}}
				count++
			}

			go func() {
				// Wait to let input get processed a bit before letting things
				// through
				time.Sleep(3 * time.Second)
				l.Unlock()
				cancel()
			}()
			writer.WaitForShutdown()

			So(len(received), ShouldEqual, writer.MaxBuffered)
			sort.SliceStable(received, func(i, j int) bool {
				return received[i].Meta["i"].(int) < received[j].Meta["i"].(int)
			})
			for i := 0; i < writer.MaxBuffered; i++ {
				So(received[i].Meta["i"].(int), ShouldEqual, i+5001)
			}
			So(writer.totalPotentiallyDropped, ShouldEqual, 5001)
		})

		Convey("Should filter out traces", func() {
			shouldSend.Store(false)
			go writer.Run(ctx)

			count := 0
			for i := 0; i < 10000; i++ {
				ch <- &trace.Span{Meta: map[string]interface{}{"i": i}}
				count++
			}

			cancel()
			writer.WaitForShutdown()

			So(len(received), ShouldEqual, 0)
			So(findInternalMetricWithName(writer, "span_writer.filtered"), ShouldEqual, 10000)
		})

		Convey("Should report internal metrics", func() {
			go writer.Run(ctx)

			count := 0
			for i := 0; i < 10000; i++ {
				ch <- &trace.Span{Meta: map[string]interface{}{"i": i}}
				count++
				if count > 9990 {
					sendShouldFail.Store(true)
				}
			}

			cancel()
			writer.WaitForShutdown()

			So(findInternalMetricWithName(writer, "span_writer.received"), ShouldBeGreaterThan, 1)
			So(findInternalMetricWithName(writer, "span_writer.sent"), ShouldEqual, len(received))
			So(findInternalMetricWithName(writer, "span_writer.sent"), ShouldBeLessThan, 10000)
			So(findInternalMetricWithName(writer, "span_writer.failed"), ShouldEqual, 10000-len(received))
			So(findInternalMetricWithName(writer, "span_writer.failed"), ShouldBeLessThan, 10000)
			So(findInternalMetricWithName(writer, "span_writer.requests_active"), ShouldEqual, 0)
		})
	})
}

func ExampleSpanRingWriter() {
	client := sfxclient.NewHTTPSink()
	filterFunc := func(dp *trace.Span) bool {
		return dp.Meta["shouldSend"].(bool)
	}

	// filterFunc can also be nil if no filtering/modification is needed.
	writer := NewSpanRingWriter(filterFunc, client.AddSpans)

	ctx, cancel := context.WithCancel(context.Background())
	go writer.Run(ctx)

	in := writer.InputChan()

	// Send traces with the writer
	in <- &trace.Span{}

	// Close the context passed to Run()
	cancel()
	// Will wait for all pending traces to be written.
	writer.WaitForShutdown()
}
