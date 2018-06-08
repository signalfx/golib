package sfxclient

import (
	"math/rand"
	"sync"
	"testing"

	"context"

	"github.com/signalfx/golib/log"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCumulativeCollectorThreadRaces(t *testing.T) {
	cc := &CumulativeCollector{
		MetricName: "mname",
		Dimensions: map[string]string{"type": "dev"},
	}
	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				cc.Add(1)
			}
		}()
	}
	go func() {
		for q := 0; q < 1000; q++ {
			cc.Datapoints()
		}
	}()
	wg.Wait()

	dps := cc.Datapoints()
	Convey("Sum should be consistent", t, func() {
		So(dps[0].Value.String(), ShouldEqual, "100000")
	})
}

func TestCumulativeCollector(t *testing.T) {
	Convey("When counter is setup", t, func() {
		cb := &CumulativeCollector{
			MetricName: "mname",
			Dimensions: map[string]string{"type": "dev"},
		}
		Convey("Empty counter should be ok", func() {
			dps := cb.Datapoints()
			So(len(dps), ShouldEqual, 1)
			So(dpNamed("mname", dps).Value.String(), ShouldEqual, "0")
		})
		Convey("No metric name should not send", func() {
			cb.MetricName = ""
			dps := cb.Datapoints()
			So(len(dps), ShouldEqual, 0)
		})
		Convey("adding a single point should make sense", func() {
			cb.Add(100)

			dps := cb.Datapoints()
			So(len(dps), ShouldEqual, 1)
			So(dpNamed("mname", dps).Value.String(), ShouldEqual, "100")
		})
	})
}

func BenchmarkCumulativeCollector(b *testing.B) {
	cb := &CumulativeCollector{}
	r := rand.New(rand.NewSource(0))
	for i := 0; i < b.N; i++ {
		cb.Add(int64(r.Intn(1024)))
	}
}

func BenchmarkCumulativeCollector10(b *testing.B) {
	benchCC(b, 10)
}

func benchCC(b *testing.B, numGoroutine int) {
	cb := &CumulativeCollector{}
	w := sync.WaitGroup{}
	w.Add(numGoroutine)
	for g := 0; g < numGoroutine; g++ {
		go func(g int) {
			r := rand.New(rand.NewSource(0))
			for i := g; i < b.N; i += numGoroutine {
				cb.Add(int64(r.Intn(1024)))
			}
			w.Done()
		}(g)
	}
	w.Wait()
}

func BenchmarkCumulativeCollector100(b *testing.B) {
	benchCC(b, 100)
}

func ExampleCumulativeCollector() {
	cb := &CumulativeCollector{
		MetricName: "mname",
		Dimensions: map[string]string{"type": "dev"},
	}
	cb.Add(1)
	cb.Add(3)
	client := NewHTTPSink()
	ctx := context.Background()
	// Will expect it to send mname=4
	log.IfErr(log.Panic, client.AddDatapoints(ctx, cb.Datapoints()))
}
