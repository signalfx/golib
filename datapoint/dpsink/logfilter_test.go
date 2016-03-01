package dpsink

import (
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/event"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
	"testing"
	"time"
)

type boolFlagCheck bool

func (b *boolFlagCheck) HasFlag(ctx context.Context) bool {
	return bool(*b)
}

func TestFilter(t *testing.T) {
	Convey("With item flagger", t, func() {
		flagCheck := boolFlagCheck(false)
		i := &ItemFlagger{
			CtxFlagCheck:        &flagCheck,
			EventMetaName:       "my_events",
			MetricDimensionName: "sf_metric",
		}
		dp1 := datapoint.New("mname", map[string]string{"org": "mine", "type": "prod"}, nil, datapoint.Gauge, time.Time{})
		dp2 := datapoint.New("mname2", map[string]string{"org": "another", "type": "prod"}, nil, datapoint.Gauge, time.Time{})

		ev1 := event.New("mname", "", map[string]string{"org": "mine", "type": "prod"}, time.Time{})
		ev2 := event.New("mname2", "", map[string]string{"org": "another", "type": "prod"}, time.Time{})
		chain := FromChain(Discard, NextWrap(i))
		ctx := context.Background()
		So(len(i.Datapoints()), ShouldEqual, 4)
		Convey("nil item flaggers should support Read operations", func() {
			i = nil
			So(i.HasDatapointFlag(dp1), ShouldBeFalse)
			So(i.HasEventFlag(ev1), ShouldBeFalse)
		})
		Convey("should not flag by default", func() {
			So(chain.AddDatapoints(ctx, []*datapoint.Datapoint{dp1, dp2}), ShouldBeNil)
			So(i.HasDatapointFlag(dp1), ShouldBeFalse)
			So(i.HasDatapointFlag(dp2), ShouldBeFalse)

			So(chain.AddEvents(ctx, []*event.Event{ev1, ev2}), ShouldBeNil)
			So(i.HasEventFlag(ev1), ShouldBeFalse)
			So(i.HasEventFlag(ev2), ShouldBeFalse)
		})
		Convey("should flag if context is flagged", func() {
			flagCheck = boolFlagCheck(true)
			So(chain.AddDatapoints(ctx, []*datapoint.Datapoint{dp1, dp2}), ShouldBeNil)
			So(i.HasDatapointFlag(dp1), ShouldBeTrue)
			So(i.HasDatapointFlag(dp2), ShouldBeTrue)

			So(chain.AddEvents(ctx, []*event.Event{ev1, ev2}), ShouldBeNil)
			So(i.HasEventFlag(ev1), ShouldBeTrue)
			So(i.HasEventFlag(ev2), ShouldBeTrue)
		})
		Convey("should flag if dimensions are flagged", func() {
			i.SetDimensions(map[string]string{"org": "mine"})
			So(i.Var().String(), ShouldContainSubstring, "mine")
			So(chain.AddDatapoints(ctx, []*datapoint.Datapoint{dp1, dp2}), ShouldBeNil)
			So(i.HasDatapointFlag(dp1), ShouldBeTrue)
			So(i.HasDatapointFlag(dp2), ShouldBeFalse)

			So(chain.AddEvents(ctx, []*event.Event{ev1, ev2}), ShouldBeNil)
			So(i.HasEventFlag(ev1), ShouldBeTrue)
			So(i.HasEventFlag(ev2), ShouldBeFalse)
		})

		Convey("should flag if metric is flagged", func() {
			i.SetDimensions(map[string]string{"sf_metric": "mname2"})
			So(chain.AddDatapoints(ctx, []*datapoint.Datapoint{dp1, dp2}), ShouldBeNil)
			So(i.HasDatapointFlag(dp1), ShouldBeFalse)
			So(i.HasDatapointFlag(dp2), ShouldBeTrue)
		})
	})
}
