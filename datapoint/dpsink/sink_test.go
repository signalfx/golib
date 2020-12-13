package dpsink

import (
	"context"
	"testing"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/datapoint/dptest"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/log"
	. "github.com/smartystreets/goconvey/convey"
)

type expect struct {
	count     int
	forwardTo Sink
}

func (e *expect) AddEvents(ctx context.Context, events []*event.Event) error {
	if len(events) != e.count {
		panic("NOPE")
	}
	if e.forwardTo != nil {
		events = append(events, nil)
		log.IfErr(log.Panic, e.forwardTo.AddEvents(ctx, events))
	}
	return nil
}

func (e *expect) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	if len(points) != e.count {
		panic("NOPE")
	}
	if e.forwardTo != nil {
		points = append(points, nil)
		log.IfErr(log.Panic, e.forwardTo.AddDatapoints(ctx, points))
	}
	return nil
}

func (e *expect) next(sendTo Sink) Sink {
	return &expect{
		count:     e.count,
		forwardTo: sendTo,
	}
}

func TestFromChain(t *testing.T) {
	e2 := expect{count: 2}
	e1 := expect{count: 1}
	e0 := expect{count: 0}

	chain := FromChain(&e2, e0.next, e1.next)
	log.IfErr(log.Panic, chain.AddDatapoints(context.Background(), []*datapoint.Datapoint{}))
	log.IfErr(log.Panic, chain.AddEvents(context.Background(), []*event.Event{}))
}

func TestIncludingDimensions(t *testing.T) {
	Convey("With a basic sink", t, func() {
		end := dptest.NewBasicSink()
		end.Resize(1)
		addInto := IncludingDimensions(map[string]string{"name": "jack"}, end)
		ctx := context.Background()
		Convey("no dimensions should be identity function", func() {
			addInto = IncludingDimensions(nil, end)
			So(addInto, ShouldEqual, end)
		})

		Convey("appending dims should work for datapoints", func() {
			dp := dptest.DP()
			So(addInto.AddDatapoints(ctx, []*datapoint.Datapoint{dp}), ShouldBeNil)
			dpOut := end.Next()
			So(dpOut.Dimensions["name"], ShouldEqual, "jack")

			w := &WithDimensions{}
			So(len(w.appendDimensions(nil)), ShouldEqual, 0)
		})
		Convey("appending dims should work for events", func() {
			e := dptest.E()
			So(addInto.AddEvents(ctx, []*event.Event{e}), ShouldBeNil)
			eOut := end.NextEvent()
			So(eOut.Dimensions["name"], ShouldEqual, "jack")

			w := &WithDimensions{}
			So(len(w.appendDimensionsEvents(nil)), ShouldEqual, 0)
		})
	})
}
