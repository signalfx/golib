package dpsink

import (
	"context"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/event"
	"github.com/signalfx/golib/v3/trace"
)

type discardSink struct{}

func (d discardSink) AddDatapoints(_ context.Context, _ []*datapoint.Datapoint) error {
	return nil
}

func (d discardSink) AddEvents(_ context.Context, _ []*event.Event) error {
	return nil
}

func (d discardSink) AddSpans(_ context.Context, _ []*trace.Span) error {
	return nil
}

// Discard is a datapoint sink that does nothing with points it gets
var Discard = discardSink{}
