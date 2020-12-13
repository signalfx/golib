package dpsink

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSinkDiscard(t *testing.T) {
	Convey("Discard sink should not error", t, func() {
		ctx := context.Background()
		So(Discard.AddEvents(ctx, nil), ShouldBeNil)
		So(Discard.AddDatapoints(ctx, nil), ShouldBeNil)
		So(Discard.AddSpans(ctx, nil), ShouldBeNil)
	})
}
