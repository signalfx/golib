package dpsink

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSinkDiscard(t *testing.T) {
	Convey("Discard sink should not error", t, func() {
		So(Discard.AddEvents(nil, nil), ShouldBeNil)
		So(Discard.AddDatapoints(nil, nil), ShouldBeNil)
		So(Discard.AddSpans(nil, nil), ShouldBeNil)
	})
}
