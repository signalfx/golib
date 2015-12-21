package log

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestPanicLogger(t *testing.T) {
	Convey("panic logger", t, func() {
		l := Panic
		Convey("should error to itself", func() {
			So(l.ErrorLogger(nil), ShouldEqual, l)
		})
		Convey("should panic", func() {
			So(func() {
				l.Log()
			}, ShouldPanic)
		})
	})
}
