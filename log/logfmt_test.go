package log

import (
	"bytes"
	"errors"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

type errMarshall struct{}

func (e *errMarshall) MarshalText() (text []byte, err error) {
	return nil, &errMarshall{}
}

func (e *errMarshall) Error() string {
	return "I am an error"
}

type errWriter struct{}

func (e *errWriter) Write([]byte) (int, error) {
	return 0, errors.New("nope")
}

func TestNewLogfmtLogger(t *testing.T) {
	Convey("A NewLogfmtLogger logger", t, func() {
		buf := &bytes.Buffer{}
		l := NewLogfmtLogger(buf, Panic)
		Convey("should forward marshall errors", func() {
			So(func() {
				l.Log(&errMarshall{}, &errMarshall{})
			}, ShouldPanic)
		})
		Convey("should forward writer errors", func() {
			l := NewLogfmtLogger(&errWriter{}, Panic)
			So(func() {
				l.Log("hi", "hi")
			}, ShouldPanic)
		})
	})
}
