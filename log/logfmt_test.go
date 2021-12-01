package log

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type marshallError struct{}

func (e *marshallError) MarshalText() (text []byte, err error) {
	return nil, &marshallError{}
}

func (e *marshallError) Error() string {
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
		Convey("should write messages", func() {
			l.Log("name", "john")
			So(strings.TrimSpace(buf.String()), ShouldResemble, "name=john")
		})
		Convey("should write old len messages", func() {
			l.Log("name")
			So(strings.TrimSpace(buf.String()), ShouldResemble, "message=name")
		})
		Convey("should write escaped messages", func() {
			l.Log("name", "john doe")
			So(strings.TrimSpace(buf.String()), ShouldResemble, `name="john doe"`)
		})
		Convey("should forward marshallError errors", func() {
			So(func() {
				l.Log(&marshallError{}, &marshallError{})
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
