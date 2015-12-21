package log

import (
	"github.com/signalfx/golib/errors"
	"gopkg.in/logfmt.v0"
	"io"
	"io/ioutil"
)

// LogfmtLogger logs out in logfmt format
type LogfmtLogger struct {
	Out io.Writer
}

// NewLogfmtLogger returns a logger that encodes keyvals to the Writer in
// logfmt format. The passed Writer must be safe for concurrent use by
// multiple goroutines if the returned Logger will be used concurrently.
func NewLogfmtLogger(w io.Writer, ErrHandler ErrorHandler) Logger {
	if w == ioutil.Discard {
		return Discard
	}
	return &ErrorLogLogger{
		RootLogger: &LogfmtLogger{
			Out: w,
		},
		ErrHandler: ErrHandler,
	}
}

// Log writes the keyvalus in logfmt format to Out
func (l *LogfmtLogger) Log(keyvals ...interface{}) error {
	// The Logger interface requires implementations to be safe for concurrent
	// use by multiple goroutines. For this implementation that means making
	// only one call to l.w.Write() for each call to Log. We first collect all
	// of the bytes into b, and then call l.w.Write(b).
	b, err := logfmt.MarshalKeyvals(keyvals...)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := l.Out.Write(b); err != nil {
		return errors.Annotate(err, "cannot write out logfmt for log")
	}
	return nil
}
