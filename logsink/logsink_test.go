package logsink

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type end struct {
	count int64
}

func (e *end) AddLogs(ctx context.Context, logs []*Log) error {
	atomic.AddInt64(&e.count, 1)
	return nil
}

type middle struct{}

func (m *middle) AddLogs(ctx context.Context, logs []*Log, sink Sink) error {
	return sink.AddLogs(ctx, logs)
}

func TestSinkBasics(t *testing.T) {
	Convey("testing middle logs", t, func() {
		nextSink := &middle{}
		next := NextWrap(nextSink)
		So(next, ShouldNotBeNil)
		bottom := &end{}
		top := FromChain(bottom, next)
		So(top, ShouldNotBeNil)
		So(top.AddLogs(context.Background(), []*Log{}), ShouldBeNil)
		So(atomic.LoadInt64(&bottom.count), ShouldEqual, int64(1))
	})
}

func TestData(t *testing.T) {
	Convey("test some data", t, func() {
		tests := []struct {
			desc string
			json string
			err  error
		}{
			{"Otel Data", OTELJsonFormat, nil},
		}
		for _, test := range tests {
			test := test
			Convey(test.desc, func() {
				var logs Logs
				err := json.Unmarshal([]byte(test.json), &logs)
				if err != nil {
					Println(err)
				}
				So(err, ShouldEqual, test.err)
				_, err = json.Marshal(logs)
				if err != nil {
					Println(err)
				}
				So(err, ShouldEqual, test.err)
			})
		}
	})
}
