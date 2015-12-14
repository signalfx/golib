package sfxclient

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

type errReader struct {
	shouldBlock chan struct{}
}

var errReadErr = errors.New("read bad")

func (e *errReader) Read(_ []byte) (n int, err error) {
	if e.shouldBlock != nil {
		<-e.shouldBlock
	}
	return 0, errReadErr
}

func TestHelperFunctions(t *testing.T) {
	Convey("Just helpers", t, func() {
		Convey("mapToDimensions should filter empty", func() {
			So(len(mapToDimensions(map[string]string{"": "hi"})), ShouldEqual, 0)
		})
	})
}

func TestHTTPDatapointSink(t *testing.T) {
	Convey("A default sink", t, func() {
		s := NewHTTPDatapointSink()
		ctx := context.Background()
		dps := GoMetricsSource.Datapoints()
		Convey("should timeout", func() {
			s.Client.Timeout = time.Millisecond
			So(s.AddDatapoints(ctx, dps), ShouldNotBeNil)
		})
		Convey("should not try dead contexts", func() {
			ctx, can := context.WithCancel(ctx)
			can()
			So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "context already closed")
			Convey("but empty points should always work", func() {
				So(s.AddDatapoints(ctx, []*datapoint.Datapoint{}), ShouldBeNil)
			})
		})
		Convey("should check failure to encode", func() {
			s.protoMarshaler = func(pb proto.Message) ([]byte, error) {
				return nil, errors.New("failure to encode")
			}
			So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "failure to encode")
		})
		Convey("should check invalid endpoints", func() {
			s.Endpoint = "%gh&%ij"
			So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "cannot parse new HTTP request to")
		})
		Convey("reading the full body should be checked", func() {
			resp := &http.Response{
				Body: ioutil.NopCloser(&errReader{}),
			}
			So(errors.Tail(s.handleResponse(resp, nil)), ShouldEqual, errReadErr)
		})
		Convey("with a test endpoint", func() {
			retString := `"OK"`
			retCode := http.StatusOK
			var blockResponse chan struct{}
			var cancelCallback func()
			seenBodyPoints := &com_signalfx_metrics_protobuf.DataPointUploadMessage{}
			ingestHappening := sync.WaitGroup{}
			ingest := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				ingestHappening.Add(1)
				defer ingestHappening.Done()
				bodyBytes := bytes.Buffer{}
				io.Copy(&bodyBytes, req.Body)
				req.Body.Close()
				proto.Unmarshal(bodyBytes.Bytes(), seenBodyPoints)
				rw.WriteHeader(retCode)
				io.WriteString(rw, retString)
				if blockResponse != nil {
					if cancelCallback != nil {
						cancelCallback()
					}
					select {
					case <-req.Cancel:
					case <-blockResponse:
					}
				}
			}))
			s.Endpoint = ingest.URL
			Convey("Send should normally work", func() {
				So(s.AddDatapoints(ctx, dps), ShouldBeNil)
			})
			Convey("Floats should work", func() {
				dps[0].Value = datapoint.NewFloatValue(1.0)
				dps = dps[0:1]
				So(s.AddDatapoints(ctx, dps), ShouldBeNil)
				So(len(seenBodyPoints.Datapoints), ShouldEqual, 1)
				So(*seenBodyPoints.Datapoints[0].Value.DoubleValue, ShouldEqual, 1.0)
			})
			Convey("Strings should work", func() {
				dps[0].Value = datapoint.NewStringValue("hi")
				dps = dps[0:1]
				So(s.AddDatapoints(ctx, dps), ShouldBeNil)
				So(len(seenBodyPoints.Datapoints), ShouldEqual, 1)
				So(*seenBodyPoints.Datapoints[0].Value.StrValue, ShouldEqual, "hi")
			})
			Convey("empty key filtering should happen", func() {
				dps[0].Dimensions = map[string]string{"": "hi"}
				dps = dps[0:1]
				So(s.AddDatapoints(ctx, dps), ShouldBeNil)
				So(len(seenBodyPoints.Datapoints[0].Dimensions), ShouldEqual, 0)
			})
			Convey("invalid rune filtering should happen", func() {
				dps[0].Dimensions = map[string]string{"hi.bob": "hi"}
				dps = dps[0:1]
				So(s.AddDatapoints(ctx, dps), ShouldBeNil)
				So(*seenBodyPoints.Datapoints[0].Dimensions[0].Key, ShouldEqual, "hi_bob")
			})
			Convey("Invalid datapoints should panic", func() {
				dps[0].MetricType = datapoint.MetricType(1001)
				So(func() { s.AddDatapoints(ctx, dps) }, ShouldPanic)
			})
			Convey("return code should be checked", func() {
				retCode = http.StatusNotAcceptable
				So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "invalid status code")
			})
			Convey("return string should be checked", func() {
				retString = `"nope"`
				So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "invalid response body")
				retString = `INVALID_JSON`
				So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "cannot unmarshal response body")
			})
			Convey("context cancel should work", func() {
				blockResponse = make(chan struct{})
				ctx, cancelCallback = context.WithCancel(ctx)
				So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "request canceled")
			})
			Convey("timeouts should work", func() {
				blockResponse = make(chan struct{})
				ctx, _ = context.WithCancel(ctx)
				s.Client.Timeout = time.Millisecond * 10
				So(errors.Details(s.AddDatapoints(ctx, dps)), ShouldContainSubstring, "Timeout exceeded")
			})
			Reset(func() {
				if blockResponse != nil {
					close(blockResponse)
				}
				//              TODO: Race detector if I uncomment ingest.Close().  I can't figure out the correct way to Close() ingest
				//					while also slowing down the http request
				//				ingest.Close()
			})
		})
	})
}
