package clientcfg

import (
	"context"
	"errors"
	"net/url"
	"testing"

	"github.com/signalfx/golib/v3/datapoint/dptest"
	"github.com/signalfx/golib/v3/distconf"
	"github.com/signalfx/golib/v3/log"
	"github.com/signalfx/golib/v3/sfxclient"
	. "github.com/smartystreets/goconvey/convey"
)

func TestClient(t *testing.T) {
	Convey("With a testing zk", t, func() {
		mem := distconf.Mem()
		dconf := distconf.New([]distconf.Reader{mem})
		conf := &ClientConfig{}
		logger := log.Discard
		conf.Load(dconf)
		ctx := context.Background()
		Convey("normal sinks should just forward", func() {
			tsink := dptest.NewBasicSink()
			tsink.Resize(1)
			sink := WatchSinkChanges(tsink, conf, logger)
			So(sink, ShouldEqual, tsink)
			So(sink.AddDatapoints(ctx, nil), ShouldBeNil)
		})
		Convey("default dimensions", func() {
			Convey("should use sourceName", func() {
				mem.Write("signalfuse.sourceName", []byte("h1"))
				dims, err := DefaultDimensions(conf)
				So(err, ShouldBeNil)
				So(dims, ShouldResemble, map[string]string{"sf_source": "h1"})
			})
			Convey("should use hostname", func() {
				conf.OsHostname = func() (name string, err error) {
					return "ahost", nil
				}
				dims, err := DefaultDimensions(conf)
				So(err, ShouldBeNil)
				So(dims, ShouldResemble, map[string]string{"sf_source": "ahost"})
			})
			Convey("check hostname error", func() {
				conf.OsHostname = func() (name string, err error) {
					return "", errors.New("nope")
				}
				dims, err := DefaultDimensions(conf)
				So(err, ShouldNotBeNil)
				So(dims, ShouldBeNil)
			})
		})
		Convey("HTTPSink should wrap", func() {
			hsink := sfxclient.NewHTTPSink()
			sink, ok := WatchSinkChanges(hsink, conf, logger).(*ClientConfigChangerSink)
			So(ok, ShouldBeTrue)
			So(sink, ShouldNotEqual, hsink)
			hsink.DatapointEndpoint = ""
			So(sink.AddDatapoints(ctx, nil), ShouldBeNil)
			So(sink.AddSpans(ctx, nil), ShouldBeNil)
			So(sink.AddEvents(ctx, nil), ShouldBeNil)
			Convey("Only hostname should append v2/datapoint", func() {
				mem.Write("sf.metrics.statsendpoint", []byte("https://ingest-2.signalfx.com"))
				So(hsink.DatapointEndpoint, ShouldEqual, "https://ingest-2.signalfx.com/v2/datapoint")
				Convey("Failing URL parses should be OK", func() {
					sink.urlParse = func(string) (*url.URL, error) {
						return nil, errors.New("nope")
					}
					mem.Write("sf.metrics.statsendpoint", []byte("_will_not_parse"))
					So(hsink.DatapointEndpoint, ShouldEqual, "https://ingest-2.signalfx.com/v2/datapoint")
				})
			})
			Convey("http should add by default", func() {
				mem.Write("sf.metrics.statsendpoint", []byte("ingest-3.signalfx.com:28080"))
				So(hsink.DatapointEndpoint, ShouldEqual, "http://ingest-3.signalfx.com:28080/v2/datapoint")
			})
			Convey("direct URL should be used", func() {
				mem.Write("sf.metrics.statsendpoint", []byte("https://ingest-4.signalfx.com/v2"))
				So(hsink.DatapointEndpoint, ShouldEqual, "https://ingest-4.signalfx.com/v2")
			})
		})
	})
}

func TestSetupSinkClientChanger(t *testing.T) {
	mem := distconf.Mem()
	dconf := distconf.New([]distconf.Reader{mem})
	conf := &ClientConfig{}
	logger := log.Discard
	conf.Load(dconf)
	authToken := func() string {
		return "test-token"
	}
	newToken := "new-token"
	Convey("http sink without auth watcher but with auth update func should work", t, func() {
		httpSink := sfxclient.NewHTTPSink()
		sink, ok := SetupSinkClientChanger(httpSink, conf, authToken, logger).(*ClientConfigChangerSink)
		So(ok, ShouldBeTrue)
		So(sink, ShouldNotBeNil)
		authUpdaterFunc := sink.AuthUpdate()
		So(authUpdaterFunc, ShouldNotBeNil)
		authUpdaterFunc(newToken)
		sink.mu.Lock()
		So(sink.Destination.AuthToken, ShouldEqual, newToken)
		sink.mu.Unlock()

		basicSink := dptest.NewBasicSink()
		So(SetupSinkClientChanger(basicSink, conf, nil, logger), ShouldEqual, basicSink)
	})
}
