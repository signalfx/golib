package httpdebug

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/signalfx/golib/v3/nettest"
	"github.com/signalfx/golib/v3/pointer"
	. "github.com/smartystreets/goconvey/convey"
)

type toExplore struct {
	name string
	age  int
}

func TestDebugServer(t *testing.T) {
	Convey("debug server should be setupable", t, func() {
		explorable := toExplore{
			name: "bob123",
			age:  10,
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		So(err, ShouldBeNil)
		ser := New(&Config{
			ReadTimeout:   pointer.Duration(time.Millisecond * 100),
			WriteTimeout:  pointer.Duration(time.Millisecond * 100),
			ExplorableObj: explorable,
		})
		listenPort := nettest.TCPPort(listener)
		done := make(chan error)
		go func() {
			done <- ser.Serve(listener)
			close(done)
		}()
		serverURL := fmt.Sprintf("http://127.0.0.1:%d", listenPort)
		Convey("and find commandline", func() {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, serverURL+"/debug/pprof/cmdline", nil)
			So(err, ShouldBeNil)
			resp, err := http.DefaultClient.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
			resp.Body.Close()
		})
		Convey("and find explorable", func() {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, serverURL+"/debug/explorer/name", nil)
			So(err, ShouldBeNil)
			resp, err := http.DefaultClient.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
			s, err := ioutil.ReadAll(resp.Body)
			So(err, ShouldBeNil)
			So(string(s), ShouldContainSubstring, "bob123")
			resp.Body.Close()
		})

		Reset(func() {
			So(listener.Close(), ShouldBeNil)
			<-done
		})
	})
}
