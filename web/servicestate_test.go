package web

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestServiceState_Checker(t *testing.T) {
	type testInfo struct {
		name           string
		method         string
		next           http.HandlerFunc
		state          int32
		health         int
		expectedHealth int
		statusCode     int
		resp           []byte
		expectedHeader string
	}
	for _, tC := range []testInfo{
		{
			name:           "Healthy status",
			method:         http.MethodGet,
			next:           nil,
			state:          healthy,
			statusCode:     http.StatusOK,
			expectedHealth: 1,
			resp:           RespOKByte,
		},
		{
			name:   "Healthy status",
			method: http.MethodPost,
			next: http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.Write(RespOKByte)
				rw.WriteHeader(http.StatusOK)
			}),
			state:          healthy,
			statusCode:     http.StatusOK,
			expectedHealth: 0,
			resp:           RespOKByte,
		},
		{
			name:   "service unavailable status",
			method: http.MethodPost,
			next: http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				panic(req)
			}),
			state:          serviceUnavailable,
			statusCode:     http.StatusServiceUnavailable,
			expectedHealth: 0,
			resp:           serviceUnavailableRespByte,
		},
		{
			name:   "service unavailable status",
			method: http.MethodGet,
			next: http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				panic(req)
			}),
			state:          serviceUnavailable,
			statusCode:     http.StatusServiceUnavailable,
			expectedHealth: 0,
			resp:           serviceUnavailableRespByte,
		},
		{
			name:           "graceful shutdown status",
			method:         http.MethodGet,
			next:           nil,
			state:          gracefulShutdown,
			statusCode:     http.StatusNotFound,
			expectedHealth: 0,
			resp:           gracefulShutdownRespByte,
			expectedHeader: Close,
		},
		{
			name:   "graceful shutdown status",
			method: http.MethodPost,
			next: http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.Write(RespOKByte)
				rw.WriteHeader(http.StatusOK)
			}),
			state:          gracefulShutdown,
			statusCode:     http.StatusOK,
			expectedHealth: 0,
			resp:           RespOKByte,
			expectedHeader: Close,
		},
	} {
		tC := tC
		t.Run(fmt.Sprintf("testing:%s and method:%s", tC.name, tC.method), func(t *testing.T) {
			Convey(fmt.Sprintf("testing:%s and method:%s", tC.name, tC.method), t, func() {
				rw := httptest.NewRecorder()
				req, _ := http.NewRequestWithContext(context.Background(), tC.method, "", nil)
				s := &ServiceState{
					state: tC.state,
				}
				h := s.Checker(tC.next, func() { tC.health++ })
				h.ServeHTTP(rw, req)
				So(rw.Code, ShouldEqual, tC.statusCode)
				So(tC.health, ShouldEqual, tC.expectedHealth)
				So(rw.Body.Bytes(), ShouldResemble, tC.resp)
				if tC.expectedHeader != "" {
					So(rw.Header().Get(Connection), ShouldEqual, tC.expectedHeader)
				}
			})
		})
	}
}

func TestServiceState_ServiceUnavailable(t *testing.T) {
	Convey("setting service as unavailable should work", t, func() {
		s := &ServiceState{}
		s.ServiceUnavailable()
		So(s.State(), ShouldEqual, serviceUnavailable)
		So(s.IsUnavailable(), ShouldBeTrue)
	})
}

func TestServiceState_Healthy(t *testing.T) {
	Convey("setting service healthy should work", t, func() {
		s := &ServiceState{serviceUnavailable}
		s.Healthy()
		So(s.State(), ShouldEqual, healthy)
		So(s.IsHealthy(), ShouldBeTrue)
	})
}

func TestServiceState_GracefulShutdown(t *testing.T) {
	Convey("setting service gracefulShutdown should work", t, func() {
		s := &ServiceState{}
		s.GracefulShutdown()
		So(s.State(), ShouldEqual, gracefulShutdown)
		So(s.IsInShutdown(), ShouldBeTrue)
	})
}
