package web

import (
	"net/http"
	"sync/atomic"
)

const (
	// healthy state of service
	healthy int32 = iota
	// gracefulShutdown state of service
	gracefulShutdown
	// serviceUnavailable right now
	serviceUnavailable
)

const (
	// RespOKStr should be used for response ok in string format
	RespOKStr = "OK"
	// Connection is used to set connection Header
	Connection = "Connection"
	// Close is used to set the value of Connection Header
	Close = "Close"
)

var (
	// RespOKByte should be used to send response ok in byte
	RespOKByte                 = []byte(RespOKStr)
	serviceUnavailableRespByte = []byte("service temporarily un-available")
	gracefulShutdownRespByte   = []byte("graceful shutdown")
)

func writeGracefulShutdownResponse(rw http.ResponseWriter) {
	rw.WriteHeader(http.StatusNotFound)
	_, _ = rw.Write(gracefulShutdownRespByte)
}

func writeServiceUnavailableResponse(rw http.ResponseWriter) {
	rw.WriteHeader(http.StatusServiceUnavailable)
	_, _ = rw.Write(serviceUnavailableRespByte)
}

// ServiceState is used to control when connections should signal they should be closed
type ServiceState struct {
	state int32
}

// ServiceUnavailable sets service state as Unavailable
func (s *ServiceState) ServiceUnavailable() {
	atomic.StoreInt32(&s.state, serviceUnavailable)
}

// Healthy sets service state as Healthy
func (s *ServiceState) Healthy() {
	atomic.StoreInt32(&s.state, healthy)
}

// GracefulShutdown sets service state as gracefulShutdown
func (s *ServiceState) GracefulShutdown() {
	atomic.StoreInt32(&s.state, gracefulShutdown)
}

// State returns current service state
func (s *ServiceState) State() int32 {
	return atomic.LoadInt32(&s.state)
}

// IsHealthy returns true if service is healthy
func (s *ServiceState) IsHealthy() bool {
	return atomic.LoadInt32(&s.state) == healthy
}

// IsInShutdown returns true if service is in graceful shutdown stage
func (s *ServiceState) IsInShutdown() bool {
	return atomic.LoadInt32(&s.state) == gracefulShutdown
}

// IsUnavailable returns true if service is temporarily un-available
func (s *ServiceState) IsUnavailable() bool {
	return atomic.LoadInt32(&s.state) == serviceUnavailable
}

// Checker will generate response writer based on status
// and is also used to check if service can accept incoming request..
// This comes in handy when the ALB refresh is not
// kicked in and we want to protect the system
// Note: next can be set to nil during GET request and it should still work
func (s *ServiceState) Checker(next http.Handler, incHealthCheck func()) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch atomic.LoadInt32(&s.state) {
		case gracefulShutdown:
			// we will optionally set header as Connection closed
			rw.Header().Set(Connection, Close)
			switch r.Method {
			case http.MethodGet:
				writeGracefulShutdownResponse(rw)
			case http.MethodPost:
				// this is incoming request during graceful shutdown...
				// let this request come through as header for connection
				// closed is already set
				next.ServeHTTP(rw, r)
			}
		case serviceUnavailable:
			// whatever the type of request is lets return 503
			writeServiceUnavailableResponse(rw)
		default:
			switch r.Method {
			case http.MethodGet:
				// GET is used during health check so write resp
				// and return
				_, _ = rw.Write(RespOKByte)
				incHealthCheck()
			case http.MethodPost:
				// if system is Healthy let the request come in
				next.ServeHTTP(rw, r)
			}
		}
	})
}
